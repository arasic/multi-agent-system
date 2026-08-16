"""Task claiming, leases, heartbeat, reaper, and result reporting (docs/architecture.md §5, ADR-005).

- claim_task:   SELECT … FOR UPDATE SKIP LOCKED on READY tasks; per-run concurrency enforced under the run row lock.
- heartbeat:    extends lease_until while the attempt is still RUNNING; returns False if it isn't (worker must stop).
- reap_expired: lease expired → ABANDONED; per-attempt runtime exceeded → TIMEOUT. Task goes back to READY or FAILED.
- report:       worker's result → COMPLETED (contract met) or RETRYABLE→READY|FAILED. Stale reports are rejected.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from mas.db.connection import Conn
from mas.db.events import emit
from mas.models.enums import AttemptStatus, RunStatus, TaskStatus
from mas.models.types import Attempt, Run, Task
from mas.orchestrator import state_machine as sm
from mas.orchestrator.contracts import missing_outputs

log = logging.getLogger(__name__)


class StaleAttempt(Exception):
    """The attempt is no longer RUNNING (reaped, cancelled, or already settled). The report is ignored."""


@dataclass(frozen=True)
class Claim:
    run: Run
    task: Task
    attempt: Attempt


def claim_task(
    conn: Conn,
    *,
    worker_id: str,
    capabilities: list[str] | tuple[str, ...],
    lease_s: int | None = None,
    run_id: UUID | None = None,
    pools: list[str] | tuple[str, ...] | None = None,
    scan_limit: int = 8,
) -> Claim | None:
    """Atomically claim one READY task this worker can do, respecting the run's max_concurrency.

    `run_id` pins the worker to one run; `pools` restricts it to runs in those pools; None = any.
    """
    caps = list(capabilities)
    with conn.transaction():
        params: list[Any] = [caps]
        filters = ""
        if run_id is not None:
            filters += " AND t.run_id = %s"
            params.append(run_id)
        if pools is not None:
            filters += " AND r.pool = ANY(%s)"
            params.append(list(pools))
        params.append(scan_limit)
        candidates = conn.execute(
            f"""
            SELECT t.* FROM tasks t
            JOIN runs r ON r.id = t.run_id
            WHERE t.status = 'READY' AND r.status = 'RUNNING' AND t.capability = ANY(%s) {filters}
            ORDER BY t.created_at, t.key
            FOR UPDATE OF t SKIP LOCKED
            LIMIT %s
            """,
            params,
        ).fetchall()
        for row in candidates:
            run_row = conn.execute("SELECT * FROM runs WHERE id = %s FOR UPDATE", (row["run_id"],)).fetchone()
            assert run_row is not None
            if RunStatus(run_row["status"]) is not RunStatus.RUNNING:
                continue
            live = conn.execute(
                """
                SELECT count(*) AS n FROM attempts a JOIN tasks t ON t.id = a.task_id
                WHERE t.run_id = %s AND a.status = 'RUNNING'
                """,
                (row["run_id"],),
            ).fetchone()["n"]  # type: ignore[index]
            if live >= run_row["max_concurrency"]:
                continue
            n = conn.execute("SELECT count(*) AS n FROM attempts WHERE task_id = %s", (row["id"],)).fetchone()["n"]  # type: ignore[index]
            lease = lease_s if lease_s is not None else run_row["lease_s"]
            att_row = conn.execute(
                """
                INSERT INTO attempts (task_id, attempt_number, status, worker_id, lease_until)
                VALUES (%s, %s, 'RUNNING', %s, now() + make_interval(secs => %s))
                RETURNING *
                """,
                (row["id"], n + 1, worker_id, lease),
            ).fetchone()
            assert att_row is not None
            task = sm.transition_task(conn, row["id"], TaskStatus.RUNNING, attempt_id=att_row["id"], worker_id=worker_id)
            emit(
                conn,
                row["run_id"],
                "attempt.leased",
                task_id=row["id"],
                attempt_id=att_row["id"],
                worker_id=worker_id,
                payload={"key": row["key"], "attempt_number": n + 1, "lease_s": lease},
            )
            return Claim(run=Run.from_row(run_row), task=task, attempt=Attempt.from_row(att_row))
    return None


def heartbeat(conn: Conn, attempt_id: UUID, lease_s: int) -> bool:
    with conn.transaction():
        row = conn.execute(
            """
            UPDATE attempts SET lease_until = now() + make_interval(secs => %s)
            WHERE id = %s AND status = 'RUNNING' RETURNING id
            """,
            (lease_s, attempt_id),
        ).fetchone()
    return row is not None


def reap_expired(conn: Conn, run_id: UUID | None = None) -> list[tuple[UUID, AttemptStatus]]:
    """Settle attempts whose lease expired (ABANDONED) or whose runtime exceeded the run limit (TIMEOUT)."""
    settled: list[tuple[UUID, AttemptStatus]] = []
    with conn.transaction():
        params: list[Any] = []
        run_filter = ""
        if run_id is not None:
            run_filter = "AND t.run_id = %s"
            params.append(run_id)
        rows = conn.execute(
            f"""
            SELECT a.id, a.lease_until, a.started_at, r.max_attempt_runtime_s,
                   (a.lease_until < now()) AS lease_expired,
                   (a.started_at + make_interval(secs => r.max_attempt_runtime_s) < now()) AS timed_out
            FROM attempts a
            JOIN tasks t ON t.id = a.task_id
            JOIN runs r ON r.id = t.run_id
            WHERE a.status = 'RUNNING' {run_filter}
              AND (a.lease_until < now() OR a.started_at + make_interval(secs => r.max_attempt_runtime_s) < now())
            FOR UPDATE OF a SKIP LOCKED
            """,
            params,
        ).fetchall()
        for r in rows:
            if r["timed_out"]:
                sm.settle_failed_attempt(conn, r["id"], AttemptStatus.TIMEOUT, reason="attempt runtime exceeded")
                settled.append((r["id"], AttemptStatus.TIMEOUT))
            else:
                sm.settle_failed_attempt(conn, r["id"], AttemptStatus.ABANDONED, reason="lease expired (worker presumed dead)")
                settled.append((r["id"], AttemptStatus.ABANDONED))
    return settled


def report(
    conn: Conn,
    attempt_id: UUID,
    *,
    success: bool,
    failure_reason: str | None = None,
    usage: dict[str, Any] | None = None,
    new_work_required: str | None = None,
) -> Task:
    """Apply a worker's result. Raises StaleAttempt if the attempt is not RUNNING anymore."""
    with conn.transaction():
        row = conn.execute(
            "SELECT a.*, t.run_id FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE a.id = %s FOR UPDATE OF a",
            (attempt_id,),
        ).fetchone()
        if row is None:
            raise LookupError(f"attempt {attempt_id} not found")
        if AttemptStatus(row["status"]) is not AttemptStatus.RUNNING:
            raise StaleAttempt(f"attempt {attempt_id} is {row['status']}, report ignored")
        task = sm.get_task(conn, row["task_id"])
        if new_work_required:
            emit(
                conn,
                row["run_id"],
                "task.new_work_required",
                task_id=task.id,
                attempt_id=attempt_id,
                worker_id=row["worker_id"],
                payload={"detail": new_work_required},
            )
        if success:
            missing = missing_outputs(conn, task, attempt_id)
            if missing:
                return sm.settle_failed_attempt(
                    conn, attempt_id, AttemptStatus.FAILED, reason=f"output contract unmet: {', '.join(missing)}", usage=usage
                )
            return sm.complete_attempt(conn, attempt_id, usage=usage)
        return sm.settle_failed_attempt(
            conn, attempt_id, AttemptStatus.FAILED, reason=failure_reason or "agent reported failure", usage=usage
        )
