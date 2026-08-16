"""Task claiming, leases, heartbeat, reaper, and atomic result reporting (docs/architecture.md §5, ADR-005).

Lock order everywhere: run → task → attempt → inserts (see state_machine.py). All row locks are FOR NO KEY UPDATE.

- claim_task:   find READY candidates without locking; per candidate, in its own short transaction: lock the run,
                check RUNNING + max_concurrency, then lock the task (SKIP LOCKED) and claim it.
- heartbeat:    extends lease_until while the attempt is still RUNNING; returns False if it isn't (worker must stop).
- reap_expired: lease expired → ABANDONED; per-attempt runtime exceeded → TIMEOUT. Locks task then attempt.
- report:       ONE transaction: lock run → task → attempt, verify the attempt is still RUNNING (else StaleAttempt),
                publish the artifacts, check the output contract, settle attempt/task. Nothing can be reaped between
                "artifacts published" and "attempt settled" because they are the same commit.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from mas.artifacts import store
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


@dataclass(frozen=True)
class ArtifactSpec:
    type: str
    ref: str
    meta: dict[str, Any]


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
    params: list[Any] = [caps]
    filters = ""
    if run_id is not None:
        filters += " AND t.run_id = %s"
        params.append(run_id)
    if pools is not None:
        filters += " AND r.pool = ANY(%s)"
        params.append(list(pools))
    params.append(scan_limit)
    # unlocked scan; each candidate is then claimed in its own transaction in lock order run → task
    candidates = conn.execute(
        f"""
        SELECT t.id AS task_id, t.run_id FROM tasks t
        JOIN runs r ON r.id = t.run_id
        WHERE t.status = 'READY' AND r.status = 'RUNNING' AND t.capability = ANY(%s) {filters}
        ORDER BY t.created_at, t.key
        LIMIT %s
        """,
        params,
    ).fetchall()
    for cand in candidates:
        with conn.transaction():
            run_row = conn.execute("SELECT * FROM runs WHERE id = %s FOR NO KEY UPDATE", (cand["run_id"],)).fetchone()
            if run_row is None or RunStatus(run_row["status"]) is not RunStatus.RUNNING:
                continue
            live = conn.execute(
                """
                SELECT count(*) AS n FROM attempts a JOIN tasks t ON t.id = a.task_id
                WHERE t.run_id = %s AND a.status = 'RUNNING'
                """,
                (cand["run_id"],),
            ).fetchone()["n"]  # type: ignore[index]
            if live >= run_row["max_concurrency"]:
                continue
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = %s AND status = 'READY' FOR NO KEY UPDATE SKIP LOCKED",
                (cand["task_id"],),
            ).fetchone()
            if row is None:
                continue  # someone else took it (or it moved on) between scan and lock
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
    """Settle attempts whose lease expired (ABANDONED) or whose runtime exceeded the run limit (TIMEOUT).

    Unlocked scan, then per attempt one transaction locking task → attempt (SKIP LOCKED) and re-checking the condition.
    """
    settled: list[tuple[UUID, AttemptStatus]] = []
    params: list[Any] = []
    run_filter = ""
    if run_id is not None:
        run_filter = "AND t.run_id = %s"
        params.append(run_id)
    rows = conn.execute(
        f"""
        SELECT a.id, a.task_id
        FROM attempts a
        JOIN tasks t ON t.id = a.task_id
        JOIN runs r ON r.id = t.run_id
        WHERE a.status = 'RUNNING' {run_filter}
          AND (a.lease_until < now() OR a.started_at + make_interval(secs => r.max_attempt_runtime_s) < now())
        """,
        params,
    ).fetchall()
    for r in rows:
        with conn.transaction():
            if sm.lock_task(conn, r["task_id"], skip_locked=True) is None:
                continue
            a = conn.execute(
                """
                SELECT a.id, a.status,
                       (a.lease_until < now()) AS lease_expired,
                       (a.started_at + make_interval(secs => r.max_attempt_runtime_s) < now()) AS timed_out
                FROM attempts a JOIN tasks t ON t.id = a.task_id JOIN runs r ON r.id = t.run_id
                WHERE a.id = %s FOR NO KEY UPDATE OF a SKIP LOCKED
                """,
                (r["id"],),
            ).fetchone()
            if a is None or a["status"] != "RUNNING" or not (a["lease_expired"] or a["timed_out"]):
                continue  # settled or heartbeat renewed in the meantime
            if a["timed_out"]:
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
    artifacts: list[ArtifactSpec] | None = None,
    failure_reason: str | None = None,
    usage: dict[str, Any] | None = None,
    new_work_required: str | None = None,
) -> Task:
    """Apply a worker's result atomically: lock run → task → attempt; publish artifacts; check contract; settle.

    Raises StaleAttempt (and publishes nothing) if the attempt is not RUNNING anymore.
    """
    with conn.transaction():
        ids = conn.execute(
            "SELECT a.task_id, t.run_id FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE a.id = %s",
            (attempt_id,),
        ).fetchone()
        if ids is None:
            raise LookupError(f"attempt {attempt_id} not found")
        sm.lock_run(conn, ids["run_id"])
        task = sm.lock_task(conn, ids["task_id"])
        assert task is not None
        att = sm.lock_attempt(conn, attempt_id)
        assert att is not None
        if att.status is not AttemptStatus.RUNNING:
            raise StaleAttempt(f"attempt {attempt_id} is {att.status.value}, report ignored")
        for a in artifacts or []:
            store.publish(
                conn,
                run_id=ids["run_id"],
                task_id=task.id,
                attempt_id=attempt_id,
                type=a.type,
                ref=a.ref,
                meta=a.meta,
            )
        if new_work_required:
            emit(
                conn,
                ids["run_id"],
                "task.new_work_required",
                task_id=task.id,
                attempt_id=attempt_id,
                worker_id=att.worker_id,
                payload={"detail": new_work_required},
            )
        if success:
            missing = missing_outputs(conn, task, attempt_id)
            if missing:
                return sm.settle_failed_attempt(
                    conn,
                    attempt_id,
                    AttemptStatus.FAILED,
                    reason=f"output contract unmet: {', '.join(missing)}",
                    usage=usage,
                )
            return sm.complete_attempt(conn, attempt_id, usage=usage)
        return sm.settle_failed_attempt(
            conn, attempt_id, AttemptStatus.FAILED, reason=failure_reason or "agent reported failure", usage=usage
        )
