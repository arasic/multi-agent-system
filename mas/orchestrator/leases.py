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
from mas.db.connection import Conn, Jsonb
from mas.db.events import emit
from mas.models.enums import ArtifactStatus, AttemptStatus, RunStatus, TaskStatus
from mas.models.types import Attempt, Run, Task
from mas.orchestrator import state_machine as sm
from mas.orchestrator.contracts import DECISION_TYPE, missing_outputs

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
    token_ceiling: int | None = None,
) -> Claim | None:
    """Atomically claim one READY task this worker can do, respecting the run's max_concurrency, and **reserve** the
    attempt's token allocation from the run's unreserved budget (I-4, hard total budget): allocation =
    min(run.max_attempt_tokens, token_ceiling, max_tokens - tokens_used - Σ allocations of RUNNING attempts), never
    below 0. Reserved tokens are simply the RUNNING attempts' allocations — settlement moves an attempt out of RUNNING
    and its actual usage into tokens_used, so nothing is released by hand and no counter can drift.

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
            allocation = reserve_allocation(conn, run_row, token_ceiling)
            att_row = conn.execute(
                """
                INSERT INTO attempts (task_id, attempt_number, status, worker_id, lease_until, token_allocation)
                VALUES (%s, %s, 'RUNNING', %s, now() + make_interval(secs => %s), %s)
                RETURNING *
                """,
                (row["id"], n + 1, worker_id, lease, allocation),
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
                payload={"key": row["key"], "attempt_number": n + 1, "lease_s": lease, "token_allocation": allocation},
            )
            return Claim(run=Run.from_row(run_row), task=task, attempt=Attempt.from_row(att_row))
    return None


def reserved_tokens(conn: Conn, run_id: UUID) -> int:
    """Tokens currently reserved by the run's RUNNING attempts (their allocations)."""
    row = conn.execute(
        """
        SELECT coalesce(sum(a.token_allocation), 0) AS reserved
        FROM attempts a JOIN tasks t ON t.id = a.task_id
        WHERE t.run_id = %s AND a.status = 'RUNNING'
        """,
        (run_id,),
    ).fetchone()
    return int(row["reserved"] or 0)  # type: ignore[index]


def reserve_allocation(conn: Conn, run_row: dict[str, Any], token_ceiling: int | None) -> int:
    """The token allocation for a new attempt of this run (caller holds the run lock — claims serialize per run)."""
    want = int(run_row["max_attempt_tokens"])
    if token_ceiling is not None:
        want = min(want, int(token_ceiling))
    free = int(run_row["max_tokens"]) - int(run_row["tokens_used"]) - reserved_tokens(conn, run_row["id"])
    return max(0, min(want, free))


def _validated_decision(meta: dict[str, Any], competing: dict[str, list[UUID]]) -> str | None:
    """A decision names a competing slot and a winner among that slot's candidates (the runtime knows the set: an agent
    can not crown an artifact it was never given). Losers default to the rest. Returns an error string or None."""
    slot = meta.get("slot")
    if not slot or slot not in competing:
        return f"decision for unknown/uncontested slot {slot!r}"
    ids = [str(x) for x in competing[slot]]
    winner = str(meta.get("winner") or "")
    if winner not in ids:
        return f"decision for {slot}: winner {winner!r} is not one of the competing artifacts {ids}"
    losers = [str(x) for x in (meta.get("losers") or [])] or [x for x in ids if x != winner]
    bad = [x for x in losers if x not in ids or x == winner]
    if bad:
        return f"decision for {slot}: losers {bad} are not competing artifacts"
    meta["losers"] = losers
    meta["rationale"] = str(meta.get("rationale") or "")[:2000]
    return None


def _apply_decision(conn: Conn, run_id: UUID, task: Task, attempt_id: UUID, meta: dict[str, Any]) -> None:
    """Winner → accepted, losers → superseded_by winner (immutable rows; only status/superseded_by change, through the
    artifact module). Already-decided artifacts (an earlier attempt of a sibling consumer) are left as they are."""
    winner = UUID(str(meta["winner"]))
    w = sm.get_artifact(conn, winner)
    if w.status is ArtifactStatus.CANDIDATE:
        store.accept(conn, winner)
    for loser in meta.get("losers") or []:
        lid = UUID(str(loser))
        cur = sm.get_artifact(conn, lid)
        if cur.status is ArtifactStatus.CANDIDATE:
            store.supersede(conn, lid, winner)
    emit(
        conn,
        run_id,
        "artifact.decided",
        task_id=task.id,
        attempt_id=attempt_id,
        payload={
            "slot": meta.get("slot"),
            "winner": str(winner),
            "losers": list(meta.get("losers") or []),
            "rationale": meta.get("rationale"),
        },
    )


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


def telemetry_usage(conn: Conn, attempt_id: UUID) -> dict[str, Any] | None:
    """What an attempt spent according to the meter's rows (written call by call, so present even when the worker died
    without reporting). The reaper settles this into the attempt and the run — a hard total budget counts dead
    attempts' spend too. In-flight calls that finish after settlement stay evidence only (overshoot ≤ one call)."""
    row = conn.execute(
        """
        SELECT coalesce(sum(input_tokens), 0) AS i, coalesce(sum(output_tokens), 0) AS o,
               coalesce(sum(cost_usd), 0) AS c, count(*) AS n, min(model) AS model
        FROM model_calls WHERE attempt_id = %s
        """,
        (attempt_id,),
    ).fetchone()
    if row is None or int(row["n"]) == 0:
        return None
    return {"model": row["model"], "input_tokens": int(row["i"]), "output_tokens": int(row["o"]), "cost_usd": float(row["c"])}


def reap_expired(conn: Conn, run_id: UUID | None = None) -> list[tuple[UUID, AttemptStatus]]:
    """Settle attempts whose lease expired (ABANDONED) or whose runtime exceeded the run limit (TIMEOUT).

    Unlocked scan, then per attempt one transaction in lock order run → task → attempt (task/attempt SKIP LOCKED),
    re-checking the condition. The run lock comes first because settlement charges the dead attempt's metered spend
    to the run (`telemetry_usage`); a reaper that touched `runs` after locking a task would reverse the lock order.
    """
    settled: list[tuple[UUID, AttemptStatus]] = []
    params: list[Any] = []
    run_filter = ""
    if run_id is not None:
        run_filter = "AND t.run_id = %s"
        params.append(run_id)
    rows = conn.execute(
        f"""
        SELECT a.id, a.task_id, t.run_id
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
            sm.lock_run(conn, r["run_id"])
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
            usage = telemetry_usage(conn, r["id"])  # the dead/hung attempt's metered spend counts against the run
            if a["timed_out"]:
                sm.settle_failed_attempt(conn, r["id"], AttemptStatus.TIMEOUT, reason="attempt runtime exceeded", usage=usage)
                settled.append((r["id"], AttemptStatus.TIMEOUT))
            else:
                sm.settle_failed_attempt(
                    conn, r["id"], AttemptStatus.ABANDONED, reason="lease expired (worker presumed dead)", usage=usage
                )
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
    competing: dict[str, list[UUID]] | None = None,
) -> Task:
    """Apply a worker's result atomically: lock run → task → attempt; publish artifacts; apply decisions on competing
    inputs (A7: winner accepted, losers superseded — only among the competing ids the runtime handed the agent);
    check the contract (a decision per competing slot is part of it); settle.

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
        decision_errors: list[str] = []
        for a in artifacts or []:
            meta = dict(a.meta)
            if a.type == DECISION_TYPE:
                err = _validated_decision(meta, competing or {})
                if err:
                    decision_errors.append(err)
                    continue  # an invalid decision is not published (nothing was decided)
            store.publish(
                conn,
                run_id=ids["run_id"],
                task_id=task.id,
                attempt_id=attempt_id,
                type=a.type,
                ref=a.ref,
                meta=meta,
            )
            if a.type == DECISION_TYPE:
                _apply_decision(conn, ids["run_id"], task, attempt_id, meta)
        if new_work_required:
            # step 13 trigger: the orchestrator's tick turns this into a re-plan (bounded by max_replans) when it can
            conn.execute(
                "UPDATE tasks SET meta = meta || %s WHERE id = %s",
                (Jsonb({"new_work_required": str(new_work_required)[:1000]}), task.id),
            )
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
            missing = missing_outputs(conn, task, attempt_id, competing=competing)
            if decision_errors:
                missing = missing + [f"invalid decision: {e}" for e in decision_errors]
            if missing:
                return sm.settle_failed_attempt(
                    conn,
                    attempt_id,
                    AttemptStatus.FAILED,
                    reason=f"output contract unmet: {', '.join(missing)}"[:1000],
                    usage=usage,
                )
            return sm.complete_attempt(conn, attempt_id, usage=usage)
        return sm.settle_failed_attempt(
            conn, attempt_id, AttemptStatus.FAILED, reason=failure_reason or "agent reported failure", usage=usage
        )
