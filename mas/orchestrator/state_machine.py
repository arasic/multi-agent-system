"""State machines for Run, Task, Attempt, Artifact (docs/architecture.md §4).

THIS IS THE ONLY MODULE THAT WRITES `status` COLUMNS. Every transition:
  1. locks the row (FOR UPDATE),
  2. checks the transition table,
  3. updates the row (plus started_at / finished_at bookkeeping),
  4. emits an event in the same transaction.

Agents never call this directly; the worker runtime reports and the orchestrator transitions.
Composite operations (settle a failed attempt, abort a run, ...) live at the bottom so that
callers never have to sequence primitives themselves.

LOCK ORDER (every transaction, no exceptions):  run -> task -> attempt -> inserts (artifacts, events).
Levels may be skipped, never reversed. Row locks are FOR NO KEY UPDATE, which does not conflict with the
FOR KEY SHARE locks that inserts referencing these rows take - so a tick holding the run row never blocks a
worker inserting an event, and no lock cycle can form through foreign keys. Use lock_run/lock_task/lock_attempt
at the top of a transaction; the transition primitives re-lock (same tx, no-op) and check the transition table.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from mas.db.connection import Conn, Jsonb
from mas.db.events import emit
from mas.models.enums import (
    TASK_TERMINAL,
    ArtifactStatus,
    AttemptStatus,
    RunStatus,
    TaskStatus,
    VerdictReason,
)
from mas.models.types import Artifact, Attempt, Run, Task

# --------------------------------------------------------------------------- transition tables

RUN_TRANSITIONS: dict[RunStatus, frozenset[RunStatus]] = {
    RunStatus.CREATED: frozenset({RunStatus.PLANNING, RunStatus.ABORTED}),
    RunStatus.PLANNING: frozenset({RunStatus.RUNNING, RunStatus.AWAITING_INPUT, RunStatus.FAILED, RunStatus.ABORTED}),
    # ADR-006: planner asked; a recorded answer sends the run back to (re)planning; budgets still end it
    RunStatus.AWAITING_INPUT: frozenset({RunStatus.PLANNING, RunStatus.REPLANNING, RunStatus.FAILED, RunStatus.ABORTED}),
    RunStatus.RUNNING: frozenset({RunStatus.VERIFYING, RunStatus.REPLANNING, RunStatus.FAILED, RunStatus.ABORTED}),
    RunStatus.VERIFYING: frozenset({RunStatus.PASSED, RunStatus.REPLANNING, RunStatus.FAILED, RunStatus.ABORTED}),
    RunStatus.REPLANNING: frozenset({RunStatus.RUNNING, RunStatus.AWAITING_INPUT, RunStatus.FAILED, RunStatus.ABORTED}),
    RunStatus.PASSED: frozenset(),
    RunStatus.FAILED: frozenset(),
    RunStatus.ABORTED: frozenset(),
}

TASK_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.READY, TaskStatus.BLOCKED, TaskStatus.CANCELLED}),
    TaskStatus.READY: frozenset({TaskStatus.RUNNING, TaskStatus.CANCELLED}),
    TaskStatus.RUNNING: frozenset({TaskStatus.COMPLETED, TaskStatus.RETRYABLE, TaskStatus.CANCELLED}),
    TaskStatus.RETRYABLE: frozenset({TaskStatus.READY, TaskStatus.FAILED, TaskStatus.CANCELLED}),
    TaskStatus.COMPLETED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.BLOCKED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
}

ATTEMPT_TRANSITIONS: dict[AttemptStatus, frozenset[AttemptStatus]] = {
    AttemptStatus.RUNNING: frozenset(
        {
            AttemptStatus.SUCCESS,
            AttemptStatus.FAILED,
            AttemptStatus.TIMEOUT,
            AttemptStatus.ABANDONED,
            AttemptStatus.CANCELLED,
        }
    ),
    AttemptStatus.SUCCESS: frozenset(),
    AttemptStatus.FAILED: frozenset(),
    AttemptStatus.TIMEOUT: frozenset(),
    AttemptStatus.ABANDONED: frozenset(),
    AttemptStatus.CANCELLED: frozenset(),
}

ARTIFACT_TRANSITIONS: dict[ArtifactStatus, frozenset[ArtifactStatus]] = {
    ArtifactStatus.CANDIDATE: frozenset({ArtifactStatus.ACCEPTED, ArtifactStatus.SUPERSEDED, ArtifactStatus.REJECTED}),
    ArtifactStatus.ACCEPTED: frozenset({ArtifactStatus.SUPERSEDED}),
    ArtifactStatus.SUPERSEDED: frozenset(),
    ArtifactStatus.REJECTED: frozenset(),
}


class IllegalTransition(Exception):
    def __init__(self, entity: str, entity_id: Any, from_status: str, to_status: str):
        self.entity, self.entity_id, self.from_status, self.to_status = entity, entity_id, from_status, to_status
        super().__init__(f"illegal {entity} transition {from_status} -> {to_status} ({entity_id})")


# --------------------------------------------------------------------------- row locks (lock order: run -> task -> attempt)


def lock_run(conn: Conn, run_id: UUID) -> Run:
    row = conn.execute("SELECT * FROM runs WHERE id = %s FOR NO KEY UPDATE", (run_id,)).fetchone()
    if row is None:
        raise LookupError(f"run {run_id} not found")
    return Run.from_row(row)


def lock_task(conn: Conn, task_id: UUID, *, skip_locked: bool = False) -> Task | None:
    q = "SELECT * FROM tasks WHERE id = %s FOR NO KEY UPDATE" + (" SKIP LOCKED" if skip_locked else "")
    row = conn.execute(q, (task_id,)).fetchone()
    if row is None:
        if skip_locked:
            return None
        raise LookupError(f"task {task_id} not found")
    return Task.from_row(row)


def lock_attempt(conn: Conn, attempt_id: UUID, *, skip_locked: bool = False) -> Attempt | None:
    q = "SELECT * FROM attempts WHERE id = %s FOR NO KEY UPDATE" + (" SKIP LOCKED" if skip_locked else "")
    row = conn.execute(q, (attempt_id,)).fetchone()
    if row is None:
        if skip_locked:
            return None
        raise LookupError(f"attempt {attempt_id} not found")
    return Attempt.from_row(row)


def can_run(a: RunStatus, b: RunStatus) -> bool:
    return b in RUN_TRANSITIONS[a]


def can_task(a: TaskStatus, b: TaskStatus) -> bool:
    return b in TASK_TRANSITIONS[a]


def can_attempt(a: AttemptStatus, b: AttemptStatus) -> bool:
    return b in ATTEMPT_TRANSITIONS[a]


def can_artifact(a: ArtifactStatus, b: ArtifactStatus) -> bool:
    return b in ARTIFACT_TRANSITIONS[a]


# --------------------------------------------------------------------------- primitives


def transition_run(
    conn: Conn,
    run_id: UUID,
    to: RunStatus,
    *,
    verdict: str | None = None,
    reason: VerdictReason | None = None,
    payload: dict[str, Any] | None = None,
) -> Run:
    row = conn.execute("SELECT * FROM runs WHERE id = %s FOR NO KEY UPDATE", (run_id,)).fetchone()
    if row is None:
        raise LookupError(f"run {run_id} not found")
    cur = RunStatus(row["status"])
    if not can_run(cur, to):
        raise IllegalTransition("run", run_id, cur, to)
    conn.execute(
        """
        UPDATE runs SET
            status = %s,
            verdict = COALESCE(%s, verdict),
            verdict_reason = CASE WHEN %s IN ('FAILED','ABORTED') THEN %s ELSE verdict_reason END,
            started_at = CASE WHEN %s = 'RUNNING' AND started_at IS NULL THEN now() ELSE started_at END,
            finished_at = CASE WHEN %s IN ('PASSED','FAILED','ABORTED') THEN now() ELSE finished_at END
        WHERE id = %s
        """,
        (to.value, verdict, to.value, reason.value if reason else None, to.value, to.value, run_id),
    )
    emit(
        conn,
        run_id,
        f"run.{to.value.lower()}",
        payload={"from": cur.value, "verdict": verdict, "reason": reason.value if reason else None, **(payload or {})},
    )
    return Run.from_row(conn.execute("SELECT * FROM runs WHERE id = %s", (run_id,)).fetchone())  # type: ignore[arg-type]


def transition_task(
    conn: Conn,
    task_id: UUID,
    to: TaskStatus,
    *,
    attempt_id: UUID | None = None,
    worker_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> Task:
    row = conn.execute("SELECT * FROM tasks WHERE id = %s FOR NO KEY UPDATE", (task_id,)).fetchone()
    if row is None:
        raise LookupError(f"task {task_id} not found")
    cur = TaskStatus(row["status"])
    if not can_task(cur, to):
        raise IllegalTransition("task", row["key"], cur, to)
    conn.execute("UPDATE tasks SET status = %s, updated_at = now() WHERE id = %s", (to.value, task_id))
    emit(
        conn,
        row["run_id"],
        f"task.{to.value.lower()}",
        task_id=task_id,
        attempt_id=attempt_id,
        worker_id=worker_id,
        payload={"key": row["key"], "from": cur.value, **(payload or {})},
    )
    row["status"] = to.value
    return Task.from_row(row)


def transition_attempt(
    conn: Conn,
    attempt_id: UUID,
    to: AttemptStatus,
    *,
    failure_reason: str | None = None,
    payload: dict[str, Any] | None = None,
) -> Attempt:
    row = conn.execute(
        """
        SELECT a.*, t.run_id, t.key FROM attempts a JOIN tasks t ON t.id = a.task_id
        WHERE a.id = %s FOR NO KEY UPDATE OF a
        """,
        (attempt_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"attempt {attempt_id} not found")
    cur = AttemptStatus(row["status"])
    if not can_attempt(cur, to):
        raise IllegalTransition("attempt", f"{row['key']}#{row['attempt_number']}", cur, to)
    conn.execute(
        """
        UPDATE attempts SET status = %s, failure_reason = COALESCE(%s, failure_reason),
            finished_at = CASE WHEN %s <> 'RUNNING' THEN now() ELSE finished_at END
        WHERE id = %s
        """,
        (to.value, failure_reason, to.value, attempt_id),
    )
    emit(
        conn,
        row["run_id"],
        f"attempt.{to.value.lower()}",
        task_id=row["task_id"],
        attempt_id=attempt_id,
        worker_id=row["worker_id"],
        payload={
            "key": row["key"],
            "attempt_number": row["attempt_number"],
            "from": cur.value,
            "failure_reason": failure_reason,
            **(payload or {}),
        },
    )
    row["status"] = to.value
    row["failure_reason"] = failure_reason or row.get("failure_reason")
    return Attempt.from_row(row)


def transition_artifact(
    conn: Conn,
    artifact_id: UUID,
    to: ArtifactStatus,
    *,
    superseded_by: UUID | None = None,
    payload: dict[str, Any] | None = None,
) -> Artifact:
    row = conn.execute("SELECT * FROM artifacts WHERE id = %s FOR NO KEY UPDATE", (artifact_id,)).fetchone()
    if row is None:
        raise LookupError(f"artifact {artifact_id} not found")
    cur = ArtifactStatus(row["status"])
    if not can_artifact(cur, to):
        raise IllegalTransition("artifact", artifact_id, cur, to)
    if to is ArtifactStatus.SUPERSEDED and superseded_by is None:
        raise ValueError("superseded requires superseded_by")
    conn.execute(
        "UPDATE artifacts SET status = %s, superseded_by = COALESCE(%s, superseded_by) WHERE id = %s",
        (to.value, superseded_by, artifact_id),
    )
    emit(
        conn,
        row["run_id"],
        f"artifact.{to.value}",
        task_id=row["task_id"],
        attempt_id=row["attempt_id"],
        payload={
            "artifact_id": str(artifact_id),
            "type": row["type"],
            "from": cur.value,
            "superseded_by": str(superseded_by) if superseded_by else None,
            **(payload or {}),
        },
    )
    row["status"] = to.value
    row["superseded_by"] = superseded_by or row.get("superseded_by")
    return Artifact.from_row(row)


# --------------------------------------------------------------------------- composites


def complete_attempt(conn: Conn, attempt_id: UUID, *, usage: dict[str, Any] | None = None) -> Task:
    """attempt → SUCCESS, task → COMPLETED. Caller has already checked the output contract."""
    att0 = conn.execute("SELECT task_id FROM attempts WHERE id = %s", (attempt_id,)).fetchone()
    if att0 is None:
        raise LookupError(f"attempt {attempt_id} not found")
    lock_task(conn, att0["task_id"])  # lock order: task before attempt
    if usage:
        _record_usage(conn, attempt_id, usage)
    att = transition_attempt(conn, attempt_id, AttemptStatus.SUCCESS)
    return transition_task(conn, att.task_id, TaskStatus.COMPLETED, attempt_id=attempt_id, worker_id=att.worker_id)


def settle_failed_attempt(
    conn: Conn,
    attempt_id: UUID,
    status: AttemptStatus,
    *,
    reason: str | None = None,
    usage: dict[str, Any] | None = None,
) -> Task:
    """attempt → FAILED|TIMEOUT|ABANDONED|CANCELLED, task → RETRYABLE → READY (retries left) or FAILED.

    Both task transitions are written (two events) so the audit trail matches the state machine.
    """
    if status not in {AttemptStatus.FAILED, AttemptStatus.TIMEOUT, AttemptStatus.ABANDONED, AttemptStatus.CANCELLED}:
        raise ValueError(f"not a failure status: {status}")
    # lock order: task before attempt (callers already holding the run lock are fine)
    att0 = conn.execute("SELECT task_id FROM attempts WHERE id = %s", (attempt_id,)).fetchone()
    if att0 is None:
        raise LookupError(f"attempt {attempt_id} not found")
    task_row = conn.execute("SELECT * FROM tasks WHERE id = %s FOR NO KEY UPDATE", (att0["task_id"],)).fetchone()
    assert task_row is not None
    if usage:
        _record_usage(conn, attempt_id, usage)
    att = transition_attempt(conn, attempt_id, status, failure_reason=reason)
    if TaskStatus(task_row["status"]) is not TaskStatus.RUNNING:
        # e.g. task was CANCELLED while the attempt was live — nothing more to settle
        return Task.from_row(task_row)
    transition_task(
        conn, att.task_id, TaskStatus.RETRYABLE, attempt_id=attempt_id, worker_id=att.worker_id, payload={"reason": reason}
    )
    used = conn.execute("SELECT count(*) AS n FROM attempts WHERE task_id = %s", (att.task_id,)).fetchone()["n"]  # type: ignore[index]
    if used < task_row["max_attempts"]:
        return transition_task(
            conn, att.task_id, TaskStatus.READY, payload={"attempts_used": used, "max_attempts": task_row["max_attempts"]}
        )
    return transition_task(
        conn,
        att.task_id,
        TaskStatus.FAILED,
        payload={"attempts_used": used, "max_attempts": task_row["max_attempts"], "reason": "attempts exhausted"},
    )


def _record_usage(conn: Conn, attempt_id: UUID, usage: dict[str, Any]) -> None:
    conn.execute(
        """
        UPDATE attempts SET
            model = COALESCE(%s, model),
            input_tokens = input_tokens + %s,
            output_tokens = output_tokens + %s,
            cost_usd = cost_usd + %s
        WHERE id = %s
        """,
        (
            usage.get("model"),
            int(usage.get("input_tokens", 0)),
            int(usage.get("output_tokens", 0)),
            float(usage.get("cost_usd", 0.0)),
            attempt_id,
        ),
    )
    conn.execute(
        """
        UPDATE runs r SET
            tokens_used = tokens_used + %s,
            cost_used_usd = cost_used_usd + %s
        FROM attempts a JOIN tasks t ON t.id = a.task_id
        WHERE a.id = %s AND r.id = t.run_id
        """,
        (int(usage.get("input_tokens", 0)) + int(usage.get("output_tokens", 0)), float(usage.get("cost_usd", 0.0)), attempt_id),
    )


def _cancel_live_work(conn: Conn, run_id: UUID, reason: str) -> None:
    """Caller holds the run lock. Lock order inside: tasks, then attempts."""
    open_tasks = conn.execute(
        "SELECT id FROM tasks WHERE run_id = %s AND status <> ALL(%s) ORDER BY created_at, key FOR NO KEY UPDATE",
        (run_id, [s.value for s in TASK_TERMINAL]),
    ).fetchall()
    live = conn.execute(
        """
        SELECT a.id FROM attempts a JOIN tasks t ON t.id = a.task_id
        WHERE t.run_id = %s AND a.status = 'RUNNING' ORDER BY a.started_at FOR NO KEY UPDATE OF a
        """,
        (run_id,),
    ).fetchall()
    for r in live:
        transition_attempt(conn, r["id"], AttemptStatus.CANCELLED, failure_reason=reason)
    for r in open_tasks:
        transition_task(conn, r["id"], TaskStatus.CANCELLED, payload={"reason": reason})


def abort_run(conn: Conn, run_id: UUID, reason: str, *, code: VerdictReason = VerdictReason.BUDGET_EXHAUSTED) -> Run:
    """Budget/deadline/operator abort: run → ABORTED, live attempts → CANCELLED, open tasks → CANCELLED."""
    run = transition_run(conn, run_id, RunStatus.ABORTED, verdict=f"ABORTED:{reason}", reason=code, payload={"detail": reason})
    _cancel_live_work(conn, run_id, f"run aborted: {reason}")
    return run


def fail_run(conn: Conn, run_id: UUID, reason: str, *, code: VerdictReason) -> Run:
    """Definite failure with a reason code (ADR-008 §6) and text: run → FAILED, live attempts → CANCELLED, open tasks →
    CANCELLED. The code is one of the six terminal reasons; the text says what happened."""
    run = transition_run(conn, run_id, RunStatus.FAILED, verdict=f"FAIL:{reason}", reason=code, payload={"detail": reason})
    _cancel_live_work(conn, run_id, f"run failed: {reason}")
    return run


def start_replan(conn: Conn, run_id: UUID, *, payload: dict[str, Any] | None = None) -> Run:
    """Bounded repair (step 13-lite): VERIFYING/RUNNING → REPLANNING and one more replan used. The caller has already
    decided the run may repair (replans_used < max_replans, no repeated progress fingerprint) — this only records it."""
    conn.execute("UPDATE runs SET replans_used = replans_used + 1 WHERE id = %s", (run_id,))
    return transition_run(conn, run_id, RunStatus.REPLANNING, payload=payload)


def pass_run(conn: Conn, run_id: UUID) -> Run:
    return transition_run(conn, run_id, RunStatus.PASSED, verdict="PASS")


def get_run(conn: Conn, run_id: UUID) -> Run:
    row = conn.execute("SELECT * FROM runs WHERE id = %s", (run_id,)).fetchone()
    if row is None:
        raise LookupError(f"run {run_id} not found")
    return Run.from_row(row)


def get_task(conn: Conn, task_id: UUID) -> Task:
    row = conn.execute("SELECT * FROM tasks WHERE id = %s", (task_id,)).fetchone()
    if row is None:
        raise LookupError(f"task {task_id} not found")
    return Task.from_row(row)


def get_attempt(conn: Conn, attempt_id: UUID) -> Attempt:
    row = conn.execute("SELECT * FROM attempts WHERE id = %s", (attempt_id,)).fetchone()
    if row is None:
        raise LookupError(f"attempt {attempt_id} not found")
    return Attempt.from_row(row)


def tasks_for_run(conn: Conn, run_id: UUID) -> list[Task]:
    rows = conn.execute("SELECT * FROM tasks WHERE run_id = %s ORDER BY created_at, key", (run_id,)).fetchall()
    return [Task.from_row(r) for r in rows]


def attempts_for_run(conn: Conn, run_id: UUID) -> list[Attempt]:
    rows = conn.execute(
        """
        SELECT a.* FROM attempts a JOIN tasks t ON t.id = a.task_id
        WHERE t.run_id = %s ORDER BY a.started_at, a.attempt_number
        """,
        (run_id,),
    ).fetchall()
    return [Attempt.from_row(r) for r in rows]


__all__ = [
    "ARTIFACT_TRANSITIONS",
    "ATTEMPT_TRANSITIONS",
    "RUN_TRANSITIONS",
    "TASK_TRANSITIONS",
    "IllegalTransition",
    "Jsonb",
    "abort_run",
    "attempts_for_run",
    "can_artifact",
    "can_attempt",
    "can_run",
    "can_task",
    "complete_attempt",
    "fail_run",
    "get_attempt",
    "get_run",
    "get_task",
    "lock_attempt",
    "lock_run",
    "lock_task",
    "pass_run",
    "settle_failed_attempt",
    "tasks_for_run",
    "transition_artifact",
    "transition_attempt",
    "transition_run",
    "transition_task",
]
