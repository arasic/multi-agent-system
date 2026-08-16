"""Per-run tick and the orchestrator loop (docs/architecture.md §4, §7, §9).

tick(run):
  1. reap expired leases / timed-out attempts
  2. enforce budgets (abort if exceeded)
  3. readiness: PENDING → READY (all deps COMPLETED) or → BLOCKED (an upstream is FAILED/BLOCKED/CANCELLED)
  4. run progression: task FAILED → REPLANNING (step 13) or FAILED; integration COMPLETED → VERIFYING
  5. verifier stage: PASS → PASSED (integration artifacts accepted); FAIL → REPLANNING (step 13) or FAILED

Nothing here calls a model. Nothing here interprets task content.
"""

from __future__ import annotations

import logging
import time
from typing import Any
from uuid import UUID

from mas.artifacts import store
from mas.db.connection import Conn
from mas.db.events import emit
from mas.models.enums import (
    INTEGRATION_CAPABILITY,
    TASK_TERMINAL,
    TASK_UPSTREAM_BLOCKING,
    ArtifactStatus,
    RunStatus,
    TaskStatus,
)
from mas.models.types import Run
from mas.orchestrator import budgets as budget_rules
from mas.orchestrator import state_machine as sm
from mas.orchestrator.leases import reap_expired
from mas.verifier.base import Verifier
from mas.verifier.stub import StubVerifier

log = logging.getLogger(__name__)


def _promote_ready(conn: Conn, run_id: UUID) -> list[str]:
    rows = conn.execute(
        """
        SELECT t.id, t.key FROM tasks t
        WHERE t.run_id = %s AND t.status = 'PENDING'
          AND NOT EXISTS (
              SELECT 1 FROM task_dependencies d JOIN tasks u ON u.id = d.depends_on_task_id
              WHERE d.task_id = t.id AND u.status <> 'COMPLETED')
        ORDER BY t.created_at, t.key
        FOR UPDATE OF t SKIP LOCKED
        """,
        (run_id,),
    ).fetchall()
    for r in rows:
        sm.transition_task(conn, r["id"], TaskStatus.READY)
    return [r["key"] for r in rows]


def _block_unreachable(conn: Conn, run_id: UUID) -> list[str]:
    rows = conn.execute(
        """
        SELECT t.id, t.key FROM tasks t
        WHERE t.run_id = %s AND t.status = 'PENDING'
          AND EXISTS (
              SELECT 1 FROM task_dependencies d JOIN tasks u ON u.id = d.depends_on_task_id
              WHERE d.task_id = t.id AND u.status = ANY(%s))
        FOR UPDATE OF t SKIP LOCKED
        """,
        (run_id, [s.value for s in TASK_UPSTREAM_BLOCKING]),
    ).fetchall()
    for r in rows:
        sm.transition_task(conn, r["id"], TaskStatus.BLOCKED, payload={"reason": "upstream task failed/blocked/cancelled"})
    return [r["key"] for r in rows]


def _integration_task(conn: Conn, run_id: UUID) -> dict[str, Any] | None:
    return conn.execute(
        "SELECT * FROM tasks WHERE run_id = %s AND capability = %s ORDER BY created_at LIMIT 1",
        (run_id, INTEGRATION_CAPABILITY),
    ).fetchone()


VERIFY_LOCK_NS = 0x4D415356  # 'MASV': advisory-lock namespace for the verifier stage


def _try_verify_lock(conn: Conn, run_id: UUID) -> bool:
    """Session-level advisory lock: one verifier per run at a time; auto-released if this process dies."""
    row = conn.execute("SELECT pg_try_advisory_lock(%s, hashtext(%s)) AS ok", (VERIFY_LOCK_NS, str(run_id))).fetchone()
    return bool(row and row["ok"])


def _verify_unlock(conn: Conn, run_id: UUID) -> None:
    try:
        conn.execute("SELECT pg_advisory_unlock(%s, hashtext(%s))", (VERIFY_LOCK_NS, str(run_id)))
    except Exception:  # connection may be gone; the lock dies with the session anyway
        log.debug("advisory unlock failed", exc_info=True)


def _verify(conn: Conn, run_id: UUID, verifier: Verifier) -> Run:
    """Verifier stage (ADR-003). Re-entrant: any orchestrator that finds the run in VERIFYING may run it,
    guarded by a session advisory lock so only one does at a time. If the process holding the lock dies,
    Postgres releases the lock and the next tick retries — a run can never be stranded in VERIFYING.
    """
    if not _try_verify_lock(conn, run_id):
        return sm.get_run(conn, run_id)  # another orchestrator is verifying this run right now
    try:
        run = sm.get_run(conn, run_id)
        if run.status is not RunStatus.VERIFYING:
            return run  # someone finished it between our tick and our lock
        try:
            result = verifier.verify(conn, run)
        except Exception as e:  # verifier crash is a FAIL with a reason, never a hang
            log.exception("verifier crashed")
            result_passed, report = False, {"error": f"verifier crashed: {e!r}"}
        else:
            result_passed, report = result.passed, dict(result.report)
        with conn.transaction():
            n = conn.execute(
                "SELECT count(*) AS n FROM artifacts WHERE run_id = %s AND type = 'verification'", (run_id,)
            ).fetchone()["n"]  # type: ignore[index]
            store.publish(
                conn,
                run_id=run_id,
                type="verification",
                ref=f"verify:{run_id}:{n + 1}",
                meta={"passed": result_passed, "verifier": getattr(verifier, "name", "?"), "report": report},
            )
            emit(
                conn,
                run_id,
                "verify.passed" if result_passed else "verify.failed",
                payload={"verifier": getattr(verifier, "name", "?"), "report": report},
            )
            if result_passed:
                integ = _integration_task(conn, run_id)
                if integ is not None:
                    # accept only the winning (SUCCESS) attempt's outputs — earlier attempts' candidates stay hints
                    for a in store.outputs_of_task(conn, integ["id"]):
                        if a.status is ArtifactStatus.CANDIDATE:
                            store.accept(conn, a.id)
                return sm.pass_run(conn, run_id)
            # TODO(step 13): if replans remain and a replanner is configured → REPLANNING with the report
            return sm.fail_run(conn, run_id, "verification failed")
    finally:
        _verify_unlock(conn, run_id)


def tick(conn: Conn, run_id: UUID, *, verifier: Verifier | None = None) -> Run:
    verifier = verifier or StubVerifier(passed=True)
    reap_expired(conn, run_id)

    do_verify = False
    with conn.transaction():
        row = conn.execute("SELECT * FROM runs WHERE id = %s FOR UPDATE", (run_id,)).fetchone()
        if row is None:
            raise LookupError(f"run {run_id} not found")
        run = Run.from_row(row)
        if run.status.terminal:
            return run

        reason = budget_rules.violation(run)
        if reason:
            return sm.abort_run(conn, run_id, reason)

        if run.status is RunStatus.RUNNING:
            _block_unreachable(conn, run_id)
            _promote_ready(conn, run_id)

            failed = conn.execute(
                "SELECT key FROM tasks WHERE run_id = %s AND status = 'FAILED' ORDER BY updated_at LIMIT 1", (run_id,)
            ).fetchone()
            if failed is not None:
                # TODO(step 13): if replans remain and a replanner is configured → REPLANNING
                return sm.fail_run(conn, run_id, f"task {failed['key']} failed (retries exhausted)")

            integ = _integration_task(conn, run_id)
            if integ is not None and TaskStatus(integ["status"]) is TaskStatus.COMPLETED:
                sm.transition_run(conn, run_id, RunStatus.VERIFYING)
                do_verify = True
            else:
                open_n = conn.execute(
                    "SELECT count(*) AS n FROM tasks WHERE run_id = %s AND status <> ALL(%s)",
                    (run_id, [s.value for s in TASK_TERMINAL]),
                ).fetchone()["n"]  # type: ignore[index]
                if open_n == 0:
                    return sm.fail_run(conn, run_id, "no runnable tasks left and integration not completed")
        elif run.status is RunStatus.VERIFYING:
            # found already VERIFYING (we or another orchestrator started it and did not finish): retry the stage
            do_verify = True
        # REPLANNING: step 13.

    if do_verify:
        return _verify(conn, run_id, verifier)
    return sm.get_run(conn, run_id)


def stall_report(conn: Conn, run_id: UUID) -> dict[str, Any]:
    """Snapshot of what is open on a run — logged by the watchdog, printed by `mas status` on non-terminal runs."""
    tasks = conn.execute(
        "SELECT key, status FROM tasks WHERE run_id = %s AND status <> ALL(%s) ORDER BY key",
        (run_id, [s.value for s in TASK_TERMINAL]),
    ).fetchall()
    atts = conn.execute(
        """
        SELECT t.key, a.attempt_number, a.worker_id,
               round(extract(epoch FROM (a.lease_until - now()))::numeric, 1) AS lease_left_s,
               round(extract(epoch FROM (now() - a.started_at))::numeric, 1) AS running_s
        FROM attempts a JOIN tasks t ON t.id = a.task_id
        WHERE t.run_id = %s AND a.status = 'RUNNING' ORDER BY a.started_at
        """,
        (run_id,),
    ).fetchall()
    last = conn.execute("SELECT type, ts FROM events WHERE run_id = %s ORDER BY id DESC LIMIT 1", (run_id,)).fetchone()
    return {
        "open_tasks": {r["key"]: r["status"] for r in tasks},
        "running_attempts": [dict(r) for r in atts],
        "last_event": (last["type"], last["ts"].isoformat()) if last else None,
    }


def run_until_terminal(
    conn: Conn,
    run_id: UUID,
    *,
    verifier: Verifier | None = None,
    tick_s: float = 0.25,
    timeout_s: float | None = None,
    stall_warn_s: float = 20.0,
) -> Run:
    """Tick one run until it reaches a terminal state. Returns the final Run.

    Watchdog: if the event log has not advanced for `stall_warn_s`, log a WARNING with the open tasks and
    RUNNING attempts (lease left, running time). Budgets (I-4) are what actually end a stuck run; this only
    makes a stall diagnosable.
    """
    t0 = time.monotonic()
    last_event_id = -1
    last_progress = time.monotonic()
    warned = False
    while True:
        run = tick(conn, run_id, verifier=verifier)
        if run.status.terminal:
            return run
        row = conn.execute("SELECT max(id) AS m FROM events WHERE run_id = %s", (run_id,)).fetchone()
        ev = int(row["m"] or 0) if row else 0
        if ev != last_event_id:
            last_event_id, last_progress, warned = ev, time.monotonic(), False
        elif not warned and time.monotonic() - last_progress > stall_warn_s:
            warned = True
            log.warning("run %s: no events for %.0fs — %s", run_id, stall_warn_s, stall_report(conn, run_id))
        if timeout_s is not None and time.monotonic() - t0 > timeout_s:
            raise TimeoutError(f"run {run_id} still {run.status} after {timeout_s}s: {stall_report(conn, run_id)}")
        time.sleep(tick_s)


def open_runs(conn: Conn, pools: list[str] | tuple[str, ...] | None = None) -> list[UUID]:
    if pools is None:
        rows = conn.execute(
            "SELECT id FROM runs WHERE status NOT IN ('PASSED','FAILED','ABORTED') ORDER BY created_at"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id FROM runs WHERE status NOT IN ('PASSED','FAILED','ABORTED') AND pool = ANY(%s) ORDER BY created_at",
            (list(pools),),
        ).fetchall()
    return [r["id"] for r in rows]


def orchestrate_forever(
    conn: Conn,
    *,
    verifier: Verifier | None = None,
    tick_s: float = 0.5,
    stop=None,
    pools: list[str] | tuple[str, ...] | None = None,
) -> None:
    """Service mode (docker compose `orchestrator`): tick every open run in `pools` (None = all) until asked to stop."""
    while stop is None or not stop.is_set():
        for rid in open_runs(conn, pools):
            try:
                tick(conn, rid, verifier=verifier)
            except Exception:
                log.exception("tick failed for run %s", rid)
                conn.rollback()
        time.sleep(tick_s)
