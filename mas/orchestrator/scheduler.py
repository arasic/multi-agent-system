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
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any
from uuid import UUID

from mas.artifacts import store
from mas.db.connection import Conn, connect
from mas.db.events import emit
from mas.models.enums import (
    INTEGRATION_CAPABILITY,
    TASK_TERMINAL,
    TASK_UPSTREAM_BLOCKING,
    ArtifactStatus,
    RunStatus,
    TaskStatus,
    VerdictReason,
)
from mas.models.types import Run
from mas.orchestrator import budgets as budget_rules
from mas.orchestrator import progress
from mas.orchestrator import state_machine as sm
from mas.orchestrator.leases import reap_expired
from mas.planner.planner import Planner
from mas.verifier.base import (
    DeferredVerification,
    MissingVerifier,
    VerificationRequest,
    VerificationResult,
    VerificationStatus,
    Verifier,
)

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
        FOR NO KEY UPDATE OF t SKIP LOCKED
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
        FOR NO KEY UPDATE OF t SKIP LOCKED
        """,
        (run_id, [s.value for s in TASK_UPSTREAM_BLOCKING]),
    ).fetchall()
    for r in rows:
        sm.transition_task(conn, r["id"], TaskStatus.BLOCKED, payload={"reason": "upstream task failed/blocked/cancelled"})
    return [r["key"] for r in rows]


def _integration_task(conn: Conn, run_id: UUID) -> dict[str, Any] | None:
    """The run's *current* integration sink: the newest one — an amendment (bounded repair) adds a new sink and the old
    one becomes COMPLETED history whose outputs are never accepted."""
    return conn.execute(
        "SELECT * FROM tasks WHERE run_id = %s AND capability = %s ORDER BY created_at DESC, key DESC LIMIT 1",
        (run_id, INTEGRATION_CAPABILITY),
    ).fetchone()


VERIFY_LOCK_NS = 0x4D415356  # 'MASV': advisory-lock namespace for the verifier stage


def _promote_integration_ref(run_id: UUID, sha: str, workspace: Any | None) -> None:
    """On PASS, point run/<run>/integration at the accepted integration commit (convenience ref; the artifact is truth).
    Deterministic filesystem/git op; skipped when this orchestrator has no workspace (e.g. no shared volume)."""
    try:
        if workspace is None:
            from mas.workers.workspace import workspace_from_settings

            workspace = workspace_from_settings()
        workspace.promote(run_id, "integration", sha)
    except Exception:  # never let a ref update fail a PASS
        log.warning("could not promote integration ref for run %s", run_id, exc_info=True)


def _try_verify_lock(conn: Conn, run_id: UUID) -> bool:
    """Session-level advisory lock: one verifier per run at a time; auto-released if this process dies."""
    row = conn.execute("SELECT pg_try_advisory_lock(%s, hashtext(%s)) AS ok", (VERIFY_LOCK_NS, str(run_id))).fetchone()
    return bool(row and row["ok"])


def _verify_unlock(conn: Conn, run_id: UUID) -> None:
    try:
        conn.execute("SELECT pg_advisory_unlock(%s, hashtext(%s))", (VERIFY_LOCK_NS, str(run_id)))
    except Exception:  # connection may be gone; the lock dies with the session anyway
        log.debug("advisory unlock failed", exc_info=True)


def _truncate_report(report: dict[str, Any], limit: int = 8192) -> dict[str, Any]:
    """The verification artifact holds the full report; the event log gets a bounded copy (long stdout/stderr cut)."""
    out: dict[str, Any] = {}
    for k, v in report.items():
        if isinstance(v, str) and len(v) > limit:
            out[k] = v[:limit] + f"…[truncated {len(v) - limit} chars; full text in the verification artifact]"
        else:
            out[k] = v
    return out


def _verification_request(conn: Conn, run: Run, workspace: Any | None) -> VerificationRequest:
    """Resolve mutable DB/workspace state before crossing the verifier boundary.

    The verifier gets no connection and cannot publish evidence or transition state. A
    missing/ambiguous integration commit remains explicit in the request and the real
    verifier rejects it; the explicit test stub may ignore it.
    """
    integ = _integration_task(conn, run.id)
    commits = [] if integ is None else [a.ref for a in store.outputs_of_task(conn, integ["id"]) if a.type == "git_commit"]
    sha = commits[0] if len(commits) == 1 else None
    repository = None
    repo_path = getattr(workspace, "repo_path", None)
    if callable(repo_path):
        try:
            repository = repo_path(run.id)
        except Exception:
            log.warning("could not resolve verification repository for run %s", run.id, exc_info=True)
    expected = None
    frozen = conn.execute(
        "SELECT meta FROM artifacts WHERE run_id = %s AND type = 'acceptance_contract' AND status = 'accepted' "
        "ORDER BY created_at DESC, id DESC LIMIT 1",
        (run.id,),
    ).fetchone()
    if frozen is not None:  # ad-hoc goal: the approved contract pins the suite the verifier must run (ADR-007)
        expected = frozen["meta"].get("suite_sha256")
    return VerificationRequest(
        run_id=run.id, benchmark=run.benchmark, repository=repository, commit_sha=sha, expected_suite_sha256=expected
    )


def _verify(conn: Conn, run_id: UUID, verifier: Verifier, workspace: Any | None = None) -> Run:
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
            result = verifier.verify(_verification_request(conn, run, workspace))
            if not isinstance(result, VerificationResult):
                raise TypeError(f"verifier returned {type(result).__name__}, expected VerificationResult")
        except Exception as e:  # verifier crash is a FAIL with a reason, never a hang
            log.exception("verifier crashed")
            result = VerificationResult.fail(
                f"verifier crashed: {e!r}",
                status=VerificationStatus.ERROR,
                evidence={"run_id": str(run_id), "error": f"verifier crashed: {e!r}"},
            )
        result_passed, report = result.passed, dict(result.report)
        request = _verification_request(conn, run, workspace)
        with conn.transaction():
            locked = sm.lock_run(conn, run_id)  # lock order: run first, then artifact rows / inserts
            if locked.status is not RunStatus.VERIFYING:
                return locked
            n = conn.execute(
                "SELECT count(*) AS n FROM artifacts WHERE run_id = %s AND type = 'verification'", (run_id,)
            ).fetchone()["n"]  # type: ignore[index]
            store.publish(
                conn,
                run_id=run_id,
                type="verification",
                ref=f"verify:{run_id}:{n + 1}",
                meta={
                    "passed": result_passed,
                    "status": result.status.value,
                    "verifier": getattr(verifier, "name", "?"),
                    "report": report,
                },
            )
            emit(
                conn,
                run_id,
                "verify.passed" if result_passed else "verify.failed",
                payload={"verifier": getattr(verifier, "name", "?"), "report": _truncate_report(report)},
            )
            if result_passed:
                integ = _integration_task(conn, run_id)
                if integ is not None:
                    # accept only the winning (SUCCESS) attempt's outputs — earlier attempts' candidates stay hints
                    for a in store.outputs_of_task(conn, integ["id"]):
                        if a.status is ArtifactStatus.CANDIDATE:
                            store.accept(conn, a.id)
                        if a.type == "git_commit":
                            _promote_integration_ref(run_id, a.ref, workspace)
                return sm.pass_run(conn, run_id)
            if result.status is not VerificationStatus.FAIL:
                # Not the code's failure: the suite did not run to a verdict on the checks (TIMEOUT: the trusted runner
                # itself did not finish — per-check timeouts are check FAILs; ERROR: verifier/sandbox crashed; INVALID:
                # the suite as frozen/configured could not be executed or validated). Never a repair trigger — an
                # amendment cannot fix infrastructure — and never a fingerprint: a coded terminal verdict instead.
                if result.status is VerificationStatus.INVALID and getattr(verifier, "name", "") != "missing":
                    code = VerdictReason.UNSUPPORTED
                else:
                    code = VerdictReason.UNRECOVERABLE_FAILURE
                return sm.fail_run(
                    conn,
                    run_id,
                    f"verification not completed ({result.status.value}): {result.reason or 'no reason given'}"[:400],
                    code=code,
                )
            # bounded repair (step 13-lite, ADR-008 §7): a deterministic decision from the progress fingerprint and
            # max_replans — never from the planner. REPLANNING is then driven by the orchestrator's next tick.
            return _after_fail(conn, locked, report, request, workspace)
    finally:
        _verify_unlock(conn, run_id)


def _integration_hash(run: Run, request: VerificationRequest, workspace: Any | None) -> str | None:
    """The diff identity of what was verified: the integration commit's tree hash when a git workspace is available
    (a repair that changed nothing repeats it), else the opaque ref."""
    sha = request.commit_sha
    if not sha:
        return None
    if workspace is None:
        try:
            from mas.workers.workspace import workspace_from_settings

            workspace = workspace_from_settings()
        except Exception:  # no workspace configured here (e.g. --workspace none): the ref is the identity
            workspace = None
    tree = getattr(workspace, "tree_sha", None)
    if callable(tree):
        try:
            t = tree(run.id, sha)
            if t:
                return f"tree:{t}"
        except Exception:
            log.debug("tree hash unavailable for %s", sha, exc_info=True)
    return f"ref:{sha}"


def _after_fail(conn: Conn, run: Run, report: dict[str, Any], request: VerificationRequest, workspace: Any | None) -> Run:
    """Caller holds the run lock (VERIFYING). Record the failure fingerprint, then decide: repair or terminal verdict."""
    fp = progress.failure_fingerprint(
        report,
        integration_hash=_integration_hash(run, request, workspace),
        accepted=[(a.type, a.ref) for a in store.accepted_for_run(conn, run.id)],
    )
    prev = [
        dict(e["payload"])
        for e in conn.execute(
            "SELECT payload FROM events WHERE run_id = %s AND type = 'verify.fingerprint' ORDER BY id", (run.id,)
        ).fetchall()
    ]
    decision = progress.decide_after_fail(fp, previous=prev, replans_used=run.replans_used, max_replans=run.budgets.max_replans)
    emit(
        conn,
        run.id,
        "verify.fingerprint",
        payload={
            **fp.as_dict(),
            "value": fp.value,
            "cycle": len(prev),
            "decision": decision.action,
            "reason": decision.reason.value if decision.reason else None,
            "detail": decision.detail,
            **decision.payload,
        },
    )
    if decision.action == "replan":
        return sm.start_replan(
            conn, run.id, payload={"cycle": len(prev) + 1, "failing_checks": list(fp.failing_checks), "detail": decision.detail}
        )
    assert decision.reason is not None
    return sm.fail_run(conn, run.id, f"verification failed: {decision.detail}", code=decision.reason)


def gc_workspace(run_id: UUID, workspace: Any | None) -> None:
    """A terminal run keeps no worktrees: remove whatever attempts left behind (crashed / abandoned workers leave theirs
    on purpose while the run is live). Best effort, idempotent; the run's bare repo (its history) stays."""
    try:
        if workspace is None:
            from mas.workers.workspace import workspace_from_settings

            workspace = workspace_from_settings()
        gc = getattr(workspace, "gc_run", None)
        if callable(gc):
            n = gc(run_id)
            if n:
                log.info("run %s: removed %d leftover worktree(s)", run_id, n)
    except Exception:  # never let cleanup change an outcome
        log.warning("worktree GC failed for run %s", run_id, exc_info=True)


def tick(
    conn: Conn,
    run_id: UUID,
    *,
    verifier: Verifier | None = None,
    planner: Planner | None = None,
    capabilities: set[str] | None = None,
    workspace: Any | None = None,
) -> Run:
    """One orchestrator step for a run. `planner` is optional: when given, PLANNING/REPLANNING runs are planned here
    (ADR-006 driver / 13-lite amendments); when None, someone else drives planning and PLANNING is left alone.
    When the run reaches a terminal state its worktrees are garbage-collected.
    """
    run = _tick(conn, run_id, verifier=verifier, planner=planner, capabilities=capabilities, workspace=workspace)
    if run.status.terminal:
        gc_workspace(run_id, workspace)
    return run


def _tick(
    conn: Conn,
    run_id: UUID,
    *,
    verifier: Verifier | None = None,
    planner: Planner | None = None,
    capabilities: set[str] | None = None,
    workspace: Any | None = None,
) -> Run:
    verifier = verifier or MissingVerifier()
    reap_expired(conn, run_id)

    # planning happens outside the run-row lock: the planner may take a while (LLM at step 11); REPLANNING = amendment
    if planner is not None:
        cur = sm.get_run(conn, run_id)
        if cur.status in {RunStatus.CREATED, RunStatus.PLANNING, RunStatus.REPLANNING}:
            from mas.orchestrator import runs as runs_mod  # local import: runs imports this module's siblings

            budget_reason = budget_rules.violation(cur)
            if budget_reason:
                with conn.transaction():
                    return sm.abort_run(conn, run_id, budget_reason)
            try:
                runs_mod.plan_run(conn, run_id, planner, capabilities=capabilities or set())
            except runs_mod.InvalidDag:
                pass  # run is already FAILED with a verdict; fall through to the normal path

    do_verify = False
    with conn.transaction():
        row = conn.execute("SELECT * FROM runs WHERE id = %s FOR NO KEY UPDATE", (run_id,)).fetchone()
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
                # step 13 (full): task-FAILED is a re-plan trigger; 13-lite repairs verifier failures only
                return sm.fail_run(
                    conn,
                    run_id,
                    f"task {failed['key']} failed (retries exhausted)",
                    code=VerdictReason.UNRECOVERABLE_FAILURE,
                )

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
                    return sm.fail_run(
                        conn,
                        run_id,
                        "no runnable tasks left and integration not completed",
                        code=VerdictReason.UNRECOVERABLE_FAILURE,
                    )
        elif run.status is RunStatus.VERIFYING:
            # found already VERIFYING (we or another orchestrator started it and did not finish): retry the stage
            do_verify = True
        elif run.status is RunStatus.REPLANNING and planner is None:
            # the verifier stage decided the run may repair, but nobody here can plan an amendment: say so now (a
            # verdict), instead of leaving the run to its wall-clock
            return sm.fail_run(
                conn,
                run_id,
                "verification failed; repair needs a planner and none is configured (--planner)",
                code=VerdictReason.UNRECOVERABLE_FAILURE,
            )

    if do_verify:
        if isinstance(verifier, DeferredVerification):
            return sm.get_run(conn, run_id)  # a verifier service (`mas verify --watch`) will pick this run up
        return _verify(conn, run_id, verifier, workspace)
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
    run_row = conn.execute("SELECT status FROM runs WHERE id = %s", (run_id,)).fetchone()
    pending: list[str] = []
    if run_row and run_row["status"] == RunStatus.AWAITING_INPUT.value:
        q = conn.execute(
            "SELECT meta FROM artifacts WHERE run_id = %s AND type = 'question' ORDER BY created_at DESC, id DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        pending = list(q["meta"].get("questions", [])) if q else []
    return {
        "status": run_row["status"] if run_row else None,
        "open_tasks": {r["key"]: r["status"] for r in tasks},
        "running_attempts": [dict(r) for r in atts],
        "pending_questions": pending,
        "last_event": (last["type"], last["ts"].isoformat()) if last else None,
    }


def run_until_terminal(
    conn: Conn,
    run_id: UUID,
    *,
    verifier: Verifier | None = None,
    planner: Planner | None = None,
    capabilities: set[str] | None = None,
    workspace: Any | None = None,
    tick_s: float = 0.25,
    timeout_s: float | None = None,
    stall_warn_s: float = 20.0,
) -> Run:
    """Tick one run until it reaches a terminal state. Returns the final Run.

    Watchdog: if the event log has not advanced for `stall_warn_s`, log a WARNING with the open tasks and
    RUNNING attempts (lease left, running time). Budgets (I-4) are what actually end a stuck run; this only
    makes a stall diagnosable. A run AWAITING_INPUT is not a stall — it is waiting on a human, on the clock.
    """
    t0 = time.monotonic()
    last_event_id = -1
    last_progress = time.monotonic()
    warned = False
    while True:
        run = tick(conn, run_id, verifier=verifier, planner=planner, capabilities=capabilities, workspace=workspace)
        if run.status.terminal:
            return run
        row = conn.execute("SELECT max(id) AS m FROM events WHERE run_id = %s", (run_id,)).fetchone()
        ev = int(row["m"] or 0) if row else 0
        if ev != last_event_id:
            last_event_id, last_progress, warned = ev, time.monotonic(), False
        elif run.status is RunStatus.AWAITING_INPUT:
            pass  # waiting on a human is not a stall; budgets bound it
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


TICK_LOCK_NS = 0x4D415354  # 'MAST': per-run advisory lock held for the duration of one tick (cross-process)


def _try_tick_lock(conn: Conn, run_id: UUID) -> bool:
    row = conn.execute("SELECT pg_try_advisory_lock(%s, hashtext(%s)) AS ok", (TICK_LOCK_NS, str(run_id))).fetchone()
    return bool(row and row["ok"])


def _tick_unlock(conn: Conn, run_id: UUID) -> None:
    try:
        conn.execute("SELECT pg_advisory_unlock(%s, hashtext(%s))", (TICK_LOCK_NS, str(run_id)))
    except Exception:
        log.debug("tick unlock failed", exc_info=True)


def _tick_job(
    database_url: str | None,
    run_id: UUID,
    verifier: Verifier | None,
    workspace: Any | None,
    planner: Planner | None,
    capabilities: set[str] | None,
) -> Run | None:
    """One tick on its own connection under the per-run tick lock. Returns None if another process holds the run."""
    conn = connect(database_url)
    try:
        if not _try_tick_lock(conn, run_id):
            return None
        try:
            return tick(conn, run_id, verifier=verifier, workspace=workspace, planner=planner, capabilities=capabilities)
        finally:
            _tick_unlock(conn, run_id)
    finally:
        conn.close()


def _service_loop(
    *,
    name: str,
    database_url: str | None,
    select_runs,
    job,
    tick_s: float,
    stop: threading.Event | None,
    max_parallel: int,
) -> None:
    """Bounded executor: at most `max_parallel` runs in flight, one DB connection per job, never the same run in two
    local threads (in-flight set) and never the same run in two processes (job takes a per-run advisory lock).
    A slow job (e.g. a 3-minute acceptance run) never blocks other runs — they proceed on the other executor slots."""
    stop = stop or threading.Event()
    in_flight: dict[UUID, Future] = {}
    scan = connect(database_url)
    try:
        with ThreadPoolExecutor(max_workers=max_parallel, thread_name_prefix=name) as ex:
            while not stop.is_set():
                for rid, fut in list(in_flight.items()):
                    if fut.done():
                        in_flight.pop(rid)
                        exc = fut.exception()
                        if exc is not None:
                            log.error("%s: job for run %s failed: %r", name, rid, exc)
                try:
                    candidates = select_runs(scan)
                except Exception:
                    log.exception("%s: scan failed", name)
                    scan.rollback()
                    candidates = []
                for rid in candidates:
                    if rid in in_flight or len(in_flight) >= max_parallel:
                        continue
                    in_flight[rid] = ex.submit(job, rid)
                stop.wait(tick_s)
            for fut in in_flight.values():  # drain: let running jobs finish (they hold locks and connections)
                try:
                    fut.result(timeout=600)
                except Exception:
                    log.debug("%s: in-flight job failed during drain", name, exc_info=True)
    finally:
        scan.close()


def orchestrate_forever(
    database_url: str | None,
    *,
    verifier: Verifier | None = None,
    tick_s: float = 0.5,
    stop: threading.Event | None = None,
    pools: list[str] | tuple[str, ...] | None = None,
    workspace: Any | None = None,
    planner: Planner | None = None,
    capabilities: set[str] | None = None,
    max_parallel: int = 4,
) -> None:
    """Orchestrator service (docker compose `orchestrator`): tick every open run in `pools` (None = all), concurrently
    and bounded, until asked to stop. With verifier=DeferredVerification() runs stop at VERIFYING for `mas verify`."""
    _service_loop(
        name="orchestrate",
        database_url=database_url,
        select_runs=lambda c: open_runs(c, pools),
        job=lambda rid: _tick_job(database_url, rid, verifier, workspace, planner, capabilities),
        tick_s=tick_s,
        stop=stop,
        max_parallel=max_parallel,
    )


# ----------------------------------------------------------------------------- verifier service


def verifying_runs(conn: Conn, pools: list[str] | tuple[str, ...] | None = None) -> list[UUID]:
    if pools is None:
        rows = conn.execute("SELECT id FROM runs WHERE status = 'VERIFYING' ORDER BY created_at").fetchall()
    else:
        rows = conn.execute(
            "SELECT id FROM runs WHERE status = 'VERIFYING' AND pool = ANY(%s) ORDER BY created_at", (list(pools),)
        ).fetchall()
    return [r["id"] for r in rows]


def _verify_job(database_url: str | None, run_id: UUID, verifier: Verifier, workspace: Any | None) -> Run:
    conn = connect(database_url)
    try:
        run = _verify(conn, run_id, verifier, workspace)
        if run.status.terminal:
            gc_workspace(run_id, workspace)
        return run  # takes the verify advisory lock; re-entrant; DB-free verifier
    finally:
        conn.close()


def verify_once(
    conn: Conn,
    *,
    verifier: Verifier,
    workspace: Any | None = None,
    pools: list[str] | tuple[str, ...] | None = None,
) -> list[tuple[UUID, RunStatus]]:
    """Verify every run currently in VERIFYING (in `pools`) once, on this connection. Tests and `mas verify --once`."""
    out: list[tuple[UUID, RunStatus]] = []
    for rid in verifying_runs(conn, pools):
        run = _verify(conn, rid, verifier, workspace)
        if run.status.terminal:
            gc_workspace(rid, workspace)
        out.append((rid, run.status))
    return out


def verify_forever(
    database_url: str | None,
    *,
    verifier: Verifier,
    tick_s: float = 0.5,
    stop: threading.Event | None = None,
    pools: list[str] | tuple[str, ...] | None = None,
    workspace: Any | None = None,
    max_parallel: int = 2,
) -> None:
    """Verifier service (`mas verify --watch`): a process WITH sandbox access that claims runs in VERIFYING (left there
    by orchestrators running with DeferredVerification) and produces real verdicts. Bounded, one connection per job;
    the verify advisory lock makes it safe to run several of these, and re-entrant after a crash."""
    if isinstance(verifier, DeferredVerification | MissingVerifier):
        raise ValueError("the verifier service needs a real verifier")
    _service_loop(
        name="verify",
        database_url=database_url,
        select_runs=lambda c: verifying_runs(c, pools),
        job=lambda rid: _verify_job(database_url, rid, verifier, workspace),
        tick_s=tick_s,
        stop=stop,
        max_parallel=max_parallel,
    )
