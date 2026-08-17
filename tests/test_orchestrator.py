"""End-to-end substrate tests with stub workers and a real Postgres. No LLM, no API key.

Maps to docs/evaluation.md pass criteria: A3 (concurrency), A4 (isolation: separate connections/threads,
per-attempt scoping), A5 (worker death), A6 (artifact consistency on retry), A9 (verifier decides),
A10 (termination inside budget + auditable). A7/A8 need the LLM planner and land in M2.
"""

import threading
import time

import pytest

from mas import metrics
from mas.artifacts import store
from mas.db.events import for_run
from mas.models.enums import ArtifactStatus, AttemptStatus, RunStatus, TaskStatus
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.orchestrator import state_machine as sm
from mas.verifier.stub import StubVerifier
from tests.conftest import CAPS, default_budgets, diamond, execute

pytestmark = pytest.mark.db


def statuses(conn, run_id) -> dict[str, TaskStatus]:
    return {t.key: t.status for t in sm.tasks_for_run(conn, run_id)}


def attempts_of(conn, run_id, key) -> list:
    tasks = {t.key: t.id for t in sm.tasks_for_run(conn, run_id)}
    return [a for a in sm.attempts_for_run(conn, run_id) if a.task_id == tasks[key]]


# ----------------------------------------------------------------------------- happy path / A3


def test_diamond_passes_with_real_concurrency(conn):
    out = execute(conn, diamond(), workers=3, stub_sleep=0.4)
    assert out.run.status is RunStatus.PASSED and out.run.verdict == "PASS"
    assert set(statuses(conn, out.run.id).values()) == {TaskStatus.COMPLETED}
    m = metrics.compute(conn, out.run.id)
    assert m.attempts == 5 and m.attempts_by_status == {"SUCCESS": 5}
    assert m.max_concurrent_attempts >= 3, m  # T2/T3/T4 ran at the same time (A3)
    # `max_concurrent_attempts` is the deterministic A3 criterion. Efficiency includes orchestration/verifier time in
    # its denominator and can fall below 1 on a loaded CI host even though three attempts genuinely overlapped.
    assert m.parallelism_efficiency and m.parallelism_efficiency > 0
    # integration artifact accepted by verifier PASS; others remain candidates (chosen-for-final semantics)
    integ = next(t for t in sm.tasks_for_run(conn, out.run.id) if t.key == "T5")
    assert [a.status for a in store.for_task(conn, integ.id)] == [ArtifactStatus.ACCEPTED]
    # verification artifact recorded at run level
    ver = conn.execute("SELECT * FROM artifacts WHERE run_id=%s AND type='verification'", (out.run.id,)).fetchall()
    assert len(ver) == 1 and ver[0]["meta"]["passed"] is True
    # A4: work was spread over separate worker threads/connections
    assert len({a.worker_id for a in sm.attempts_for_run(conn, out.run.id)}) >= 2


def test_dependency_order_is_respected(conn):
    out = execute(conn, diamond(), workers=4, stub_sleep=0.2)
    evs = for_run(conn, out.run.id)
    idx = {(e.type, e.payload.get("key")): e.id for e in evs}
    assert idx[("task.completed", "T1")] < idx[("task.ready", "T2")]
    assert idx[("task.completed", "T1")] < idx[("task.ready", "T4")]
    assert max(idx[("task.completed", k)] for k in ("T2", "T3", "T4")) < idx[("task.ready", "T5")]
    assert idx[("task.completed", "T5")] < idx[("run.verifying", None)] < idx[("run.passed", None)]


def test_downstream_context_receives_upstream_outputs(conn):
    out = execute(conn, diamond(), workers=2, stub_sleep=0.1)
    tasks = {t.key: t.id for t in sm.tasks_for_run(conn, out.run.id)}
    t2_art = store.for_task(conn, tasks["T2"])[0]
    assert t2_art.type == "git_commit"
    t1_art = store.for_task(conn, tasks["T1"])[0]
    assert str(t1_art.id) in t2_art.meta["inputs"]  # T2 saw T1's output, not the whole run
    integ_art = store.for_task(conn, tasks["T5"])[0]
    assert set(integ_art.meta["inputs"]) == {str(store.for_task(conn, tasks[k])[0].id) for k in ("T2", "T3", "T4")}


# ----------------------------------------------------------------------------- retries / A6


def test_failed_attempt_is_retried_then_succeeds(conn):
    out = execute(conn, diamond({"T3": {"fail_attempts": 1}}), workers=3, stub_sleep=0.1)
    assert out.run.status is RunStatus.PASSED
    atts = attempts_of(conn, out.run.id, "T3")
    assert [a.status for a in atts] == [AttemptStatus.FAILED, AttemptStatus.SUCCESS]
    assert atts[0].failure_reason.startswith("scripted failure")
    types = [e.type for e in for_run(conn, out.run.id) if e.payload.get("key") == "T3"]
    # RUNNING → RETRYABLE → READY is written as two events (audit trail matches the state machine)
    assert types.index("task.retryable") < types.index("task.ready", types.index("task.retryable"))
    m = metrics.compute(conn, out.run.id)
    assert m.retries == 1


def test_agent_crash_becomes_failed_attempt_not_hang(conn):
    out = execute(conn, diamond({"T2": {"crash_attempts": 1}}), workers=2, stub_sleep=0.1)
    assert out.run.status is RunStatus.PASSED
    atts = attempts_of(conn, out.run.id, "T2")
    assert atts[0].status is AttemptStatus.FAILED and "agent crashed" in atts[0].failure_reason
    assert atts[1].status is AttemptStatus.SUCCESS


def test_output_contract_unmet_is_a_failed_attempt(conn):
    # T4 publishes a document when a git_commit is required → contract unmet → retry with the same script → exhausts
    out = execute(
        conn,
        diamond({"T4": {"artifacts": [{"type": "document", "name": "oops.md"}]}}),
        workers=2,
        stub_sleep=0.05,
        budgets=default_budgets(max_attempts_per_task=2),
    )
    assert out.run.status is RunStatus.FAILED
    atts = attempts_of(conn, out.run.id, "T4")
    assert all(a.status is AttemptStatus.FAILED and "output contract unmet: git_commit" in a.failure_reason for a in atts)
    assert len(atts) == 2


def test_retries_exhausted_fails_run_and_blocks_downstream(conn):
    out = execute(
        conn, diamond({"T2": {"fail_attempts": 99}}), workers=3, stub_sleep=0.05, budgets=default_budgets(max_attempts_per_task=2)
    )
    assert out.run.status is RunStatus.FAILED
    assert out.run.verdict.startswith("FAIL:task T2 failed")
    st = statuses(conn, out.run.id)
    assert st["T2"] is TaskStatus.FAILED
    assert st["T5"] in {TaskStatus.BLOCKED, TaskStatus.CANCELLED}  # integration can never run
    assert all(s.terminal for s in st.values())  # nothing left dangling
    assert len(attempts_of(conn, out.run.id, "T2")) == 2


def test_retry_starts_clean_but_prior_candidates_persist_as_hints(conn):
    # first attempt of T3 publishes an artifact and then dies; second attempt succeeds. Both artifact rows exist;
    # only the successful attempt's artifact is a dependency output for T5.
    out = execute(conn, diamond({"T3": {"die_attempts": 1, "artifacts": [{"type": "git_commit"}]}}), workers=3, stub_sleep=0.1)
    # note: die_attempts returns before publishing, so attempt 1 has no artifacts; emulate a hint by publishing manually
    assert out.run.status is RunStatus.PASSED
    atts = attempts_of(conn, out.run.id, "T3")
    assert [a.status for a in atts] == [AttemptStatus.ABANDONED, AttemptStatus.SUCCESS]
    with conn.transaction():
        hint = store.publish(
            conn, run_id=out.run.id, task_id=atts[0].task_id, attempt_id=atts[0].id, type="git_commit", ref="stub:hint"
        )
    assert hint.status is ArtifactStatus.CANDIDATE
    # dependency-output query excludes artifacts from non-SUCCESS attempts
    from mas.workers.runtime import _dependency_outputs

    t5 = next(t for t in sm.tasks_for_run(conn, out.run.id) if t.key == "T5")
    outs = _dependency_outputs(conn, t5.id)
    assert hint.id not in {a.id for a in outs}
    assert len(outs) == 3


# ----------------------------------------------------------------------------- worker death / A5


def test_worker_death_is_recovered_by_reaper(conn):
    out = execute(conn, diamond({"T2": {"die_attempts": 1}}), workers=3, stub_sleep=0.2, budgets=default_budgets(lease_s=1))
    assert out.run.status is RunStatus.PASSED
    atts = attempts_of(conn, out.run.id, "T2")
    assert [a.status for a in atts] == [AttemptStatus.ABANDONED, AttemptStatus.SUCCESS]
    assert "lease expired" in atts[0].failure_reason
    assert atts[0].worker_id != atts[1].worker_id  # another worker picked it up
    dead = [w for w in out.workers if w.stats.died]
    assert len(dead) == 1
    m = metrics.compute(conn, out.run.id)
    assert m.abandoned == 1 and m.retries == 1


def test_hung_attempt_times_out_and_zombie_report_is_rejected(conn):
    # T3 hangs; per-attempt runtime limit is 1s → reaper marks TIMEOUT; heartbeat then sees not-RUNNING and cancels
    # the agent; the worker's late report is rejected as stale. Attempt 2 succeeds.
    out = execute(
        conn,
        diamond({"T3": {"hang_attempts": 1}}),
        workers=3,
        stub_sleep=0.1,
        budgets=default_budgets(lease_s=1, max_attempt_runtime_s=1),
    )
    assert out.run.status is RunStatus.PASSED
    atts = attempts_of(conn, out.run.id, "T3")
    assert atts[0].status is AttemptStatus.TIMEOUT
    assert atts[1].status is AttemptStatus.SUCCESS
    assert sum(w.stats.stale for w in out.workers) == 1
    # every attempt is terminal, only one attempt per task except T3
    m = metrics.compute(conn, out.run.id)
    assert m.attempts == 6 and m.timeouts == 1


# ----------------------------------------------------------------------------- budgets / A10


def test_wallclock_budget_aborts_run_and_cancels_everything(conn):
    out = execute(conn, diamond(), workers=3, stub_sleep=5.0, budgets=default_budgets(max_wallclock_s=1, lease_s=1))
    assert out.run.status is RunStatus.ABORTED
    assert out.run.verdict.startswith("ABORTED:max_wallclock_s")
    assert all(s.terminal for s in statuses(conn, out.run.id).values())
    assert all(a.status is AttemptStatus.CANCELLED for a in sm.attempts_for_run(conn, out.run.id))
    assert out.run.finished_at is not None
    m = metrics.compute(conn, out.run.id)
    assert m.wall_clock_s is not None and m.wall_clock_s < 5  # did not wait for the 5s "work"


def test_run_deadline_aborts(conn):
    from datetime import UTC, datetime, timedelta

    out = execute(
        conn, diamond(), workers=1, stub_sleep=3.0, budgets=default_budgets(deadline_at=datetime.now(UTC) + timedelta(seconds=1))
    )
    assert out.run.status is RunStatus.ABORTED and "deadline" in out.run.verdict


def test_max_concurrency_one_serialises_config_c(conn):
    out = execute(conn, diamond(), workers=3, stub_sleep=0.3, budgets=default_budgets(max_concurrency=1))
    assert out.run.status is RunStatus.PASSED
    m = metrics.compute(conn, out.run.id)
    assert m.max_concurrent_attempts == 1
    assert m.parallelism_efficiency is not None and m.parallelism_efficiency <= 1.0


# ----------------------------------------------------------------------------- verifier / A9


def test_verifier_alone_decides_pass(conn):
    v = StubVerifier(passed=False, report={"failed": ["GET /x → 500"]})
    out = execute(conn, diamond(), workers=3, stub_sleep=0.05, verifier=v)
    assert out.run.status is RunStatus.FAILED
    # no planner here → the run may not repair; the verdict says so (13-lite) with a reason code
    assert out.run.verdict.startswith("FAIL:verification failed") and "planner" in out.run.verdict
    assert out.run.verdict_reason == "UNRECOVERABLE_FAILURE"
    assert v.calls == 1
    integ = next(t for t in sm.tasks_for_run(conn, out.run.id) if t.key == "T5")
    assert integ.status is TaskStatus.COMPLETED  # agents "succeeded"; the verifier still said no
    assert [a.status for a in store.for_task(conn, integ.id)] == [ArtifactStatus.CANDIDATE]  # not accepted
    ev = [e for e in for_run(conn, out.run.id) if e.type == "verify.failed"]
    assert len(ev) == 1 and ev[0].payload["report"]["failed"] == ["GET /x → 500"]


def test_verifier_accepts_only_winning_attempts_outputs(conn):
    """P1: integration attempt 1 fails but leaves a candidate git_commit behind; attempt 2 succeeds.
    On PASS only attempt 2's artifact may become `accepted`; attempt 1's stays a candidate hint."""
    out = execute(conn, diamond({"T5": {"fail_attempts": 1, "publish_on_fail": True}}), workers=2, stub_sleep=0.05)
    assert out.run.status is RunStatus.PASSED
    integ = next(t for t in sm.tasks_for_run(conn, out.run.id) if t.key == "T5")
    atts = attempts_of(conn, out.run.id, "T5")
    assert [a.status for a in atts] == [AttemptStatus.FAILED, AttemptStatus.SUCCESS]
    arts = store.for_task(conn, integ.id)
    by_attempt = {a.attempt_id: a.status for a in arts}
    assert by_attempt[atts[0].id] is ArtifactStatus.CANDIDATE  # loser: hint only
    assert by_attempt[atts[1].id] is ArtifactStatus.ACCEPTED  # winner
    assert [a.id for a in store.accepted_for_run(conn, out.run.id)] == [next(a.id for a in arts if a.attempt_id == atts[1].id)]


def test_verifying_is_reentrant_after_orchestrator_crash(conn, monkeypatch):
    """P1: if the orchestrator dies after RUNNING→VERIFYING but before the verdict, the next tick (any process)
    must finish verification instead of leaving the run stranded in VERIFYING."""
    from mas.orchestrator import scheduler as sched

    real_verify = sched._verify
    calls = {"n": 0}

    def crash_once(conn_, run_id, verifier, workspace=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("orchestrator process died mid-verification")
        return real_verify(conn_, run_id, verifier, workspace)

    monkeypatch.setattr(sched, "_verify", crash_once)
    with pytest.raises(RuntimeError, match="died mid-verification"):
        execute(conn, diamond(), workers=2, stub_sleep=0.05)
    run_row = conn.execute("SELECT id, status FROM runs").fetchone()
    assert run_row["status"] == "VERIFYING"  # stranded — exactly the bug
    final = sched.tick(conn, run_row["id"], verifier=StubVerifier(passed=True))  # a fresh tick, e.g. another orchestrator
    assert final.status is RunStatus.PASSED and final.verdict == "PASS"
    assert calls["n"] == 2


def test_verify_lock_serialises_orchestrators(conn):
    """While one session holds the verify lock for a run, another orchestrator's tick is a no-op; after release it verifies."""
    from mas.db import connect
    from mas.orchestrator import scheduler as sched
    from tests.conftest import DB_URL

    # drive a run to VERIFYING without verifying: verifier that raises BaseException-free crash via monkeypatch is
    # heavier; simpler: complete the run with a lock already held by another session so _verify backs off.
    other = connect(DB_URL)
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    assert other.execute("SELECT pg_try_advisory_lock(%s, hashtext(%s)) AS ok", (sched.VERIFY_LOCK_NS, str(run.id))).fetchone()[
        "ok"
    ]
    try:
        import threading

        from mas.workers.runtime import Worker, run_worker_thread, wait_all
        from mas.workers.stub import StubAgent

        stop = threading.Event()
        ws = [
            Worker(f"w{i}", list(CAPS), StubAgent({"sleep_s": 0.05}), database_url=DB_URL, poll_s=0.05, run_id=run.id)
            for i in range(2)
        ]
        threads = [run_worker_thread(w, stop) for w in ws]
        try:
            with pytest.raises(TimeoutError):
                sched.run_until_terminal(conn, run.id, verifier=StubVerifier(passed=True), tick_s=0.1, timeout_s=4)
        finally:
            stop.set()
            wait_all(threads, 5)
        assert sm.get_run(conn, run.id).status is RunStatus.VERIFYING  # backed off: someone else holds the lock
        other.execute("SELECT pg_advisory_unlock(%s, hashtext(%s))", (sched.VERIFY_LOCK_NS, str(run.id)))
        final = sched.tick(conn, run.id, verifier=StubVerifier(passed=True))
        assert final.status is RunStatus.PASSED
    finally:
        other.close()


def test_verifier_crash_is_a_fail_not_a_hang(conn):
    class Boom:
        name = "boom"

        def verify(self, request):
            raise RuntimeError("kaboom")

    out = execute(conn, diamond(), workers=2, stub_sleep=0.05, verifier=Boom())
    assert out.run.status is RunStatus.FAILED
    ver = conn.execute("SELECT meta FROM artifacts WHERE run_id=%s AND type='verification'", (out.run.id,)).fetchone()
    assert "kaboom" in ver["meta"]["report"]["error"]


# ----------------------------------------------------------------------------- audit / A10


def test_run_is_replayable_from_events(conn):
    out = execute(conn, diamond({"T4": {"fail_attempts": 1}}), workers=3, stub_sleep=0.1)
    evs = for_run(conn, out.run.id)
    types = [e.type for e in evs]
    assert types[0] == "run.created" and types[-1] == "run.passed"
    # reconstruct final task states purely from task.* events
    final: dict[str, str] = {}
    for e in evs:
        if e.type.startswith("task.") and e.type not in {"task.created", "task.new_work_required"} and e.payload.get("key"):
            final[e.payload["key"]] = e.type.split(".", 1)[1].upper()
    assert final == {k: v.value for k, v in statuses(conn, out.run.id).items()}
    # every attempt has a leased event and a terminal event
    leased = sum(1 for t in types if t == "attempt.leased")
    settled = sum(
        1
        for t in types
        if t in {"attempt.success", "attempt.failed", "attempt.timeout", "attempt.abandoned", "attempt.cancelled"}
    )
    assert leased == settled == metrics.compute(conn, out.run.id).attempts


def test_usage_is_accounted_on_attempts_and_run(conn):
    usage = {"model": "stub", "input_tokens": 1000, "output_tokens": 100, "cost_usd": 0.01}
    d = diamond({k: {"usage": usage} for k in ("T1", "T2", "T3", "T4", "T5")})
    out = execute(conn, d, workers=2, stub_sleep=0.02)
    assert out.run.status is RunStatus.PASSED
    m = metrics.compute(conn, out.run.id)
    assert m.input_tokens == 5000 and m.output_tokens == 500 and abs(m.cost_usd - 0.05) < 1e-9
    assert out.run.tokens_used == 5500 and abs(out.run.cost_used_usd - 0.05) < 1e-6


def test_token_budget_aborts(conn):
    usage = {"model": "stub", "input_tokens": 1000, "output_tokens": 0, "cost_usd": 0.0}
    d = diamond({k: {"usage": usage} for k in ("T1", "T2", "T3", "T4", "T5")})
    # rule 8 admits the plan only if the run can fund one attempt per task at its per-attempt allocation (5 x 200 <= 1500);
    # the stubs then self-report 1000 tokens each and the run's own token budget aborts it (I-4)
    out = execute(conn, d, workers=1, stub_sleep=0.05, budgets=default_budgets(max_tokens=1500, max_attempt_tokens=200))
    assert out.run.status is RunStatus.ABORTED and "max_tokens" in out.run.verdict


def test_new_work_required_is_recorded_for_replanner(conn):
    out = execute(conn, diamond({"T2": {"new_work_required": "need a migration task"}}), workers=2, stub_sleep=0.02)
    assert out.run.status is RunStatus.PASSED  # M1: recorded only; step 13 turns it into a re-plan trigger
    ev = [e for e in for_run(conn, out.run.id) if e.type == "task.new_work_required"]
    assert len(ev) == 1 and ev[0].payload["detail"] == "need a migration task"


def test_pools_isolate_runs_from_foreign_workers(conn):
    """A run in pool 'local:x' must never be served by workers pinned to pool 'default' (compose services)."""
    from mas.orchestrator import leases

    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS), pool="local:x")
    scheduler.tick(conn, run.id)  # T1 → READY
    assert leases.claim_task(conn, worker_id="foreign", capabilities=list(CAPS), pools=["default"]) is None
    assert leases.claim_task(conn, worker_id="foreign2", capabilities=list(CAPS), pools=["other", "default"]) is None
    c = leases.claim_task(conn, worker_id="local", capabilities=list(CAPS), pools=["local:x"])
    assert c is not None and c.task.key == "T1" and c.run.pool == "local:x"
    # pools=None (unpinned) may serve anything; run_id pin still applies
    other = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    scheduler.tick(conn, other.id)
    c2 = leases.claim_task(conn, worker_id="any", capabilities=list(CAPS), pools=None)
    assert c2 is not None and c2.run.id == other.id and c2.run.pool == "default"
    assert set(scheduler.open_runs(conn, ["default"])) == {other.id}
    assert set(scheduler.open_runs(conn)) == {run.id, other.id}


def test_worker_threads_stop_when_run_terminates(conn):
    out = execute(conn, diamond(), workers=2, stub_sleep=0.05)
    time.sleep(0.2)
    assert all(not t.is_alive() for t in out.threads)
    assert threading.active_count() < 20


def test_wallclock_budget_is_one_clock_from_creation(conn):
    """A long wait before RUNNING must not grant a fresh execution budget (budget defect, 2026-08-16)."""
    from datetime import UTC, datetime, timedelta

    from mas.orchestrator import budgets as budget_rules

    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(max_wallclock_s=10), capabilities=set(CAPS))
    # simulate: created 9s ago, started running just now
    with conn.transaction():
        conn.execute("UPDATE runs SET created_at = now() - interval '9 seconds', started_at = now() WHERE id = %s", (run.id,))
    r = sm.get_run(conn, run.id)
    assert budget_rules.violation(r, now=datetime.now(UTC)) is None
    assert "max_wallclock_s" in (budget_rules.violation(r, now=datetime.now(UTC) + timedelta(seconds=2)) or "")
    # a run that started 2s ago but was created 12s ago is over budget
    with conn.transaction():
        conn.execute(
            "UPDATE runs SET created_at = now() - interval '12 seconds', started_at = now() - interval '2 seconds' WHERE id = %s",
            (run.id,),
        )
    assert "max_wallclock_s" in (budget_rules.violation(sm.get_run(conn, run.id)) or "")
