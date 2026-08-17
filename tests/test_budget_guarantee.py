"""Hard total budget and honest failure classification (review of rule 8 / 13-lite, 2026-08-17).

- planner usage is charged to the run (settled from telemetry rows, never self-reported) and can end the run;
- attempt allocations are *reservations*: concurrent attempts can never jointly be handed more than the run has left;
- verifier ERROR / INVALID / TIMEOUT never trigger code repair — they are coded terminal verdicts;
- the observed per-task time used by rule 8 is a lower bound (shortest success), so a slow history cannot falsely reject;
- a terminal run keeps no worktrees: what a crashed worker left behind is garbage-collected."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from mas.db.events import for_run
from mas.models.enums import AttemptStatus, RunStatus, VerdictReason
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.orchestrator import state_machine as sm
from mas.planner.dag import DagSpec
from mas.planner.llm import LLMPlanner
from mas.planner.planner import StubPlanner
from mas.planner.validator import Remaining, validate
from mas.providers.fake import FakeProvider
from mas.providers.telemetry import DbSink
from mas.verifier.base import VerificationResult, VerificationStatus
from mas.verifier.stub import StubVerifier
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.stub import StubAgent
from mas.workers.workspace import GitWorkspace, git_available
from tests.conftest import CAPS, DB_URL, default_budgets, diamond
from tests.test_planner_llm import _contract_json, _dag_json

pytestmark = pytest.mark.db


# ----------------------------------------------------------------------------- planner usage is charged


def test_planner_calls_are_charged_to_the_run_and_can_end_it(conn):
    """Every planning round's tokens/cost land on runs.tokens_used / cost_used_usd from the meter's rows (plan.usage
    event); a round that crosses max_tokens aborts the run with BUDGET_EXHAUSTED before anything is installed."""
    b = default_budgets(max_tokens=100_000, max_attempt_tokens=10_000)  # coherent with rule 8: 6 tasks x 10k fit
    run = runs_mod.create_run(conn, goal="benchmark run", benchmark="url_shortener", budgets=b)
    inner = FakeProvider([_dag_json()], input_tokens=400, output_tokens=200, model="fake-planner")
    planner = LLMPlanner(inner, sink_factory=lambda rid: DbSink(conn))
    r = runs_mod.plan_run(conn, run.id, planner, capabilities=set(CAPS))
    assert r.status is RunStatus.RUNNING and r.tokens_used == 600
    ev = [e for e in for_run(conn, run.id) if e.type == "plan.usage"]
    assert len(ev) == 1 and ev[0].payload["tokens"] == 600 and ev[0].payload["calls"] == 1
    assert conn.execute("SELECT count(*) AS n FROM model_calls WHERE run_id = %s AND NOT settled", (run.id,)).fetchone()["n"] == 0
    # settlement is idempotent
    assert runs_mod.settle_planner_usage(conn, run.id) is None
    # a run with 500 tokens left: the planner's one call (600) crosses the line → ABORTED BUDGET_EXHAUSTED, no tasks
    run2 = runs_mod.create_run(conn, goal="x", benchmark="url_shortener", budgets=b)
    with conn.transaction():
        conn.execute("UPDATE runs SET tokens_used = %s WHERE id = %s", (100_000 - 500, run2.id))
    inner2 = FakeProvider([_dag_json()], input_tokens=400, output_tokens=200, model="fake-planner")
    r2 = runs_mod.plan_run(conn, run2.id, LLMPlanner(inner2, sink_factory=lambda rid: DbSink(conn)), capabilities=set(CAPS))
    assert r2.status is RunStatus.ABORTED and r2.verdict_reason == VerdictReason.BUDGET_EXHAUSTED.value
    assert "max_tokens" in r2.verdict and sm.tasks_for_run(conn, run2.id) == []
    assert r2.tokens_used == 100_000 - 500 + 600  # overshoot ≤ one call, and it is on the record


def test_planner_usage_is_charged_even_when_the_round_fails(conn):
    """A round that ends in a planner error still charges what it spent (the rows were written call by call)."""
    run = runs_mod.create_run(conn, goal="Build a small URL-shortener HTTP service", budgets=default_budgets(max_plan_attempts=1))
    inner = FakeProvider(["not json", "still not json", "nope"], input_tokens=100, output_tokens=50, model="fake-planner")
    planner = LLMPlanner(inner, sink_factory=lambda rid: DbSink(conn))
    r = runs_mod.plan_run(conn, run.id, planner, capabilities=set(CAPS))
    assert r.status is RunStatus.FAILED and r.verdict_reason == VerdictReason.INVALID_PLAN.value
    assert r.tokens_used == 3 * 150  # three parse attempts, all charged
    # and the contract path charges too
    run2 = runs_mod.create_run(conn, goal="Build a small URL-shortener HTTP service", budgets=default_budgets())
    inner2 = FakeProvider([_contract_json()], input_tokens=100, output_tokens=50, model="fake-planner")
    r2 = runs_mod.plan_run(conn, run2.id, LLMPlanner(inner2, sink_factory=lambda rid: DbSink(conn)), capabilities=set(CAPS))
    assert r2.status is RunStatus.AWAITING_INPUT and r2.tokens_used == 150


# ----------------------------------------------------------------------------- reservations


def test_concurrent_attempts_reserve_from_the_unreserved_budget(conn):
    """Three tasks are READY at once. With 250 tokens unreserved and a 100-token allocation, the third *simultaneous*
    claim gets what is left (50), never a fourth full share: Σ RUNNING allocations + tokens_used never exceeds
    max_tokens. Settling one attempt frees its share for the next claim. Deterministic: claims are made directly, no
    threads, no timing."""
    from mas.orchestrator import leases

    wide = DagSpec.from_dict(
        {
            "tasks": [
                {
                    "id": k,
                    "capability": "implementation",
                    "goal": k,
                    "depends_on": [],
                    "output_contract": {"artifacts": ["git_commit"]},
                }
                for k in ("A", "B", "C")
            ]
        }
    )
    budgets = default_budgets(max_tokens=500, max_attempt_tokens=100, max_concurrency=3)  # rule 8: 4 x 100 fits
    run = runs_mod.create_run_from_dag(conn, wide, budgets=budgets, capabilities=set(CAPS))
    with conn.transaction():  # half the budget is already spent when execution starts: 250 unreserved
        conn.execute("UPDATE runs SET tokens_used = 250 WHERE id = %s", (run.id,))
    scheduler.tick(conn, run.id, verifier=StubVerifier(True))  # A, B, C → READY
    claims = [leases.claim_task(conn, worker_id=f"w{i}", capabilities=list(CAPS), run_id=run.id) for i in range(3)]
    assert all(c is not None for c in claims)
    allocs = [c.attempt.token_allocation for c in claims]
    assert allocs == [100, 100, 50], allocs  # the three simultaneous shares fit in 250 exactly
    assert leases.reserved_tokens(conn, run.id) == 250
    assert (
        leases.claim_task(conn, worker_id="w9", capabilities=list(CAPS), run_id=run.id) is None
    )  # nothing READY (and max_concurrency)
    # a worker-side ceiling lowers the ask, never raises it
    ev = [e for e in for_run(conn, run.id) if e.type == "attempt.leased"]
    assert [e.payload["token_allocation"] for e in ev] == [100, 100, 50]
    # settle the first attempt (used 30 of its 100): its reservation is gone, its usage is charged, the next claim
    # for the integration task gets min(100, 500 - 280 - 150) = 70
    leases.report(
        conn,
        claims[0].attempt.id,
        success=True,
        artifacts=[leases.ArtifactSpec(type="git_commit", ref="stub:A", meta={})],
        usage={"model": "stub", "input_tokens": 20, "output_tokens": 10, "cost_usd": 0.0},
    )
    for c in claims[1:]:
        leases.report(
            conn,
            c.attempt.id,
            success=True,
            artifacts=[leases.ArtifactSpec(type="git_commit", ref=f"stub:{c.task.key}", meta={})],
        )
    assert leases.reserved_tokens(conn, run.id) == 0
    scheduler.tick(conn, run.id, verifier=StubVerifier(True))  # T_integrate → READY
    c4 = leases.claim_task(conn, worker_id="w4", capabilities=list(CAPS), run_id=run.id, token_ceiling=60)
    assert c4 is not None and c4.task.key == "T_integrate" and c4.attempt.token_allocation == 60  # ceiling 60 < 100 < free 220
    run_now = sm.get_run(conn, run.id)
    assert run_now.tokens_used == 280 and run_now.tokens_used + leases.reserved_tokens(conn, run.id) <= 500


def test_rule8_remaining_is_net_of_reservations(conn):
    run = runs_mod.create_run_from_dag(
        conn, diamond(), budgets=default_budgets(max_tokens=1000, max_attempt_tokens=100), capabilities=set(CAPS)
    )
    scheduler.tick(conn, run.id, verifier=StubVerifier(True))  # T1 READY
    from mas.orchestrator import leases

    c = leases.claim_task(conn, worker_id="w", capabilities=list(CAPS), run_id=run.id)
    assert c is not None and c.attempt.token_allocation == 100
    rem = runs_mod.remaining_budget(conn, sm.get_run(conn, run.id))
    assert rem.tokens == 900 and rem.open_tasks == 5


# ----------------------------------------------------------------------------- verifier ERROR / INVALID / TIMEOUT


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (VerificationStatus.ERROR, VerdictReason.UNRECOVERABLE_FAILURE),
        (VerificationStatus.TIMEOUT, VerdictReason.UNRECOVERABLE_FAILURE),
        (VerificationStatus.INVALID, VerdictReason.UNSUPPORTED),
    ],
)
def test_infrastructure_verifier_outcomes_never_trigger_repair(conn, status, code):
    """A verifier that could not run the suite is not the code's fault: no fingerprint, no REPLANNING, no amendment —
    a coded terminal verdict, even with repair budget left and a planner that would happily amend."""
    from tests.test_repair import FIX1

    result = VerificationResult.fail(f"scripted {status.value}", status=status)
    planner = StubPlanner(diamond(), amendments=[FIX1])
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(max_replans=2), capabilities=set(CAPS))
    stop = threading.Event()
    agent = StubAgent({"sleep_s": 0.02})
    ws = [Worker(f"w{i}", list(CAPS), agent, database_url=DB_URL, poll_s=0.05, run_id=run.id) for i in range(2)]
    ts = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(
            conn,
            run.id,
            verifier=StubVerifier(script=[result]),
            planner=planner,
            capabilities=set(CAPS),
            tick_s=0.1,
            timeout_s=60,
        )
    finally:
        stop.set()
        wait_all(ts, 10)
    assert final.status is RunStatus.FAILED and final.verdict_reason == code.value
    assert final.verdict.startswith(f"FAIL:verification not completed ({status.value})") and final.replans_used == 0
    types = [e.type for e in for_run(conn, run.id)]
    assert "verify.fingerprint" not in types and "run.replanning" not in types
    assert planner.amendment_calls == 0


def test_missing_verifier_is_unrecoverable_not_unsupported(conn):
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    stop = threading.Event()
    ws = [
        Worker(f"w{i}", list(CAPS), StubAgent({"sleep_s": 0.02}), database_url=DB_URL, poll_s=0.05, run_id=run.id)
        for i in range(2)
    ]
    ts = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=None, tick_s=0.1, timeout_s=60)  # MissingVerifier
    finally:
        stop.set()
        wait_all(ts, 10)
    assert final.status is RunStatus.FAILED and final.verdict_reason == VerdictReason.UNRECOVERABLE_FAILURE.value
    assert "no acceptance verifier" in final.verdict


# ----------------------------------------------------------------------------- slow history is not a lower bound


def test_slow_history_does_not_falsely_reject_a_replan(conn):
    """One 30 s attempt (a timeout) and one 1 s success in this run: the per-task history rule 8 reports is the shortest
    *success* (1 s), and it is advisory - a warning on the plan.validated event, never a rejection."""
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(max_wallclock_s=600), capabilities=set(CAPS))
    with conn.transaction():
        t1 = conn.execute("SELECT id FROM tasks WHERE run_id = %s AND key = 'T1'", (run.id,)).fetchone()["id"]
        conn.execute(
            "INSERT INTO attempts (task_id, attempt_number, status, worker_id, started_at, finished_at) VALUES "
            "(%s, 1, 'TIMEOUT', 'w', now() - interval '40 seconds', now() - interval '10 seconds'), "
            "(%s, 2, 'SUCCESS', 'w', now() - interval '9 seconds', now() - interval '8 seconds')",
            (t1, t1),
        )
    rem = runs_mod.remaining_budget(conn, sm.get_run(conn, run.id))
    assert rem.observed_attempt_s is not None and abs(rem.observed_attempt_s - 1.0) < 0.5, rem
    r = validate(
        diamond(),
        budgets=default_budgets(),
        remaining=Remaining(wallclock_s=20, observed_attempt_s=rem.observed_attempt_s, tokens=10**9),
    )
    assert r.ok and r.warnings == [], (r.errors, r.warnings)
    # and even a history that would not fit is only advisory (never rejects a repair)
    r = validate(diamond(), budgets=default_budgets(), remaining=Remaining(wallclock_s=2, observed_attempt_s=30.0, tokens=10**9))
    assert r.ok and r.warnings and r.warnings[0].rule == "8-advisory"
    # with only failed attempts on record, the shortest settled attempt is the bound (still not the mean)
    with conn.transaction():
        conn.execute("UPDATE attempts SET status = 'FAILED' WHERE task_id = %s AND attempt_number = 2", (t1,))
    rem2 = runs_mod.remaining_budget(conn, sm.get_run(conn, run.id))
    assert rem2.observed_attempt_s is not None and abs(rem2.observed_attempt_s - 1.0) < 0.5


# ----------------------------------------------------------------------------- abandoned worktree GC


@pytest.mark.skipif(not git_available(), reason="git not on PATH")
def test_terminal_run_leaves_no_worktrees_even_after_a_worker_death(conn, tmp_path: Path):
    gws = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    run = runs_mod.create_run_from_dag(
        conn, diamond({"T2": {"die_attempts": 1}}), budgets=default_budgets(lease_s=1), capabilities=set(CAPS)
    )
    stop = threading.Event()
    agent = StubAgent({"sleep_s": 0.05})
    ws = [Worker(f"w{i}", list(CAPS), agent, database_url=DB_URL, poll_s=0.05, run_id=run.id, workspace=gws) for i in range(3)]
    ts = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=StubVerifier(True), workspace=gws, tick_s=0.1, timeout_s=90)
    finally:
        stop.set()
        wait_all(ts, 10)
    assert final.status is RunStatus.PASSED
    atts = sm.attempts_for_run(conn, run.id)
    assert any(a.status is AttemptStatus.ABANDONED for a in atts)  # the death happened
    run_dir = tmp_path / "worktrees" / str(run.id)
    assert not run_dir.exists(), sorted(p.name for p in run_dir.iterdir()) if run_dir.exists() else None
    assert (tmp_path / "repos" / f"{run.id}.git" / "HEAD").exists()  # history stays
    assert gws.gc_run(run.id) == 0  # idempotent


# ----------------------------------------------------------------------------- verifier ERROR: one bounded retry


def test_verifier_error_is_retried_once_then_terminal(conn):
    """A transient infrastructure ERROR re-runs the verification once (verify.retry; run stays VERIFYING); a second
    ERROR is a coded terminal verdict. A first ERROR followed by PASS passes - no repair, no fingerprint, no amendment."""
    err = VerificationResult.fail("sandbox unavailable", status=VerificationStatus.ERROR)
    ok = VerificationResult.pass_()
    for script, expect in (([err, ok], RunStatus.PASSED), ([err, err], RunStatus.FAILED)):
        run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
        stop = threading.Event()
        agent = StubAgent({"sleep_s": 0.02})
        ws = [Worker(f"w{i}", list(CAPS), agent, database_url=DB_URL, poll_s=0.05, run_id=run.id) for i in range(2)]
        ts = [run_worker_thread(w, stop) for w in ws]
        v = StubVerifier(script=script)
        try:
            final = scheduler.run_until_terminal(conn, run.id, verifier=v, tick_s=0.1, timeout_s=60)
        finally:
            stop.set()
            wait_all(ts, 10)
        types = [e.type for e in for_run(conn, run.id)]
        assert final.status is expect and types.count("verify.retry") == 1 and v.calls == 2
        assert "verify.fingerprint" not in types and "run.replanning" not in types
        if expect is RunStatus.FAILED:
            assert final.verdict_reason == VerdictReason.UNRECOVERABLE_FAILURE.value and "(ERROR)" in final.verdict


# ----------------------------------------------------------------------------- reconciliation sweep


@pytest.mark.skipif(not git_available(), reason="git not on PATH")
def test_reconcile_workspaces_removes_only_old_terminal_or_unknown_run_dirs(conn, tmp_path: Path):
    import os
    import time
    import uuid

    gws = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    root = tmp_path / "worktrees"
    # a terminal run whose directory outlived it (a crash between "run ended" and gc), an unknown run's directory,
    # a live run's directory, and a directory that is not ours
    done = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    with conn.transaction():
        sm.abort_run(conn, done.id, "test")
        conn.execute("UPDATE runs SET finished_at = now() - interval '1 hour' WHERE id = %s", (done.id,))
    live = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    unknown_id = str(uuid.uuid4())
    for name in (str(done.id), unknown_id, str(live.id), "not-a-run"):
        (root / name / "task-1").mkdir(parents=True)
    old = time.time() - 3600
    os.utime(root / unknown_id, (old, old))
    removed = scheduler.reconcile_workspaces(conn, gws, grace_s=300)
    assert removed == 2
    assert not (root / str(done.id)).exists() and not (root / unknown_id).exists()
    assert (root / str(live.id) / "task-1").exists() and (root / "not-a-run" / "task-1").exists()
    # a recently finished run is left alone until the grace period passes
    recent = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    with conn.transaction():
        sm.abort_run(conn, recent.id, "test")
    (root / str(recent.id) / "task-1").mkdir(parents=True)
    assert scheduler.reconcile_workspaces(conn, gws, grace_s=300) == 0
    assert scheduler.reconcile_workspaces(conn, gws, grace_s=0) == 1 and not (root / str(recent.id)).exists()
