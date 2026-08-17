"""Hard total budget and honest failure classification (review of rule 8 / 13-lite, 2026-08-17).

- planner usage is charged to the run (settled from telemetry rows, never self-reported) and can end the run;
- attempt allocations are *reservations*: concurrent attempts can never jointly be handed more than the run has left;
- verifier ERROR / INVALID / TIMEOUT never trigger code repair — they are coded terminal verdicts;
- the observed per-task time used by rule 8 is a lower bound (shortest success), so a slow history cannot falsely reject;
- a terminal run keeps no worktrees: what a crashed worker left behind is garbage-collected."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from mas.db.events import for_run
from mas.models.enums import AttemptStatus, RunStatus, VerdictReason
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.orchestrator import state_machine as sm
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
    """T2/T3/T4 run concurrently. With 250 tokens unreserved and a 100-token allocation, the third concurrent attempt
    gets what is left (50), never a fourth full share: the sum of RUNNING allocations + tokens_used never exceeds
    max_tokens; settlement frees the reservation for later attempts."""
    budgets = default_budgets(max_tokens=500, max_attempt_tokens=100, max_concurrency=3)  # rule 8: 5 x 100 fits
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=budgets, capabilities=set(CAPS))
    with conn.transaction():  # half the budget is already spent when execution starts: 250 unreserved
        conn.execute("UPDATE runs SET tokens_used = 250 WHERE id = %s", (run.id,))
    stop = threading.Event()
    agent = StubAgent({"sleep_s": 0.6})  # long enough for all three to be RUNNING at once
    ws = [Worker(f"w{i}", list(CAPS), agent, database_url=DB_URL, poll_s=0.02, run_id=run.id) for i in range(3)]
    ts = [run_worker_thread(w, stop) for w in ws]
    seen: list[list[int]] = []

    def watch() -> None:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not stop.is_set():
            rows = conn.execute(
                "SELECT a.token_allocation AS alloc FROM attempts a JOIN tasks t ON t.id = a.task_id "
                "WHERE t.run_id = %s AND a.status = 'RUNNING' AND t.key IN ('T2','T3','T4')",
                (run.id,),
            ).fetchall()
            allocs = sorted(int(r["alloc"]) for r in rows)
            if len(allocs) == 3:
                seen.append(allocs)
                return
            time.sleep(0.02)

    try:
        watcher = threading.Thread(target=watch, daemon=True)
        watcher.start()
        final = scheduler.run_until_terminal(conn, run.id, verifier=StubVerifier(True), tick_s=0.05, timeout_s=60)
        watcher.join(5)
    finally:
        stop.set()
        wait_all(ts, 10)
    assert final.status is RunStatus.PASSED
    assert seen and seen[0] == [50, 100, 100], seen  # the three concurrent shares fit in 250 exactly
    atts = {a.attempt_number: a for a in sm.attempts_for_run(conn, run.id)}
    assert all(a.token_allocation is not None and a.token_allocation <= 100 for a in atts.values())
    # T1 (alone) got a full share; T5 (after the others settled with zero usage) got a full share again
    tasks = {t.id: t.key for t in sm.tasks_for_run(conn, run.id)}
    by_key = {tasks[a.task_id]: a for a in sm.attempts_for_run(conn, run.id)}
    assert by_key["T1"].token_allocation == 100 and by_key["T5"].token_allocation == 100
    ev = [e for e in for_run(conn, run.id) if e.type == "attempt.leased"]
    assert all("token_allocation" in e.payload for e in ev)


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
    """One 30 s attempt (a timeout) and one 1 s success in this run: the per-task time rule 8 uses is the shortest
    *success* (1 s) — the mean (15.5 s) would have rejected a 3-task chain with 20 s left."""
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
    assert r.ok, r.errors
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
