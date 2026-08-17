"""Step 9 (DB) — model telemetry through the worker runtime: metered ctx.model, model_calls rows, attempt/run
accounting, per-attempt call budgets bounded by the run's remaining tokens, evidence surviving worker death, metrics."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from mas import metrics
from mas.models.enums import AttemptStatus, RunStatus
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.orchestrator import state_machine as sm
from mas.orchestrator.contracts import required_artifacts
from mas.providers.fake import FakeProvider
from mas.providers.pricing import Price, Pricing
from mas.providers.telemetry import AttemptBudgetExceeded, DbSink, MemorySink, MeteredProvider
from mas.verifier.stub import StubVerifier
from mas.workers.base import AgentResult, ArtifactOut, TaskContext
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from tests.conftest import CAPS, DB_URL, default_budgets, diamond

pytestmark = pytest.mark.db


class ModelAgent:
    """Test agent that talks to ctx.model like an LLM worker would (step 10 will replace it with the real one)."""

    name = "model-agent"

    def __init__(self, calls: int = 2, *, until_budget: bool = False, die_after: bool = False):
        self.calls = calls
        self.until_budget = until_budget
        self.die_after = die_after
        self.seen: list[dict[str, Any]] = []

    def execute(self, ctx: TaskContext) -> AgentResult:
        assert ctx.model is not None, "the runtime must hand over a metered model"
        for i in range(self.calls):
            comp = ctx.model.complete([{"role": "user", "content": f"{ctx.task.key} step {i}"}], max_tokens=64)
            self.seen.append({"task": ctx.task.key, "text": comp.text, "usage": comp.usage.as_dict()})
        if self.until_budget:
            try:
                while True:
                    ctx.model.complete([{"role": "user", "content": "more"}], max_tokens=64)
            except AttemptBudgetExceeded as e:
                return AgentResult(success=False, failure_reason=f"budget: {e}")
        if self.die_after:
            return AgentResult(success=False, failure_reason="simulated death", simulate_death=True)
        outs = [
            ArtifactOut(type=t, ref=f"model:{ctx.task.key}:{ctx.attempt.attempt_number}:{name or t}", meta={"name": name})
            for t, name in required_artifacts(ctx.task)
        ]
        return AgentResult(success=True, artifacts=outs)


def _run(conn, dag, *, agent, provider, workers=2, budgets=None, pricing=None, attempt_max_calls=None, attempt_max_tokens=None):
    run = runs_mod.create_run_from_dag(conn, dag, budgets=budgets or default_budgets(), capabilities=set(CAPS))
    stop = threading.Event()
    ws = [
        Worker(
            f"w{i + 1}",
            list(CAPS),
            agent,
            database_url=DB_URL,
            poll_s=0.05,
            run_id=run.id,
            provider=provider,
            pricing=pricing,
            attempt_max_calls=attempt_max_calls,
            attempt_max_tokens=attempt_max_tokens,
        )
        for i in range(workers)
    ]
    threads = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=StubVerifier(passed=True), tick_s=0.1, timeout_s=90)
    finally:
        stop.set()
        wait_all(threads, 10)
    return final, ws


def _calls(conn, run_id):
    return conn.execute("SELECT * FROM model_calls WHERE run_id = %s ORDER BY id", (run_id,)).fetchall()


def test_db_sink_writes_rows_immediately(conn):
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    sink = DbSink(conn)
    m = MeteredProvider(FakeProvider("hi", input_tokens=10, output_tokens=2), sink=sink, role="planner", run_id=run.id)
    m.complete([{"role": "user", "content": "plan"}])
    m.complete([{"role": "user", "content": "plan again"}])
    rows = _calls(conn, run.id)
    assert sink.written == 2 and [r["seq"] for r in rows] == [1, 2]
    assert rows[0]["role"] == "planner" and rows[0]["attempt_id"] is None and rows[0]["status"] == "ok"
    assert rows[0]["input_tokens"] == 10 and rows[0]["output_tokens"] == 2 and rows[0]["priced"] is True
    assert rows[0]["meta"]["max_tokens"] == 4096


def test_worker_meters_calls_and_settles_usage_from_the_meter(conn):
    provider = FakeProvider(["done"], input_tokens=100, output_tokens=10, model="vendor-x")
    provider.cost_per_mtok = (0.0, 0.0)  # provider does not price...
    pricing = Pricing({"vendor-x": Price(10.0, 20.0)})  # ...config does
    agent = ModelAgent(calls=2)
    final, _ = _run(conn, diamond(), agent=agent, provider=provider, pricing=pricing)
    assert final.status is RunStatus.PASSED, final.verdict
    attempts = sm.attempts_for_run(conn, final.id)
    assert len(attempts) == 5 and all(a.status is AttemptStatus.SUCCESS for a in attempts)
    for a in attempts:  # settlement summary comes from the meter: 2 calls × (100 in + 10 out), priced from config
        assert (a.model, a.input_tokens, a.output_tokens) == ("vendor-x", 200, 20)
        assert abs(a.cost_usd - 2 * (100 * 10 + 10 * 20) / 1e6) < 1e-9
    run = sm.get_run(conn, final.id)
    assert run.tokens_used == 5 * 220 and abs(run.cost_used_usd - 5 * 0.0024) < 1e-6
    rows = _calls(conn, final.id)
    assert len(rows) == 10 and {r["attempt_id"] for r in rows} == {a.id for a in attempts}
    assert all(r["role"] == "worker" and r["provider"] == "fake" and r["model"] == "vendor-x" and r["priced"] for r in rows)
    assert {r["seq"] for r in rows} == {1, 2}
    # metrics / `mas status` see the same evidence, per model
    m = metrics.compute(conn, final.id)
    assert m.model_calls == 10 and m.model_call_errors == 0 and m.unpriced_calls == 0
    assert m.call_input_tokens == 1000 and m.call_output_tokens == 100 and abs(m.call_cost_usd - 5 * 0.0024) < 1e-6
    assert list(m.per_model) == ["fake:vendor-x/worker"] and m.per_model["fake:vendor-x/worker"]["calls"] == 10
    assert m.input_tokens == 1000 and abs(m.cost_usd - m.call_cost_usd) < 1e-6  # settlement and evidence agree


def test_unpriced_model_is_flagged_not_hidden(conn):
    provider = FakeProvider(["done"], input_tokens=50, output_tokens=5, model="mystery")
    provider.cost_per_mtok = (0.0, 0.0)
    # simulate a real provider: unpriced usage, no price table
    orig = provider.complete

    def unpriced(*a, **kw):
        c = orig(*a, **kw)
        c.usage = c.usage.with_cost(None)
        return c

    provider.complete = unpriced  # type: ignore[method-assign]
    final, _ = _run(conn, diamond(), agent=ModelAgent(calls=1), provider=provider, pricing=Pricing())
    assert final.status is RunStatus.PASSED
    m = metrics.compute(conn, final.id)
    assert m.model_calls == 5 and m.unpriced_calls == 5 and m.call_cost_usd == 0.0
    assert all(not r["priced"] for r in _calls(conn, final.id))


def test_attempt_call_budget_ends_a_runaway_agent(conn):
    provider = FakeProvider("again", input_tokens=10, output_tokens=1)
    agent = ModelAgent(calls=0, until_budget=True)
    dag = diamond()
    final, _ = _run(
        conn, dag, agent=agent, provider=provider, budgets=default_budgets(max_attempts_per_task=1), attempt_max_calls=3
    )
    assert final.status is RunStatus.FAILED
    attempts = sm.attempts_for_run(conn, final.id)
    a = attempts[0]
    assert a.status is AttemptStatus.FAILED and "call budget exhausted (3/3 calls)" in (a.failure_reason or "")
    assert (a.input_tokens, a.output_tokens) == (30, 3)  # exactly three calls were paid for
    rows = [r for r in _calls(conn, final.id) if r["attempt_id"] == a.id]
    assert [r["status"] for r in rows] == ["ok", "ok", "ok", "budget"]  # 3 real calls + the refusal that ended it
    assert metrics.compute(conn, final.id).model_call_refused == 1


def test_run_token_budget_caps_the_attempt_budget(conn):
    """A run that is nearly out of tokens gets attempts bounded by what is left. Token usage is only known after a response,
    so the call that crosses the line completes (overshoot ≤ one call); the next call is refused and the run then aborts."""
    provider = FakeProvider("x", input_tokens=40, output_tokens=10)  # 50 tokens per call
    # coherent budgets (rule 8): 5 tasks x 10k allocation fits 100k; the worker-side ceiling below matches it
    run_budgets = default_budgets(max_tokens=100_000, max_attempt_tokens=10_000, max_attempts_per_task=1)
    dag = diamond()
    run = runs_mod.create_run_from_dag(conn, dag, budgets=run_budgets, capabilities=set(CAPS))
    conn.execute("UPDATE runs SET tokens_used = %s WHERE id = %s", (100_000 - 120, run.id))  # 120 tokens left
    agent = ModelAgent(calls=0, until_budget=True)
    stop = threading.Event()
    w = Worker(
        "w1", list(CAPS), agent, database_url=DB_URL, poll_s=0.05, run_id=run.id, provider=provider, attempt_max_tokens=10_000
    )
    t = run_worker_thread(w, stop)
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=StubVerifier(passed=True), tick_s=0.1, timeout_s=60)
    finally:
        stop.set()
        wait_all([t], 10)
    a = sm.attempts_for_run(conn, run.id)[0]
    assert a.status is AttemptStatus.FAILED and "token budget exhausted" in (a.failure_reason or "")
    assert a.input_tokens + a.output_tokens == 150  # 3 calls fit into 120 (the third crosses, the fourth is refused)
    # the overshoot is one call at most; settlement pushes the run over its token budget → the orchestrator aborts it
    assert final.status is RunStatus.ABORTED and sm.get_run(conn, run.id).tokens_used == 100_000 - 120 + 150


def test_call_evidence_survives_worker_death(conn):
    provider = FakeProvider("x", input_tokens=10, output_tokens=1)
    dag = diamond()
    run = runs_mod.create_run_from_dag(conn, dag, budgets=default_budgets(lease_s=1), capabilities=set(CAPS))
    dying = ModelAgent(calls=2, die_after=True)
    stop = threading.Event()
    w = Worker("w-dies", list(CAPS), dying, database_url=DB_URL, poll_s=0.05, run_id=run.id, provider=provider)
    t = run_worker_thread(w, stop)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and not w.dead.is_set():
        scheduler.tick(conn, run.id, verifier=StubVerifier(passed=True))  # readies T1; the worker claims and dies on it
        time.sleep(0.05)
    stop.set()
    wait_all([t], 10)
    assert w.dead.is_set() and w.stats.died == 1
    rows = _calls(conn, run.id)
    att = sm.attempts_for_run(conn, run.id)[0]
    assert len(rows) == 2 and all(r["attempt_id"] == att.id for r in rows)  # the calls are on record...
    assert att.input_tokens == 0 and att.status is AttemptStatus.RUNNING  # ...though nothing was ever settled
    # a healthy worker finishes the run after the reaper recovers the task; the dead attempt's rows stay
    healthy = ModelAgent(calls=1)
    stop2 = threading.Event()
    w2 = Worker("w-ok", list(CAPS), healthy, database_url=DB_URL, poll_s=0.05, run_id=run.id, provider=provider)
    t2 = run_worker_thread(w2, stop2)
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=StubVerifier(passed=True), tick_s=0.1, timeout_s=90)
    finally:
        stop2.set()
        wait_all([t2], 10)
    assert final.status is RunStatus.PASSED
    m = metrics.compute(conn, run.id)
    # the reaper settled the dead attempt's spend from its telemetry rows (hard total budget): 7 calls, all counted
    assert m.model_calls == 2 + 5 and m.input_tokens == 7 * 10
    dead_rows = [r for r in _calls(conn, run.id) if r["attempt_id"] == att.id]
    dead = sm.get_attempt(conn, att.id)
    assert len(dead_rows) == 2 and dead.status is AttemptStatus.ABANDONED and dead.input_tokens == 20
    assert final.tokens_used == 7 * 11  # attempts' spend (dead one included); no planner in this run


def test_worker_without_provider_hands_over_no_model(conn):
    class NoModelAgent:
        name = "no-model"

        def execute(self, ctx: TaskContext) -> AgentResult:
            assert ctx.model is None
            outs = [ArtifactOut(type=t, ref=f"n:{ctx.task.key}:{name or t}") for t, name in required_artifacts(ctx.task)]
            return AgentResult(success=True, artifacts=outs, usage={"model": "stub", "input_tokens": 3, "output_tokens": 1})

    final, _ = _run(conn, diamond(), agent=NoModelAgent(), provider=None)
    assert final.status is RunStatus.PASSED
    assert _calls(conn, final.id) == [] and metrics.compute(conn, final.id).model_calls == 0
    assert sm.get_run(conn, final.id).tokens_used == 5 * 4  # agent-reported usage still settles as before (M1 path)


def test_memory_sink_meter_is_db_free():
    """The meter itself needs no database (planner / `mas models --ping` paths)."""
    sink = MemorySink()
    m = MeteredProvider(FakeProvider("pong"), sink=sink, role="ping")
    assert m.complete([{"role": "user", "content": "ping"}]).text == "pong" and sink.records[0].role == "ping"


def test_runtime_clamps_model_calls_to_the_attempt_runtime(conn):
    """A provider call that would outlive max_attempt_runtime_s is cut by the meter's timeout, recorded as an unpriced
    error, and the attempt ends — no billing continues after the orchestrator has given up on the attempt."""
    provider = FakeProvider([{"text": "slow", "delay_s": 60}, "never reached"])
    agent = ModelAgent(calls=2)
    t0 = time.monotonic()
    final, _ = _run(
        conn,
        diamond(),
        agent=agent,
        provider=provider,
        budgets=default_budgets(max_attempt_runtime_s=3, max_attempts_per_task=1),
        workers=1,
    )
    assert time.monotonic() - t0 < 60
    a = sm.attempts_for_run(conn, final.id)[0]
    assert a.status in {AttemptStatus.FAILED, AttemptStatus.TIMEOUT}, a  # worker reported the failure, or the reaper won the race
    if a.status is AttemptStatus.FAILED:
        assert "timed out" in (a.failure_reason or "")
    rows = [r for r in _calls(conn, final.id) if r["attempt_id"] == a.id]
    assert rows and rows[0]["status"] == "error" and not rows[0]["priced"] and "timed out" in rows[0]["error"]
    assert rows[0]["meta"]["timeout_s"] <= 3.0
    assert len(provider.calls) == 1 and provider.calls[0]["timeout_s"] <= 3.0  # the second scripted call never happened
    assert final.status is RunStatus.FAILED
