"""ADR-006: the planner may ask before it plans. LLM-free via StubPlanner. Antipatterns B3 (MAST 2.2)."""

from datetime import UTC, datetime, timedelta

import pytest

from mas.artifacts import store
from mas.db.events import for_run
from mas.models.enums import RunStatus, TaskStatus
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.orchestrator import state_machine as sm
from mas.planner.dag import QA
from mas.planner.planner import StubPlanner
from mas.verifier.stub import StubVerifier
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.stub import StubAgent
from tests.conftest import CAPS, DB_URL, default_budgets, diamond

pytestmark = pytest.mark.db


def _run(conn, budgets=None):
    # a benchmark names an existing acceptance suite; without one an ad-hoc goal must get a contract first (ADR-007)
    return runs_mod.create_run(conn, goal="build the diamond", budgets=budgets or default_budgets(), benchmark="diamond")


def test_planner_asks_then_plans_then_run_passes(conn):
    planner = StubPlanner(diamond(), questions=[["Which database?", "Python version?"]])
    run = _run(conn)
    # tick with a planner: CREATED → PLANNING → planner asks → AWAITING_INPUT
    r = scheduler.tick(conn, run.id, planner=planner, capabilities=set(CAPS))
    assert r.status is RunStatus.AWAITING_INPUT
    assert runs_mod.pending_questions(conn, run.id) == ["Which database?", "Python version?"]
    q_art = [a for a in conn.execute("SELECT * FROM artifacts WHERE run_id=%s AND type='question'", (run.id,))]
    assert len(q_art) == 1 and q_art[0]["meta"]["batch"] == 1
    assert sm.get_run(conn, run.id).questions_asked == 1
    # more ticks while waiting: nothing changes, no tasks yet, no stall
    for _ in range(3):
        assert scheduler.tick(conn, run.id, planner=planner, capabilities=set(CAPS)).status is RunStatus.AWAITING_INPUT
    assert sm.tasks_for_run(conn, run.id) == []
    # human answers → back to PLANNING → planner now returns the DAG → RUNNING
    r = runs_mod.answer(conn, run.id, "sqlite; python 3.12")
    assert r.status is RunStatus.PLANNING
    assert runs_mod.qa_history(conn, run.id) == [
        QA(questions=["Which database?", "Python version?"], answer="sqlite; python 3.12")
    ]
    r = scheduler.tick(conn, run.id, planner=planner, capabilities=set(CAPS))
    assert r.status is RunStatus.RUNNING
    assert len(sm.tasks_for_run(conn, run.id)) == 5
    # the planner saw the Q&A on its second call
    assert planner.requests[1].qa[0].answer == "sqlite; python 3.12"
    # finish the run with workers to prove nothing downstream broke
    import threading

    stop = threading.Event()
    ws = [
        Worker(f"w{i}", list(CAPS), StubAgent({"sleep_s": 0.05}), database_url=DB_URL, poll_s=0.05, run_id=run.id)
        for i in range(2)
    ]
    ts = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(
            conn, run.id, verifier=StubVerifier(True), planner=planner, capabilities=set(CAPS), tick_s=0.1, timeout_s=30
        )
    finally:
        stop.set()
        wait_all(ts, 5)
    assert final.status is RunStatus.PASSED
    types = [e.type for e in for_run(conn, run.id)]
    assert (
        types.index("plan.questions")
        < types.index("run.awaiting_input")
        < types.index("plan.answered")
        < types.index("plan.validated")
    )


def test_multiple_batches_and_max_questions_budget(conn):
    planner = StubPlanner(diamond(), questions=[["q1"], ["q2"], ["q3"], ["q4"]])
    run = _run(conn, default_budgets(max_questions=2))
    scheduler.tick(conn, run.id, planner=planner, capabilities=set(CAPS))
    runs_mod.answer(conn, run.id, "a1")
    scheduler.tick(conn, run.id, planner=planner, capabilities=set(CAPS))
    assert sm.get_run(conn, run.id).questions_asked == 2
    runs_mod.answer(conn, run.id, "a2")
    final = scheduler.tick(conn, run.id, planner=planner, capabilities=set(CAPS))  # third batch exceeds max_questions=2
    assert final.status is RunStatus.FAILED
    assert "max_questions" in final.verdict
    assert len(runs_mod.qa_history(conn, run.id)) == 2


def test_waiting_for_a_human_is_bounded_by_the_clock(conn):
    """I-4: AWAITING_INPUT still ends. Wall-clock counts from creation, deadline applies."""
    planner = StubPlanner(diamond(), questions=[["q"]])
    run = _run(conn, default_budgets(deadline_at=datetime.now(UTC) + timedelta(seconds=1)))
    assert scheduler.tick(conn, run.id, planner=planner, capabilities=set(CAPS)).status is RunStatus.AWAITING_INPUT
    import time

    time.sleep(1.2)
    final = scheduler.tick(conn, run.id, planner=planner, capabilities=set(CAPS))
    assert final.status is RunStatus.ABORTED and "deadline" in final.verdict
    with pytest.raises(runs_mod.NotAwaitingInput):
        runs_mod.answer(conn, run.id, "too late")


def test_answer_requires_awaiting_input_and_empty_batch_fails_run(conn):
    from mas.planner.dag import Questions

    run = _run(conn)
    with pytest.raises(runs_mod.NotAwaitingInput):
        runs_mod.answer(conn, run.id, "nobody asked")
    with conn.transaction():
        sm.transition_run(conn, run.id, RunStatus.PLANNING)
    final = runs_mod.ask_questions(conn, run.id, Questions(questions=["  ", ""]))
    assert final.status is RunStatus.FAILED and "empty question batch" in final.verdict


def test_planner_that_proceeds_records_its_assumptions(conn):
    """ADR-006 policy: don't ask when it isn't material — but say what you assumed, on the record."""
    d = diamond()
    d.assumptions = ["storage: sqlite (single-node is enough for the acceptance suite)", "python 3.12"]
    run = runs_mod.create_run_from_dag(conn, d, budgets=default_budgets(), capabilities=set(CAPS))
    assert run.status is RunStatus.RUNNING
    art = conn.execute("SELECT meta FROM artifacts WHERE run_id=%s AND type='assumptions'", (run.id,)).fetchall()
    assert len(art) == 1 and art[0]["meta"]["assumptions"] == d.assumptions
    types = [e.type for e in for_run(conn, run.id)]
    assert "plan.assumptions" in types and types.index("plan.assumptions") < types.index("run.running")
    # no questions were asked; the human can veto by aborting, nothing was hidden
    assert sm.get_run(conn, run.id).questions_asked == 0


def test_metrics_split_human_wait_from_machine_time(conn):
    from mas import metrics

    planner = StubPlanner(diamond(), questions=[["q"]])
    run = _run(conn)
    scheduler.tick(conn, run.id, planner=planner, capabilities=set(CAPS))
    import time

    time.sleep(0.6)  # the human "thinks"
    m_waiting = metrics.compute(conn, run.id)
    assert m_waiting.human_wait_s >= 0.5 and m_waiting.questions == 1
    runs_mod.answer(conn, run.id, "a")
    import threading

    stop = threading.Event()
    ws = [
        Worker(f"w{i}", list(CAPS), StubAgent({"sleep_s": 0.05}), database_url=DB_URL, poll_s=0.05, run_id=run.id)
        for i in range(2)
    ]
    ts = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(
            conn, run.id, verifier=StubVerifier(True), planner=planner, capabilities=set(CAPS), tick_s=0.1, timeout_s=30
        )
    finally:
        stop.set()
        wait_all(ts, 5)
    assert final.status is RunStatus.PASSED
    m = metrics.compute(conn, run.id)
    assert m.total_s is not None and m.human_wait_s >= 0.5
    assert m.machine_s is not None and abs((m.machine_s + m.human_wait_s) - m.total_s) < 0.01
    assert m.wall_clock_s is not None and m.wall_clock_s <= m.machine_s + 0.01  # execution phase excludes the wait


def test_tools_allow_list_reaches_the_agent(conn):
    """Rule 4 result is stored on the task and handed to the agent as ctx.tools."""
    seen: dict[str, list[str]] = {}

    class Spy(StubAgent):
        def execute(self, ctx):
            seen[ctx.task.key] = list(ctx.tools)
            return super().execute(ctx)

    d = diamond()
    d.by_id()["T2"].tools = ["filesystem", "python"]
    run = runs_mod.create_run_from_dag(conn, d, budgets=default_budgets(), capabilities=set(CAPS))
    tasks = {t.key: t for t in sm.tasks_for_run(conn, run.id)}
    assert tasks["T2"].tools == ["filesystem", "python"]
    assert "git" in tasks["T3"].tools and "git" not in tasks["T1"].tools  # defaults per capability
    import threading

    stop = threading.Event()
    ws = [Worker(f"w{i}", list(CAPS), Spy({"sleep_s": 0.02}), database_url=DB_URL, poll_s=0.05, run_id=run.id) for i in range(2)]
    ts = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=StubVerifier(True), tick_s=0.1, timeout_s=30)
    finally:
        stop.set()
        wait_all(ts, 5)
    assert final.status is RunStatus.PASSED
    assert seen["T2"] == ["filesystem", "python"]
    assert seen["T1"] == ["filesystem", "model"]
    assert all(t.status is TaskStatus.COMPLETED for t in sm.tasks_for_run(conn, run.id))
    # question/answer artifacts are run-level and immutable like everything else
    assert store.accepted_for_run(conn, run.id)  # integration artifact accepted
