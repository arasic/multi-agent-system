"""Concurrency regression: workers + an aggressive independent reaper + the scheduler, simultaneously, on several
runs with 1 s leases. Asserts zero PostgreSQL deadlocks, zero unexpected stale reports, zero unexpected abandonments,
and correct outcomes. This is the test that would have caught the task→run vs run→task lock inversion.
"""

import logging
import threading
import time

import pytest

from mas.db import connect
from mas.models.enums import AttemptStatus, RunStatus
from mas.orchestrator import leases, scheduler
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import state_machine as sm
from mas.verifier.stub import StubVerifier
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.stub import StubAgent
from mas.workers.workspace import GitWorkspace, NullWorkspace, git_available
from tests.conftest import CAPS, DB_URL, default_budgets, diamond

pytestmark = pytest.mark.db


class _DeadlockCounter(logging.Handler):
    def __init__(self):
        super().__init__()
        self.deadlocks = 0
        self.errors: list[str] = []

    def emit(self, record):
        msg = record.getMessage()
        text = msg + (str(record.exc_info[1]) if record.exc_info and record.exc_info[1] else "")
        if "deadlock" in text.lower():
            self.deadlocks += 1
        if record.levelno >= logging.ERROR:
            self.errors.append(text[:200])


def _hammer(conn, dags, *, workspace, workers=4, reaper_period=0.05, tick_s=0.05, timeout=120):
    runs = [runs_mod.create_run_from_dag(conn, d, budgets=default_budgets(lease_s=1), capabilities=set(CAPS)) for d in dags]
    stop = threading.Event()
    agent = StubAgent({"sleep_s": 0.05})
    ws = [
        Worker(f"w{i}", list(CAPS), agent, database_url=DB_URL, poll_s=0.02, workspace=workspace, pools=None)
        for i in range(workers)
    ]
    threads = [run_worker_thread(w, stop) for w in ws]

    # an independent, aggressive reaper (as a second orchestrator would be)
    reaped: list = []

    def reaper():
        c = connect(DB_URL)
        try:
            while not stop.is_set():
                try:
                    reaped.extend(leases.reap_expired(c))
                except Exception:
                    logging.getLogger("test.reaper").exception("reaper failed")
                    c.rollback()
                time.sleep(reaper_period)
        finally:
            c.close()

    rt = threading.Thread(target=reaper, daemon=True)
    rt.start()

    # scheduler: tick all runs round-robin until all terminal
    t0 = time.monotonic()
    try:
        while True:
            open_ = [r for r in runs if not sm.get_run(conn, r.id).status.terminal]
            if not open_:
                break
            for r in open_:
                scheduler.tick(conn, r.id, verifier=StubVerifier(True), workspace=workspace)
            if time.monotonic() - t0 > timeout:
                raise TimeoutError("runs did not finish")
            time.sleep(tick_s)
    finally:
        stop.set()
        wait_all(threads, 10)
        rt.join(5)
    return runs, ws, reaped


@pytest.fixture
def counter():
    h = _DeadlockCounter()
    logging.getLogger().addHandler(h)
    yield h
    logging.getLogger().removeHandler(h)


def test_workers_reaper_scheduler_no_deadlocks_null_workspace(conn, counter):
    dags = [diamond() for _ in range(6)]
    runs, ws, reaped = _hammer(conn, dags, workspace=NullWorkspace(), workers=6)
    assert all(sm.get_run(conn, r.id).status is RunStatus.PASSED for r in runs), [
        (sm.get_run(conn, r.id).status, sm.get_run(conn, r.id).verdict) for r in runs
    ]
    assert counter.deadlocks == 0, counter.errors
    assert sum(w.stats.stale for w in ws) == 0
    assert reaped == []  # nothing legitimately expired: heartbeats covered every attempt through settlement
    for r in runs:
        atts = sm.attempts_for_run(conn, r.id)
        assert all(a.status is AttemptStatus.SUCCESS for a in atts), [(a.status, a.failure_reason) for a in atts]


@pytest.mark.skipif(not git_available(), reason="git not on PATH")
def test_workers_reaper_scheduler_no_deadlocks_git_workspace(conn, counter, tmp_path):
    gws = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    dags = [diamond() for _ in range(4)]
    runs, ws, reaped = _hammer(conn, dags, workspace=gws, workers=5, timeout=180)
    assert all(sm.get_run(conn, r.id).status is RunStatus.PASSED for r in runs), [
        (sm.get_run(conn, r.id).status, sm.get_run(conn, r.id).verdict) for r in runs
    ]
    assert counter.deadlocks == 0, counter.errors
    assert sum(w.stats.stale for w in ws) == 0
    assert reaped == []
    # no leaked worktrees
    for r in runs:
        d = gws.worktree_root / str(r.id)
        assert not d.exists() or not any(d.iterdir())


def test_deliberate_deaths_produce_exactly_one_abandonment_each(conn, counter):
    """With deaths scripted, the ONLY abandonments are the scripted ones — the reaper never eats a live attempt."""
    dags = [diamond({"T2": {"die_attempts": 1}}), diamond({"T3": {"die_attempts": 1}}), diamond()]
    runs, ws, reaped = _hammer(conn, dags, workspace=NullWorkspace(), workers=6, timeout=120)
    assert all(sm.get_run(conn, r.id).status is RunStatus.PASSED for r in runs)
    assert counter.deadlocks == 0, counter.errors
    abandoned = [a for r in runs for a in sm.attempts_for_run(conn, r.id) if a.status is AttemptStatus.ABANDONED]
    assert len(abandoned) == 2
    assert sum(1 for w in ws if w.stats.died) == 2
    assert sum(w.stats.stale for w in ws) == 0
