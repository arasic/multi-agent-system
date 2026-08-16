"""7C: concurrent, bounded run ticking + the verifier service.

- a slow verifier on run A must not block run B (bounded executor, one connection per tick)
- two orchestrator loops on the same runs never tick / verify the same run at the same time (advisory locks)
- `--verifier external` (DeferredVerification) leaves runs in VERIFYING; `verify_once` / `verify_forever` finish them
- the real sandboxed verifier can be the service (Docker test)
- no leaked database connections
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from mas.models.enums import RunStatus
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.orchestrator import state_machine as sm
from mas.planner.dag import DagSpec
from mas.verifier.acceptance import AcceptanceVerifier, SandboxLimits
from mas.verifier.base import DeferredVerification, VerificationRequest, VerificationResult
from mas.verifier.stub import StubVerifier
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.stub import StubAgent
from mas.workers.workspace import GitWorkspace, NullWorkspace
from tests.conftest import CAPS, DB_URL, default_budgets, diamond

pytestmark = pytest.mark.db
ROOT = Path(__file__).resolve().parents[1]


class SlowFor:
    """Verifier that sleeps for specific runs, passes everything, and records overlapping verifications per run."""

    name = "slow-for-test"

    def __init__(self, slow_run_ids: set, sleep_s: float):
        self.slow = set(slow_run_ids)
        self.sleep_s = sleep_s
        self.lock = threading.Lock()
        self.active: dict = {}
        self.max_concurrent_per_run: dict = {}
        self.calls: list = []

    def verify(self, request: VerificationRequest) -> VerificationResult:
        rid = request.run_id
        with self.lock:
            self.active[rid] = self.active.get(rid, 0) + 1
            self.max_concurrent_per_run[rid] = max(self.max_concurrent_per_run.get(rid, 0), self.active[rid])
            self.calls.append((rid, time.monotonic()))
        try:
            if rid in self.slow:
                time.sleep(self.sleep_s)
            return VerificationResult.pass_(evidence={"verifier": self.name})
        finally:
            with self.lock:
                self.active[rid] -= 1


def _workers(run_ids, n=3, stop=None, workspace=None):
    stop = stop or threading.Event()
    agent = StubAgent({"sleep_s": 0.05})
    ws = [
        Worker(f"w{i}", list(CAPS), agent, database_url=DB_URL, poll_s=0.03, workspace=workspace or NullWorkspace())
        for i in range(n)
    ]
    return ws, [run_worker_thread(w, stop) for w in ws], stop


def _connections(conn) -> int:
    return conn.execute("SELECT count(*) AS n FROM pg_stat_activity WHERE datname = current_database()").fetchone()["n"]


def test_slow_verifier_on_run_a_does_not_block_run_b(conn):
    a = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    b = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    verifier = SlowFor({a.id}, sleep_s=4.0)
    ws, threads, stop = _workers([a.id, b.id])
    svc_stop = threading.Event()
    svc = threading.Thread(
        target=scheduler.orchestrate_forever,
        args=(DB_URL,),
        kwargs=dict(verifier=verifier, tick_s=0.1, stop=svc_stop, pools=None, workspace=NullWorkspace(), max_parallel=4),
        daemon=True,
    )
    before = _connections(conn)
    svc.start()
    try:
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            fa, fb = sm.get_run(conn, a.id), sm.get_run(conn, b.id)
            if fa.status.terminal and fb.status.terminal:
                break
            time.sleep(0.1)
    finally:
        svc_stop.set()
        svc.join(20)
        stop.set()
        wait_all(threads, 10)
    fa, fb = sm.get_run(conn, a.id), sm.get_run(conn, b.id)
    assert fa.status is RunStatus.PASSED and fb.status is RunStatus.PASSED
    # B finished while A was still inside its 4 s verification: B's finish precedes A's finish by ~sleep_s
    assert (fa.finished_at - fb.finished_at).total_seconds() > 2.0, (fa.finished_at, fb.finished_at)
    assert verifier.max_concurrent_per_run[a.id] == 1 and verifier.max_concurrent_per_run[b.id] == 1
    time.sleep(0.5)
    assert _connections(conn) <= before + 1  # per-tick connections were closed (tolerance: the loop's scan conn shutting down)


def test_two_orchestrators_never_tick_or_verify_the_same_run_concurrently(conn):
    runs = [runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS)) for _ in range(3)]
    verifier = SlowFor({r.id for r in runs}, sleep_s=1.0)  # every verification is slow → overlap would be visible
    ws, threads, stop = _workers([r.id for r in runs], n=4)
    stops = [threading.Event(), threading.Event()]
    loops = [
        threading.Thread(
            target=scheduler.orchestrate_forever,
            args=(DB_URL,),
            kwargs=dict(verifier=verifier, tick_s=0.05, stop=stops[i], pools=None, workspace=NullWorkspace(), max_parallel=3),
            daemon=True,
        )
        for i in range(2)
    ]
    for t in loops:
        t.start()
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and not all(sm.get_run(conn, r.id).status.terminal for r in runs):
            time.sleep(0.1)
    finally:
        for s_ in stops:
            s_.set()
        for t in loops:
            t.join(20)
        stop.set()
        wait_all(threads, 10)
    assert all(sm.get_run(conn, r.id).status is RunStatus.PASSED for r in runs)
    # each run verified exactly once, never concurrently — two orchestrators, advisory locks
    assert all(verifier.max_concurrent_per_run[r.id] == 1 for r in runs)
    assert sum(1 for rid, _ in verifier.calls) == 3
    for r in runs:
        ver = conn.execute("SELECT count(*) AS n FROM artifacts WHERE run_id=%s AND type='verification'", (r.id,)).fetchone()["n"]
        assert ver == 1


def test_external_verifier_defers_and_verify_service_completes(conn):
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    ws, threads, stop = _workers([run.id])
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            r = scheduler.tick(conn, run.id, verifier=DeferredVerification())
            if r.status is RunStatus.VERIFYING:
                break
            time.sleep(0.05)
    finally:
        stop.set()
        wait_all(threads, 10)
    assert sm.get_run(conn, run.id).status is RunStatus.VERIFYING
    # further ticks with the deferred verifier leave it alone (no verdict, no artifact)
    for _ in range(3):
        assert scheduler.tick(conn, run.id, verifier=DeferredVerification()).status is RunStatus.VERIFYING
    assert (
        conn.execute("SELECT count(*) AS n FROM artifacts WHERE run_id=%s AND type='verification'", (run.id,)).fetchone()["n"]
        == 0
    )
    # the verifier service finishes it
    out = scheduler.verify_once(conn, verifier=StubVerifier(True))
    assert out == [(run.id, RunStatus.PASSED)]
    assert sm.get_run(conn, run.id).verdict == "PASS"


def test_verify_forever_service_loop_finishes_deferred_runs(conn):
    runs = [runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS)) for _ in range(2)]
    ws, threads, stop = _workers([r.id for r in runs])
    orch_stop, ver_stop = threading.Event(), threading.Event()
    orch = threading.Thread(
        target=scheduler.orchestrate_forever,
        args=(DB_URL,),
        kwargs=dict(verifier=DeferredVerification(), tick_s=0.05, stop=orch_stop, workspace=NullWorkspace()),
        daemon=True,
    )
    ver = threading.Thread(
        target=scheduler.verify_forever,
        args=(DB_URL,),
        kwargs=dict(verifier=StubVerifier(True), tick_s=0.05, stop=ver_stop, workspace=NullWorkspace(), max_parallel=2),
        daemon=True,
    )
    orch.start()
    ver.start()
    try:
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline and not all(sm.get_run(conn, r.id).status.terminal for r in runs):
            time.sleep(0.1)
    finally:
        orch_stop.set()
        ver_stop.set()
        orch.join(20)
        ver.join(20)
        stop.set()
        wait_all(threads, 10)
    assert all(sm.get_run(conn, r.id).status is RunStatus.PASSED for r in runs)


def test_verify_forever_rejects_non_verifiers():
    with pytest.raises(ValueError):
        scheduler.verify_forever(DB_URL, verifier=DeferredVerification(), stop=threading.Event())


@pytest.mark.docker
def test_service_mode_real_verdict_with_sandbox(conn, tmp_path, verifier_image):
    """Orchestrator (external) + verifier service (real AcceptanceVerifier) on the contract benchmark → PASS."""
    app = (ROOT / "tests" / "fixtures" / "apps" / "known_good_with_tests" / "app.py").read_text(encoding="utf-8")
    tst = (ROOT / "tests" / "fixtures" / "apps" / "known_good_with_tests" / "test_app.py").read_text(encoding="utf-8")
    dag = DagSpec.from_dict(
        {
            "goal": "contract benchmark via services",
            "benchmark": "url_shortener_contract",
            "tasks": [
                {
                    "id": "build",
                    "capability": "implementation",
                    "goal": "",
                    "depends_on": [],
                    "output_contract": {"artifacts": ["git_commit"]},
                    "meta": {"stub": {"files": {"app.py": app, "test_app.py": tst}}},
                },
                {
                    "id": "integrate",
                    "capability": "integration",
                    "goal": "",
                    "depends_on": ["build"],
                    "output_contract": {"artifacts": ["git_commit"]},
                },
            ],
        }
    )
    gws = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    run = runs_mod.create_run_from_dag(conn, dag, budgets=default_budgets(max_wallclock_s=300), capabilities=set(CAPS))
    ws, threads, stop = _workers([run.id], n=1, workspace=gws)
    orch_stop, ver_stop = threading.Event(), threading.Event()
    orch = threading.Thread(
        target=scheduler.orchestrate_forever,
        args=(DB_URL,),
        kwargs=dict(verifier=DeferredVerification(), tick_s=0.05, stop=orch_stop, workspace=gws),
        daemon=True,
    )
    real = AcceptanceVerifier(ROOT / "acceptance", image=verifier_image, limits=SandboxLimits(timeout_s=300))
    ver = threading.Thread(
        target=scheduler.verify_forever,
        args=(DB_URL,),
        kwargs=dict(verifier=real, tick_s=0.1, stop=ver_stop, workspace=gws, max_parallel=1),
        daemon=True,
    )
    orch.start()
    ver.start()
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and not sm.get_run(conn, run.id).status.terminal:
            time.sleep(0.2)
    finally:
        orch_stop.set()
        ver_stop.set()
        orch.join(30)
        ver.join(30)
        stop.set()
        wait_all(threads, 10)
    final = sm.get_run(conn, run.id)
    assert final.status is RunStatus.PASSED, final.verdict
    meta = conn.execute("SELECT meta FROM artifacts WHERE run_id=%s AND type='verification'", (run.id,)).fetchone()["meta"]
    assert meta["verifier"] == "acceptance-docker-v1" and meta["status"] == "PASS"
    assert [c["id"] for c in meta["report"]["checks"]] == [
        "compiles",
        "tests_pass",
        "health_ok",
        "shorten_created",
        "resolve_redirects",
        "survives_restart",
    ]
