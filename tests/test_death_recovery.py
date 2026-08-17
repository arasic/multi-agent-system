"""Step 10 gate — death recovery across the intelligence path: an LLM worker killed mid-attempt, a provider outage,
the execution runner dying mid-command, and a sandbox container killed underneath the runner. Every case must end in
a run verdict inside budget with the evidence on record, and never a leaked sandbox."""

from __future__ import annotations

import subprocess
import threading
import time

import pytest

from mas import metrics
from mas.models.enums import AttemptStatus, RunStatus
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.orchestrator import state_machine as sm
from mas.providers.base import ProviderUnavailable
from mas.providers.fake import FakeProvider, builder_script
from mas.verifier.stub import StubVerifier
from mas.workers.exec_remote import RemoteExecutionBackend
from mas.workers.exec_runner import ExecRunner
from mas.workers.execution import LocalExecutionBackend, SandboxExecutionBackend, SandboxSpec
from mas.workers.llm import LLMAgent
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.workspace import GitWorkspace
from tests.conftest import CAPS, DB_URL, default_budgets, diamond

pytestmark = pytest.mark.db


def _slow_builder(delay_s: float):
    """fake:builder, but every model turn takes `delay_s` — long enough to kill a worker mid-attempt."""

    def script(messages, tools):
        time.sleep(delay_s)
        return builder_script(messages, tools)

    return script


def _workers(run, gws, agent, provider, n, *, remote_worker_ids=None):
    ws = []
    for i in range(n):
        wid = f"dw{i}"
        factory = None
        if remote_worker_ids is not None:
            factory = lambda wt, claim, wid=wid: RemoteExecutionBackend(  # noqa: E731
                DB_URL, run_id=claim.run.id, task_id=claim.task.id, attempt_id=claim.attempt.id, worker_id=wid, poll_s=0.05
            )
        ws.append(
            Worker(
                wid,
                list(CAPS),
                agent,
                database_url=DB_URL,
                poll_s=0.05,
                run_id=run.id,
                workspace=gws,
                provider=provider,
                exec_backend_factory=factory,
            )
        )
    return ws


def _serve_runner(runner: ExecRunner):
    stop = threading.Event()
    t = threading.Thread(target=runner.serve_forever, args=(stop,), daemon=True)
    t.start()
    return t, stop


def test_llm_worker_killed_mid_attempt_is_recovered_and_its_session_closed(conn, tmp_path):
    gws = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(lease_s=1), capabilities=set(CAPS))
    provider = FakeProvider(_slow_builder(0.6), input_tokens=50, output_tokens=5, model="slow-builder")
    runner = ExecRunner(
        DB_URL,
        worktree_root=tmp_path / "worktrees",
        backend_factory=lambda wt, aid: LocalExecutionBackend(wt, unsafe_ok=True),
        lease_s=2.0,
        tick_s=0.05,
    )
    rt, rstop = _serve_runner(runner)
    ws = _workers(run, gws, LLMAgent(), provider, 3, remote_worker_ids=True)
    stop = threading.Event()
    threads = [run_worker_thread(w, stop) for w in ws]
    killed = {}

    def chaos():  # kill the first worker seen busy on an implementation task (T2..T4) mid-attempt
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            for w in ws:
                if w.busy and w.current and w.current.task.key in {"T2", "T3", "T4"}:
                    time.sleep(0.9)  # let it get past its first model turn (write_file done, run_python in flight)
                    killed["worker"], killed["task"], killed["attempt"] = w.worker_id, w.current.task.key, w.current.attempt.id
                    w.die()
                    return
            time.sleep(0.05)

    threading.Thread(target=chaos, daemon=True).start()
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=StubVerifier(True), tick_s=0.1, timeout_s=120, workspace=gws)
    finally:
        stop.set()
        wait_all(threads, 10)
        rstop.set()
        rt.join(10)
    assert killed, "chaos never found a busy implementation worker"
    assert final.status is RunStatus.PASSED, (
        final.verdict,
        [(a.status, a.failure_reason) for a in sm.attempts_for_run(conn, run.id)],
    )
    dead = sm.get_attempt(conn, killed["attempt"])
    assert dead.status is AttemptStatus.ABANDONED  # reaped, then the task was retried by a live worker
    m = metrics.compute(conn, run.id)
    assert m.abandoned == 1 and m.attempts == 6 and m.model_calls >= 6
    # the dead attempt's remote-exec session was closed by the runner (attempt no longer RUNNING) — no orphan session
    assert killed["attempt"] not in runner.sessions
    assert (
        conn.execute("SELECT count(*) AS n FROM exec_sessions WHERE attempt_id = %s", (killed["attempt"],)).fetchone()["n"] == 0
    )
    # any request the dead worker left behind is terminal (done / cancelled / error) — nothing pending or leased
    left = conn.execute("SELECT status FROM exec_requests WHERE attempt_id = %s", (killed["attempt"],)).fetchall()
    assert all(r["status"] in ("done", "cancelled", "error", "abandoned") for r in left), left


def test_provider_outage_fails_the_attempt_typed_and_the_retry_succeeds(conn, tmp_path):
    gws = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    outages = {"left": 2}

    def flaky(messages, tools):
        if outages["left"] > 0:  # the first two model calls of the run hit an outage
            outages["left"] -= 1
            raise ProviderUnavailable("upstream 503")
        return builder_script(messages, tools)

    provider = FakeProvider(flaky, input_tokens=50, output_tokens=5)
    ws = _workers(run, gws, LLMAgent(), provider, 2)
    stop = threading.Event()
    threads = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=StubVerifier(True), tick_s=0.1, timeout_s=90, workspace=gws)
    finally:
        stop.set()
        wait_all(threads, 10)
    assert final.status is RunStatus.PASSED, final.verdict
    attempts = sm.attempts_for_run(conn, run.id)
    failed = [a for a in attempts if a.status is AttemptStatus.FAILED]
    assert failed and all("model provider error" in (a.failure_reason or "") for a in failed)
    m = metrics.compute(conn, run.id)
    assert m.model_call_errors == 2 and m.retries >= 1
    errs = conn.execute("SELECT status, priced FROM model_calls WHERE run_id = %s AND status = 'error'", (run.id,)).fetchall()
    assert len(errs) == 2 and all(not e["priced"] for e in errs)  # outages are on record, unpriced


@pytest.mark.docker
def test_sandbox_killed_underneath_the_runner_is_replaced(tmp_path, verifier_image):
    root = tmp_path / "wt"
    root.mkdir()
    sb = SandboxExecutionBackend(root, attempt_id="killme", spec=SandboxSpec(image=verifier_image, max_life_s=120))
    try:
        assert sb.run_shell("echo one > a.txt", timeout_s=20).exit_code == 0
        first = sb.container
        subprocess.run(["docker", "rm", "-f", first], capture_output=True, check=False)  # the sandbox dies underneath us
        r = sb.run_shell("echo two", timeout_s=20)
        assert r.error and "gone" in r.error and r.exit_code not in (0, None)  # typed, not a silent 'No such container'
        r = sb.run_shell("cat a.txt; echo three > b.txt", timeout_s=20)  # a fresh container for the same worktree
        assert r.exit_code == 0 and "one" in r.output and (root / "b.txt").exists()
        assert sb.alive()
    finally:
        sb.close()
    assert not sb.alive()
