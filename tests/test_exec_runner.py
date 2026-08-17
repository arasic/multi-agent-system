"""Step 10 — the execution-runner path: RemoteExecutionBackend (worker side, ids only) → exec_requests (Postgres) →
ExecRunner (trusted host side: validates, derives the worktree, sandbox per attempt, bounded result).

DB tests use an injectable *local* backend on the runner (test-only, so the DB protocol is exercised without Docker);
the Docker-marked tests use the real per-attempt sandbox and check container lifecycle."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from uuid import UUID

import pytest

from mas.models.enums import AttemptStatus, RunStatus
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.orchestrator import state_machine as sm
from mas.orchestrator.leases import claim_task
from mas.verifier.stub import StubVerifier
from mas.workers.exec_remote import RemoteExecutionBackend
from mas.workers.exec_runner import ExecRunner
from mas.workers.execution import LocalExecutionBackend, SandboxSpec
from mas.workers.llm import LLMAgent
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.tools import ToolLayer
from mas.workers.workspace import GitWorkspace
from tests.conftest import CAPS, DB_URL, default_budgets, diamond

pytestmark = pytest.mark.db


# ----------------------------------------------------------------------------- fixtures / helpers


class _Live:
    """A RUNNING attempt with a real worktree (git workspace), claimed like a worker would."""

    def __init__(self, conn, tmp_path: Path, *, budgets=None):
        self.conn = conn
        self.gws = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
        self.run = runs_mod.create_run_from_dag(
            conn, diamond(), budgets=budgets or default_budgets(max_attempt_runtime_s=120), capabilities=set(CAPS)
        )
        scheduler.tick(conn, self.run.id, verifier=StubVerifier(True), workspace=self.gws)  # T1 READY
        self.claim = claim_task(conn, worker_id="w-test", capabilities=list(CAPS), lease_s=60, run_id=self.run.id, pools=None)
        assert self.claim is not None
        self.handle = self.gws.create(self.claim.run, self.claim.task, self.claim.attempt, [])
        self.worktree = self.handle.path
        self.attempt_id: UUID = self.claim.attempt.id

    def remote(self, **kw) -> RemoteExecutionBackend:
        c = self.claim
        return RemoteExecutionBackend(
            DB_URL, run_id=c.run.id, task_id=c.task.id, attempt_id=c.attempt.id, worker_id="w-test", poll_s=0.05, **kw
        )


def _local_runner(tmp_path: Path, **kw) -> ExecRunner:
    """Runner with the test-only local backend (DB protocol under test, no Docker)."""
    return ExecRunner(
        DB_URL,
        worktree_root=tmp_path / "worktrees",
        backend_factory=lambda wt, aid: LocalExecutionBackend(wt, unsafe_ok=True),
        lease_s=kw.pop("lease_s", 2.0),
        tick_s=0.05,
        **kw,
    )


def _serve(runner: ExecRunner) -> tuple[threading.Thread, threading.Event]:
    stop = threading.Event()
    t = threading.Thread(target=runner.serve_forever, args=(stop,), daemon=True)
    t.start()
    return t, stop


def _rows(conn, attempt_id):
    return conn.execute("SELECT * FROM exec_requests WHERE attempt_id = %s ORDER BY id", (attempt_id,)).fetchall()


# ----------------------------------------------------------------------------- the path works, ids only


def test_remote_backend_to_runner_to_worktree(conn, tmp_path):
    live = _implementation_attempt(conn, tmp_path)
    runner = _local_runner(tmp_path)
    t, stop = _serve(runner)
    try:
        rb = live.remote()
        res = rb.run(["python", "-c", "open('made-by-runner.txt','w').write('hi'); print('ok')"], timeout_s=30)
        assert res.exit_code == 0 and "ok" in res.output and not res.error, res
        assert (live.worktree / "made-by-runner.txt").read_text() == "hi"  # executed IN the derived worktree
        res = rb.run_shell("echo shell-works", timeout_s=30)
        assert res.exit_code == 0 and "shell-works" in res.output
        # the layer sees a confined-looking backend with the same tools as the sandbox one
        with ToolLayer(live.worktree, ["filesystem", "python", "shell"], backend=rb, close_backend=False) as tl:
            assert "run_command" in tl.tool_names()
            r = tl.dispatch("run_python", {"code": "print(6*7)"})
            assert "42" in r.content and not r.is_error
        rb.close()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and live.attempt_id in runner.sessions:
            time.sleep(0.05)
        assert live.attempt_id not in runner.sessions  # the close request closed the session
    finally:
        stop.set()
        t.join(10)
    rows = _rows(conn, live.attempt_id)
    assert [r["kind"] for r in rows] == ["argv", "shell", "argv", "close"]
    assert all(r["status"] == "done" and r["output"] is None and r["consumed_at"] is not None for r in rows[:3])
    assert rows[0]["result"]["output_sha256"] and rows[0]["result"]["output_bytes"] > 0  # bounded hashes/sizes persist
    assert rows[0]["runner_id"] == runner.runner_id
    sess = conn.execute("SELECT * FROM exec_sessions WHERE attempt_id = %s", (live.attempt_id,)).fetchone()
    assert sess is None or sess["runner_id"] == runner.runner_id


def test_no_path_is_ever_accepted_and_ids_must_match(conn, tmp_path):
    live = _Live(conn, tmp_path)
    runner = _local_runner(tmp_path)
    # the request carries ids only: a forged path can only be smuggled through the command, which runs in the worktree
    rb = live.remote()
    assert not hasattr(rb, "root") and "path" not in RemoteExecutionBackend.__init__.__code__.co_varnames
    # a request whose task/run ids do not belong to the attempt is refused
    other = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    other_task = sm.tasks_for_run(conn, other.id)[0]
    forged = RemoteExecutionBackend(
        DB_URL, run_id=other.id, task_id=other_task.id, attempt_id=live.attempt_id, worker_id="evil", poll_s=0.05
    )
    t, stop = _serve(runner)
    try:
        res = forged.run_shell("echo x", timeout_s=10)
        assert res.error and "do not match" in res.error and res.exit_code is None
        # a runner-side worktree derived from the ids: it does not exist for a bogus attempt row combination... and an
        # attempt whose worktree was never created is refused too
        live2 = _implementation_attempt(conn, tmp_path / "other")  # other worktree root than the runner's → "does not exist"
        res = live2.remote().run_shell("echo x", timeout_s=10)
        assert res.error and "worktree" in res.error
    finally:
        stop.set()
        t.join(10)


def test_rejected_for_non_running_attempt_and_ungranted_family(conn, tmp_path):
    live = _Live(conn, tmp_path)
    runner = _local_runner(tmp_path)
    # T1 is an architecture task: tools = filesystem + model (no shell / python)
    assert set(live.claim.task.tools) == {"filesystem", "model"}
    t, stop = _serve(runner)
    try:
        res = live.remote().run_shell("echo x", timeout_s=10)
        assert res.error and "not granted" in res.error
        res = live.remote().run(["python", "-c", "print(1)"], timeout_s=10)
        assert res.error and "not granted" in res.error
        # settle the attempt → not RUNNING → refused
        with conn.transaction():
            sm.settle_failed_attempt(conn, live.attempt_id, AttemptStatus.FAILED, reason="test")
        res = live.remote().run_shell("echo x", timeout_s=10)
        assert res.error and "not RUNNING" in res.error
    finally:
        stop.set()
        t.join(10)
    assert runner.stats["error"] == 3 and runner.stats["done"] == 0


def _implementation_attempt(conn, tmp_path):
    """A RUNNING attempt whose task grants shell/python (T2..T4 after T1 completes)."""
    live = _Live(conn, tmp_path)
    with conn.transaction():
        from mas.artifacts import store

        store.publish(
            conn,
            run_id=live.run.id,
            task_id=live.claim.task.id,
            attempt_id=live.attempt_id,
            type="document",
            ref="doc:design.md",
            meta={"name": "design.md"},
        )
        sm.complete_attempt(conn, live.attempt_id)
    scheduler.tick(conn, live.run.id, verifier=StubVerifier(True), workspace=live.gws)
    claim = claim_task(conn, worker_id="w-impl", capabilities=list(CAPS), lease_s=120, run_id=live.run.id, pools=None)
    assert claim is not None and "shell" in claim.task.tools
    handle = live.gws.create(claim.run, claim.task, claim.attempt, [])
    live.claim, live.handle, live.worktree, live.attempt_id = claim, handle, handle.path, claim.attempt.id
    return live


def test_two_runners_never_execute_the_same_request_and_one_owns_an_attempt(conn, tmp_path):
    live = _implementation_attempt(conn, tmp_path)
    r1, r2 = _local_runner(tmp_path, runner_id="runner-A"), _local_runner(tmp_path, runner_id="runner-B")
    t1, s1 = _serve(r1)
    t2, s2 = _serve(r2)
    try:
        rb = live.remote()
        for i in range(8):
            res = rb.run_shell(f"echo req-{i}", timeout_s=10)
            assert res.exit_code == 0 and f"req-{i}" in res.output
    finally:
        s1.set()
        s2.set()
        t1.join(10)
        t2.join(10)
    rows = _rows(conn, live.attempt_id)
    assert len(rows) == 8 and all(r["status"] == "done" for r in rows)
    owners = {r["runner_id"] for r in rows}
    assert len(owners) == 1  # one runner owned the attempt's session; the other never touched its requests
    assert (r1.stats["done"], r2.stats["done"]) in {(8, 0), (0, 8)}


def test_worker_cancel_and_attempt_settlement_cancel_a_running_command(conn, tmp_path):
    live = _implementation_attempt(conn, tmp_path)
    runner = _local_runner(tmp_path)
    t, stop = _serve(runner)
    try:
        cancel = threading.Event()
        threading.Timer(0.8, cancel.set).start()
        t0 = time.monotonic()
        res = live.remote().run(["python", "-c", "import time; time.sleep(30)"], timeout_s=60, cancel=cancel)
        assert res.cancelled and time.monotonic() - t0 < 10
        # the attempt leaves RUNNING mid-command (worker died → reaper) → runner cancels and closes the session
        rb = live.remote()
        out = {}

        def _run():
            out["res"] = rb.run(["python", "-c", "import time; time.sleep(30)"], timeout_s=60)

        th = threading.Thread(target=_run)
        th.start()
        time.sleep(1.0)
        with conn.transaction():
            sm.settle_failed_attempt(conn, live.attempt_id, AttemptStatus.ABANDONED, reason="worker died")
        th.join(15)
        assert not th.is_alive() and (out["res"].cancelled or out["res"].error), out
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and live.attempt_id in runner.sessions:
            time.sleep(0.05)
        assert live.attempt_id not in runner.sessions  # session GC closed the sandbox
    finally:
        stop.set()
        t.join(10)


def test_flood_and_timeout_stay_bounded_through_the_remote_transport(conn, tmp_path):
    live = _implementation_attempt(conn, tmp_path)
    runner = ExecRunner(
        DB_URL,
        worktree_root=tmp_path / "worktrees",
        backend_factory=lambda wt, aid: LocalExecutionBackend(wt, unsafe_ok=True, max_output_bytes=8_000),
        lease_s=2.0,
        tick_s=0.05,
        max_result_output_bytes=4_000,
    )
    t, stop = _serve(runner)
    try:
        rb = live.remote()
        res = rb.run(["python", "-c", "import sys\nwhile True: sys.stdout.write('y' * 65536); sys.stdout.flush()"], timeout_s=30)
        assert res.truncated and len(res.output) < 4_500 and "[output truncated by runner]" in res.output
        t0 = time.monotonic()
        res = rb.run(["python", "-c", "import time; time.sleep(30)"], timeout_s=1.5)
        assert res.timed_out and time.monotonic() - t0 < 12
    finally:
        stop.set()
        t.join(10)
    rows = _rows(conn, live.attempt_id)
    assert rows[0]["result"]["output_bytes"] <= 4_100 and rows[0]["result"]["truncated"] is True
    assert rows[1]["result"]["timed_out"] is True and rows[1]["output"] is None  # consumed → cleared


def test_runner_death_mid_command_yields_a_typed_abandoned_result(conn, tmp_path):
    """The lease of a request whose runner died is reaped by the next runner: ABANDONED, never replayed."""
    live = _implementation_attempt(conn, tmp_path)
    # a request that looks mid-flight from a dead runner: leased with an expired lease
    conn.execute(
        """
        INSERT INTO exec_requests (run_id, task_id, attempt_id, worker_id, family, kind, command, timeout_s, status, runner_id,
                                   lease_until, started_at)
        VALUES (%s, %s, %s, 'w', 'shell', 'shell', 'echo never-again', 30, 'leased', 'dead-runner',
                now() - interval '5 seconds', now())
        """,
        (live.run.id, live.claim.task.id, live.attempt_id),
    )
    runner = _local_runner(tmp_path)
    runner.run_once(conn)
    row = _rows(conn, live.attempt_id)[0]
    assert row["status"] == "abandoned" and row["result"]["abandoned"] is True and "not replayed" in row["result"]["error"]
    # the worker side sees it typed
    rb = live.remote()
    res = rb._wait(int(row["id"]), timeout_s=30, cancel=None)
    assert res.abandoned and "not replayed" in (res.error or "") and "ABANDONED" in res.render()
    # a live worker request afterwards works normally: the runner took over the attempt session
    t, stop = _serve(runner)
    try:
        assert live.remote().run_shell("echo alive", timeout_s=10).exit_code == 0
    finally:
        stop.set()
        t.join(10)


def test_no_runner_means_a_typed_error_not_a_hang(conn, tmp_path):
    live = _implementation_attempt(conn, tmp_path)
    rb = live.remote(pickup_grace_s=0.5)
    t0 = time.monotonic()
    res = rb.run_shell("echo x", timeout_s=10)
    assert res.error and "no execution runner" in res.error and time.monotonic() - t0 < 5
    assert _rows(conn, live.attempt_id)[0]["status"] == "cancelled"


def test_llm_worker_end_to_end_through_the_remote_path(conn, tmp_path):
    """The compose shape, in-process: workers with the remote backend, a runner (local backend here) executing their
    commands, the diamond built by the scripted LLM loop."""
    from mas.providers.fake import FakeProvider
    from tests.test_llm_runtime import _scripted_builder

    gws = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    provider = FakeProvider(_scripted_builder, input_tokens=100, output_tokens=10)
    runner = _local_runner(tmp_path)
    rt, rstop = _serve(runner)
    stop = threading.Event()
    ws = [
        Worker(
            f"cw{i}",
            list(CAPS),
            LLMAgent(),
            database_url=DB_URL,
            poll_s=0.05,
            run_id=run.id,
            workspace=gws,
            provider=provider,
            exec_backend_factory=lambda wt, claim, i=i: RemoteExecutionBackend(
                DB_URL, run_id=claim.run.id, task_id=claim.task.id, attempt_id=claim.attempt.id, worker_id=f"cw{i}", poll_s=0.05
            ),
        )
        for i in range(2)
    ]
    threads = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=StubVerifier(True), tick_s=0.1, timeout_s=120, workspace=gws)
    finally:
        stop.set()
        wait_all(threads, 10)
        rstop.set()
        rt.join(10)
    assert final.status is RunStatus.PASSED, final.verdict
    reqs = conn.execute("SELECT kind, status FROM exec_requests WHERE run_id = %s ORDER BY id", (run.id,)).fetchall()
    assert [r["kind"] for r in reqs].count("argv") == 3  # run_python once per implementation task
    assert all(r["status"] == "done" for r in reqs)
    assert not runner.sessions  # every session closed (close requests / settlement GC)
    traces = conn.execute("SELECT meta FROM artifacts WHERE run_id = %s AND type = 'log'", (run.id,)).fetchall()
    assert all(t["meta"]["sandbox"]["backend"] == "sandbox-remote" for t in traces)


# ----------------------------------------------------------------------------- Docker: the real sandbox behind the runner


@pytest.mark.docker
def test_remote_path_with_real_sandbox_and_runner_process_death(conn, tmp_path, verifier_image):
    live = _implementation_attempt(conn, tmp_path)
    spec = SandboxSpec(image=verifier_image, max_life_s=20)
    runner = ExecRunner(DB_URL, worktree_root=tmp_path / "worktrees", spec=spec, lease_s=2.0, tick_s=0.05, runner_id="sbx-runner")
    t, stop = _serve(runner)
    container = f"mas-exec-{str(live.attempt_id).replace('-', '')[:24]}"
    try:
        rb = live.remote()
        res = rb.run_shell("echo made > from-sandbox.txt; ls /data 2>&1; cat /etc/hostname", timeout_s=30)
        assert res.exit_code == 0 and (live.worktree / "from-sandbox.txt").read_text().strip() == "made"
        assert "No such file" in res.output  # no host paths inside
        sess = conn.execute("SELECT * FROM exec_sessions WHERE attempt_id = %s", (live.attempt_id,)).fetchone()
        assert (
            sess["runner_id"] == "sbx-runner" and sess["container"] == container and str(sess["image_id"]).startswith("sha256:")
        )
        assert rb.identity()["image_id"] == sess["image_id"]
    finally:
        stop.set()
        t.join(10)
    assert (
        subprocess.run(
            ["docker", "ps", "-a", "-q", "--filter", f"name=^{container}$"], capture_output=True, text=True
        ).stdout.strip()
        == ""
    )

    # runner process killed mid-command: the request is reaped as abandoned by the next runner; the container ends by
    # itself (container-side timeout on the command, --rm + max_life_s on the container)
    env = dict(
        os.environ,
        MAS_DATABASE_URL=DB_URL,
        MAS_WORKTREE_ROOT=str(tmp_path / "worktrees"),
        MAS_EXEC_IMAGE=verifier_image,
        MAS_EXEC_MAX_LIFE_S="20",
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "mas.cli", "execute", "--watch", "--tick-s", "0.1", "--id", "doomed"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
    )
    try:
        rb = live.remote(pickup_grace_s=30)
        holder: dict = {}
        th = threading.Thread(target=lambda: holder.setdefault("res", rb.run_shell("sleep 8; echo late", timeout_s=8)))
        th.start()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            row = _rows(conn, live.attempt_id)[-1]
            if row["status"] == "leased" and row["runner_id"] == "doomed":
                break
            time.sleep(0.1)
        assert row["status"] == "leased", row
        time.sleep(0.5)
        proc.kill()  # the runner dies mid-command
        proc.wait(10)
        # a new runner reaps the lease → abandoned
        r2 = ExecRunner(DB_URL, worktree_root=tmp_path / "worktrees", spec=spec, lease_s=2.0, tick_s=0.05, runner_id="successor")
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            r2.reap(conn)
            if _rows(conn, live.attempt_id)[-1]["status"] == "abandoned":
                break
            time.sleep(0.2)
        th.join(20)
        assert holder["res"].abandoned, holder
        # the doomed runner's container ends by itself within max_life_s (+ grace)
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if (
                subprocess.run(
                    ["docker", "ps", "-a", "-q", "--filter", f"name=^{container}$"], capture_output=True, text=True
                ).stdout.strip()
                == ""
            ):
                break
            time.sleep(1)
        assert (
            subprocess.run(
                ["docker", "ps", "-a", "-q", "--filter", f"name=^{container}$"], capture_output=True, text=True
            ).stdout.strip()
            == ""
        )
    finally:
        if proc.poll() is None:
            proc.kill()
        if os.name != "nt":
            try:
                os.kill(proc.pid, signal.SIGKILL)
            except Exception:
                pass
