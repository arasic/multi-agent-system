"""Permanent key-less width-benchmark gate: real worktrees + trusted adapter suite + real sandbox verifier."""

import threading
from pathlib import Path

import pytest

from mas.evaluation import width_dag
from mas.models.enums import RunStatus
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.verifier.acceptance import AcceptanceVerifier
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.stub import StubAgent
from mas.workers.workspace import GitWorkspace
from tests.conftest import CAPS, DB_URL, default_budgets

# Explicit markers keep the pure unit tier key-less and database-free.
pytestmark = [pytest.mark.db, pytest.mark.docker]


def test_width_four_stub_fixture_passes_real_acceptance(conn, tmp_path, verifier_image):
    root = Path(__file__).resolve().parents[1]
    dag = width_dag(4)
    run = runs_mod.create_run_from_dag(conn, dag, budgets=default_budgets(max_wallclock_s=180), capabilities=set(CAPS))
    workspace = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    stop = threading.Event()
    workers = [
        Worker(
            f"width-{i}",
            list(CAPS),
            StubAgent({"sleep_s": 0.01}),
            database_url=DB_URL,
            poll_s=0.02,
            run_id=run.id,
            workspace=workspace,
        )
        for i in range(4)
    ]
    threads = [run_worker_thread(w, stop) for w in workers]
    try:
        final = scheduler.run_until_terminal(
            conn,
            run.id,
            verifier=AcceptanceVerifier(root / "acceptance", image=verifier_image),
            workspace=workspace,
            timeout_s=90,
        )
    finally:
        stop.set()
        wait_all(threads, 10)
    assert final.status is RunStatus.PASSED
