"""Test fixtures. DB tests need a reachable Postgres (docker compose up -d postgres); they are skipped otherwise.

Isolation: every pytest process gets its OWN throwaway database (`mas_test_<pid>`), created at session start
from the server in MAS_DATABASE_URL and dropped at session end. So two test runs at once, or tests while the
compose orchestrator/workers are up on the `mas` database, never interfere. Tables are truncated between tests.

The whole suite runs with NO API key — stub workers; explicit stub verification for substrate tests and the real
Docker acceptance runner for Step 7 fixtures (CLAUDE.md rule).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from mas.db import connect, migrate
from mas.models.types import Budgets, Run
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.planner.dag import DagSpec
from mas.verifier.base import Verifier
from mas.verifier.stub import StubVerifier
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.stub import StubAgent

CAPS = ("architecture", "implementation", "testing", "integration", "solve")
REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_URL = os.environ.get("MAS_DATABASE_URL", "postgresql://mas:mas@localhost:5432/mas")
TEST_DB = f"mas_test_{os.getpid()}"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Tiers by capability, so the default loop is fast (see CLAUDE.md "Tests"): any test that uses a Docker-backed
    fixture is `docker`; a test whose module is `db` is `db`; everything else is pure unit. `scripts/test.py` selects:
    unit = not db and not docker · core = not docker · full = everything (run with -n auto: per-process test DBs)."""
    for item in items:
        if "verifier_image" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.docker)


# Set once the session DB exists; read by execute() and by the CLI (via MAS_DATABASE_URL) during the session.
DB_URL = make_conninfo(BASE_URL, dbname=TEST_DB)


def _url(dbname: str) -> str:
    return make_conninfo(BASE_URL, dbname=dbname)


def _server_available() -> bool:
    try:
        with psycopg.connect(_url("postgres"), connect_timeout=2):
            return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def test_db_url() -> str:
    """Create a per-process database, point MAS_DATABASE_URL at it for the session, drop it afterwards."""
    if not _server_available():
        pytest.skip(f"PostgreSQL not reachable at {conninfo_to_dict(BASE_URL).get('host')} (docker compose up -d postgres)")
    admin = psycopg.connect(_url("postgres"), autocommit=True)
    admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(TEST_DB)))
    admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TEST_DB)))
    prev = os.environ.get("MAS_DATABASE_URL")
    os.environ["MAS_DATABASE_URL"] = DB_URL
    try:
        with connect(DB_URL) as c:
            migrate(c)
        yield DB_URL
    finally:
        if prev is None:
            os.environ.pop("MAS_DATABASE_URL", None)
        else:
            os.environ["MAS_DATABASE_URL"] = prev
        try:
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(TEST_DB)))
        finally:
            admin.close()


@pytest.fixture(scope="session")
def migrated(test_db_url: str) -> None:
    return None  # migrations applied in test_db_url; kept for readability of dependent fixtures


@pytest.fixture
def conn(migrated: None):
    c = connect(DB_URL)
    with c.transaction():
        c.execute("TRUNCATE runs CASCADE")
    yield c
    c.close()


# ----------------------------------------------------------------------------- DAG builders


def diamond(stub: dict[str, dict[str, Any]] | None = None, *, integration: bool = True) -> DagSpec:
    """T1 → {T2, T3, T4} → T5(integration). `stub` = per-task stub script overrides."""
    stub = stub or {}

    def t(id: str, cap: str, deps: list[str], arts: list[str]) -> dict[str, Any]:
        d: dict[str, Any] = {
            "id": id,
            "capability": cap,
            "goal": f"do {id}",
            "depends_on": deps,
            "output_contract": {"artifacts": arts},
        }
        if id in stub:
            d["meta"] = {"stub": stub[id]}
        return d

    tasks = [
        t("T1", "architecture", [], ["document:design.md"]),
        t("T2", "implementation", ["T1"], ["git_commit"]),
        t("T3", "implementation", ["T1"], ["git_commit"]),
        t("T4", "implementation", ["T1"], ["git_commit"]),
    ]
    if integration:
        tasks.append(t("T5", "integration", ["T2", "T3", "T4"], ["git_commit"]))
    return DagSpec.from_dict({"goal": "diamond", "tasks": tasks})


# ----------------------------------------------------------------------------- run harness


@dataclass
class Outcome:
    run: Run
    workers: list[Worker]
    agent: StubAgent
    verifier: Any
    threads: list[threading.Thread] = field(default_factory=list)


def default_budgets(**over: Any) -> Budgets:
    base = dict(
        max_concurrency=4,
        max_tasks=50,
        max_attempts_per_task=3,
        max_replans=2,
        max_plan_attempts=3,
        max_tokens=2_000_000,
        max_cost_usd=20.0,
        max_wallclock_s=60,
        max_attempt_runtime_s=30,
        lease_s=1,
    )
    base.update(over)
    return Budgets(**base)


def execute(
    conn,
    dag: DagSpec,
    *,
    workers: int = 3,
    caps: tuple[str, ...] = CAPS,
    budgets: Budgets | None = None,
    verifier: Verifier | None = None,
    stub_sleep: float = 0.3,
    on_start: Callable[[list[Worker]], None] | None = None,
    timeout: float = 90,  # > default max_wallclock_s (60): the run's own budget must abort first (I-4)
) -> Outcome:
    budgets = budgets or default_budgets()
    verifier = verifier or StubVerifier(passed=True)
    run = runs_mod.create_run_from_dag(conn, dag, budgets=budgets, capabilities=set(caps))
    stop = threading.Event()
    agent = StubAgent(default_script={"sleep_s": stub_sleep})
    ws = [Worker(f"w{i + 1}", list(caps), agent, database_url=DB_URL, poll_s=0.05, run_id=run.id) for i in range(workers)]
    threads = [run_worker_thread(w, stop) for w in ws]
    if on_start:
        on_start(ws)
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=verifier, tick_s=0.1, timeout_s=timeout)
    finally:
        stop.set()
        wait_all(threads, 10)
    return Outcome(run=final, workers=ws, agent=agent, verifier=verifier, threads=threads)


# ----------------------------------------------------------------------------- Docker sandbox image (Step 7 tests)


@pytest.fixture(scope="session")
def verifier_image():
    if shutil.which("docker") is None:
        pytest.skip("Docker CLI is unavailable")
    ping = subprocess.run(["docker", "info"], capture_output=True, timeout=15, check=False)
    if ping.returncode != 0:
        pytest.skip("Docker daemon is unavailable")
    image = f"mas-verifier-test:{os.getpid()}"
    built = subprocess.run(
        ["docker", "build", "-q", "-f", "acceptance/Dockerfile.verifier", "-t", image, "."],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert built.returncode == 0, built.stderr
    yield image
    subprocess.run(["docker", "image", "rm", "-f", image], capture_output=True, timeout=30, check=False)
