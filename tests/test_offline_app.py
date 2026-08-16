"""Step 10 — offline trivial app, end to end and for real (Docker + DB, no API key):

hand-written DAG (build → integrate)  ─▶  LLMAgent driven by a scripted FakeProvider that writes the URL-shortener
app and its tests through the tool layer, runs pytest INSIDE the per-attempt sandbox container, and calls finish
─▶  runtime commits + publishes (git_commit, execution trace with the sandbox image identity)
─▶  the real AcceptanceVerifier runs the contract suite (url_shortener_contract) on the integration commit ─▶ PASS
"""

from __future__ import annotations

import subprocess
import threading
from pathlib import Path

import pytest

from mas.models.enums import RunStatus
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.orchestrator import state_machine as sm
from mas.planner.dag import DagSpec
from mas.providers.fake import FakeProvider
from mas.verifier.acceptance import AcceptanceVerifier, SandboxLimits
from mas.workers.execution import SandboxExecutionBackend, SandboxSpec
from mas.workers.llm import LLMAgent
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.workspace import GitWorkspace
from tests.conftest import CAPS, DB_URL, default_budgets

pytestmark = [pytest.mark.db, pytest.mark.docker]
ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures" / "apps" / "known_good_with_tests"


def _builder(messages, tools):
    """Scripted 'model': write app.py, write test_app.py, run pytest in the sandbox, finish."""
    brief = messages[1]["content"]
    done = [m for m in messages if m["role"] == "tool"]
    if "integration" in brief.split("\n")[0]:
        return {"tool_calls": [{"id": "f", "name": "finish", "input": {"success": True, "summary": "integrated"}}]}
    steps = [
        ("write_file", {"path": "app.py", "content": (FIX / "app.py").read_text(encoding="utf-8")}),
        ("write_file", {"path": "test_app.py", "content": (FIX / "test_app.py").read_text(encoding="utf-8")}),
        ("run_pytest", {"args": ["test_app.py"]}),
    ]
    if len(done) < len(steps):
        name, args = steps[len(done)]
        return {"tool_calls": [{"id": f"s{len(done)}", "name": name, "input": args}]}
    last = done[-1]["content"]
    ok = "exit_code=0" in last and "passed" in last
    return {
        "tool_calls": [
            {
                "id": "f",
                "name": "finish",
                "input": {
                    "success": ok,
                    "summary": "built and tested" if ok else "tests failed",
                    "failure_reason": "" if ok else last[:300],
                },
            }
        ]
    }


def test_offline_trivial_app_built_by_the_llm_loop_passes_the_real_verifier(conn, tmp_path, verifier_image):
    dag = DagSpec.from_dict(
        {
            "goal": "url shortener (offline, scripted model)",
            "benchmark": "url_shortener_contract",
            "tasks": [
                {
                    "id": "build",
                    "capability": "implementation",
                    "goal": "implement app.py per the contract and test it",
                    "depends_on": [],
                    "output_contract": {"artifacts": ["git_commit"]},
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
    run = runs_mod.create_run_from_dag(
        conn, dag, budgets=default_budgets(max_wallclock_s=600, max_attempt_runtime_s=300), capabilities=set(CAPS)
    )
    spec = SandboxSpec(image=verifier_image, max_life_s=600)
    made: list[SandboxExecutionBackend] = []

    def factory(worktree, attempt_id):
        b = SandboxExecutionBackend(worktree, attempt_id=attempt_id, spec=spec)
        made.append(b)
        return b

    provider = FakeProvider(_builder, input_tokens=300, output_tokens=40, model="fake-builder")
    stop = threading.Event()
    w = Worker(
        "llm-1",
        list(CAPS),
        LLMAgent(),
        database_url=DB_URL,
        poll_s=0.05,
        run_id=run.id,
        workspace=gws,
        provider=provider,
        exec_backend_factory=factory,
    )
    t = run_worker_thread(w, stop)
    verifier = AcceptanceVerifier(ROOT / "acceptance", image=verifier_image, limits=SandboxLimits(timeout_s=300))
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=verifier, tick_s=0.1, timeout_s=400, workspace=gws)
    finally:
        stop.set()
        wait_all([t], 10)
    assert final.status is RunStatus.PASSED, (
        final.verdict,
        [(a.status, a.failure_reason) for a in sm.attempts_for_run(conn, final.id)],
    )
    # the sandbox ran the tests: one container for the build attempt (the integrate attempt ran no commands)
    assert len(made) == 2 and made[0].commands >= 1
    assert all(not b.alive() for b in made)  # closed by the runtime
    assert (
        subprocess.run(["docker", "ps", "-a", "-q", "--filter", "name=mas-exec-"], capture_output=True, text=True).stdout.strip()
        == ""
    )
    # evidence: traces carry the sandbox identity (image id), and the verification artifact carries the verifier's
    rows = conn.execute("SELECT type, meta FROM artifacts WHERE run_id = %s ORDER BY created_at", (run.id,)).fetchall()
    traces = [r["meta"] for r in rows if r["type"] == "log" and r["meta"].get("kind") == "execution_trace"]
    build_trace = [tr for tr in traces if tr["task"] == "build"][0]
    assert [c["name"] for c in build_trace["tool_calls"]] == ["write_file", "write_file", "run_pytest", "finish"]
    assert build_trace["sandbox"]["backend"] == "sandbox-docker" and build_trace["sandbox"]["image"] == verifier_image
    assert str(build_trace["sandbox"]["image_id"]).startswith("sha256:") and build_trace["sandbox"]["commands"] == 1
    ver = [r["meta"] for r in rows if r["type"] == "verification"][0]
    assert ver["status"] == "PASS" and ver["verifier"] == "acceptance-docker-v1"
    assert [c["id"] for c in ver["report"]["checks"]] == [
        "compiles",
        "tests_pass",
        "health_ok",
        "shorten_created",
        "resolve_redirects",
        "survives_restart",
    ]
    assert len(provider.calls) == 5  # 4 turns for build + 1 for integrate
