"""Step 10, part 2 — LLMAgent through the real worker runtime (DB + git worktrees, scripted FakeProvider):
the diamond DAG is built end to end by the tool-call loop, traces are published, telemetry lines up, and the runtime
owns the execution backend's lifetime."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from mas import metrics
from mas.models.enums import AttemptStatus, RunStatus
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.orchestrator import state_machine as sm
from mas.providers.fake import FakeProvider
from mas.verifier.stub import StubVerifier
from mas.workers.execution import LocalExecutionBackend
from mas.workers.llm import LLMAgent
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.workspace import GitWorkspace
from tests.conftest import CAPS, DB_URL, default_budgets, diamond

pytestmark = pytest.mark.db


def _scripted_builder(messages, tools):
    """A 'model' that reads the brief and does the task: architecture → docs/design.md; implementation → src/<task>.txt
    (+ runs pytest when it can); integration → merge is done by input assembly, just finish."""
    brief = messages[1]["content"]
    key = brief.split("# Task ")[1].split(" ")[0]
    tool_names = {t["name"] for t in tools}
    already = [m for m in messages if m["role"] == "tool"]
    if "document:design.md" in brief:
        if not already:
            return {
                "tool_calls": [{"id": "w", "name": "write_file", "input": {"path": "docs/design.md", "content": "# design\n"}}]
            }
        return {
            "tool_calls": [
                {
                    "id": "f",
                    "name": "finish",
                    "input": {
                        "success": True,
                        "summary": "designed",
                        "artifacts": [{"type": "document", "path": "docs/design.md", "name": "design.md"}],
                    },
                }
            ]
        }
    if key.startswith("T5") or "integration" in brief.split("\n")[0]:
        return {"tool_calls": [{"id": "f", "name": "finish", "input": {"success": True, "summary": "integrated"}}]}
    # implementation
    if not already:
        return {"tool_calls": [{"id": "w", "name": "write_file", "input": {"path": f"src/{key}.txt", "content": f"{key}\n"}}]}
    if len(already) == 1 and "run_pytest" in tool_names:
        return {"tool_calls": [{"id": "p", "name": "run_python", "input": {"code": f"print('checked {key}')"}}]}
    return {"tool_calls": [{"id": "f", "name": "finish", "input": {"success": True, "summary": f"implemented {key}"}}]}


def _run(conn, tmp_path: Path, *, workers: int = 3, exec_factory=None):
    gws = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    stop = threading.Event()
    provider = FakeProvider(_scripted_builder, input_tokens=200, output_tokens=30, model="fake-builder")
    agent = LLMAgent()
    ws = [
        Worker(
            f"llm{i + 1}",
            list(CAPS),
            agent,
            database_url=DB_URL,
            poll_s=0.05,
            run_id=run.id,
            workspace=gws,
            provider=provider,
            exec_backend_factory=exec_factory,
        )
        for i in range(workers)
    ]
    threads = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(
            conn, run.id, verifier=StubVerifier(passed=True), tick_s=0.1, timeout_s=120, workspace=gws
        )
    finally:
        stop.set()
        wait_all(threads, 10)
    return final, gws, provider


def test_llm_agent_builds_the_diamond_end_to_end(conn, tmp_path):
    final, gws, provider = _run(conn, tmp_path)
    assert final.status is RunStatus.PASSED, (
        final.verdict,
        [(a.status, a.failure_reason) for a in sm.attempts_for_run(conn, final.id)],
    )
    attempts = sm.attempts_for_run(conn, final.id)
    assert len(attempts) == 5 and all(a.status is AttemptStatus.SUCCESS for a in attempts)
    # every attempt published an execution trace; T1 also its document; T2-T4 their runtime-minted git_commit
    rows = conn.execute("SELECT * FROM artifacts WHERE run_id = %s ORDER BY created_at", (final.id,)).fetchall()
    traces = [r for r in rows if r["type"] == "log" and (r["meta"] or {}).get("kind") == "execution_trace"]
    assert len(traces) == 5
    by_task = {}
    tasks = {t.id: t for t in sm.tasks_for_run(conn, final.id)}
    for r in traces:
        by_task[tasks[r["task_id"]].key] = r["meta"]
    assert by_task["T1"]["outcome"]["success"] and [t["name"] for t in by_task["T1"]["tool_calls"]] == ["write_file", "finish"]
    assert by_task["T2"]["model"] == {"provider": "fake", "model": "fake-builder"}
    assert by_task["T2"]["sandbox"] is None  # no execution backend in this run → command tools were never offered
    docs = [r for r in rows if r["type"] == "document"]
    assert len(docs) == 1 and docs[0]["ref"].endswith(":docs/design.md") and docs[0]["meta"]["name"] == "design.md"
    commits = [r for r in rows if r["type"] == "git_commit"]
    assert len(commits) >= 4  # T2, T3, T4, T5 (integration merge)
    # the integration commit contains the three implementation files and the design doc
    integ = [c for c in commits if tasks[c["task_id"]].key == "T5"][0]
    files = gws.files_at(final.id, integ["ref"])
    assert {"src/T2.txt", "src/T3.txt", "src/T4.txt", "docs/design.md"} <= set(files)
    # telemetry: settled usage equals the meter's; per-attempt call counts match the traces
    m = metrics.compute(conn, final.id)
    assert m.model_calls == sum(t["counts"]["turns"] for t in by_task.values()) and m.model_call_errors == 0
    assert m.input_tokens == m.call_input_tokens == 200 * m.model_calls
    assert len(provider.calls) == m.model_calls


def test_runtime_owns_the_execution_backend_and_records_its_identity(conn, tmp_path):
    created: list[LocalExecutionBackend] = []

    def factory(worktree, claim):
        b = LocalExecutionBackend(worktree, unsafe_ok=True)  # test-only; the CLI never builds this one
        created.append(b)
        return b

    final, _, _ = _run(conn, tmp_path, workers=2, exec_factory=factory)
    assert final.status is RunStatus.PASSED, final.verdict
    assert len(created) == 5  # one backend per attempt...
    assert all(not b._home.exists() for b in created)  # ...closed by the runtime after settlement (home dir removed)
    rows = conn.execute("SELECT meta FROM artifacts WHERE run_id = %s AND type = 'log'", (final.id,)).fetchall()
    sandboxes = [r["meta"]["sandbox"] for r in rows]
    assert all(s and s["backend"] == "local-unconfined" for s in sandboxes)
    impl = [r["meta"] for r in rows if r["meta"]["counts"]["tool_calls"] == 3]  # implementation tasks: write, run_python, finish
    assert len(impl) == 3 and all([t["name"] for t in tr["tool_calls"]] == ["write_file", "run_python", "finish"] for tr in impl)
    assert json.dumps(rows[0]["meta"])  # trace is plain JSON in artifact meta


@pytest.mark.docker
def test_fake_builder_builds_the_url_shortener_benchmark_with_sandbox_and_real_verifier(conn, tmp_path, verifier_image):
    """The offline demo double (`fake:builder`) through the real pipeline: benchmark DAG, LLM loop, per-attempt sandbox
    for run_python, runtime commits + integration merge, real acceptance verifier on the url_shortener suite → PASS."""
    from mas import providers
    from mas.planner.dag import DagSpec
    from mas.verifier.acceptance import AcceptanceVerifier, SandboxLimits
    from mas.workers.execution import SandboxExecutionBackend, SandboxSpec

    root = Path(__file__).resolve().parents[1]
    dag = DagSpec.from_file(root / "benchmarks" / "url_shortener" / "dag.json")
    gws = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    run = runs_mod.create_run_from_dag(
        conn, dag, budgets=default_budgets(max_wallclock_s=600, max_attempt_runtime_s=300), capabilities=set(CAPS)
    )
    provider = providers.from_spec("fake:builder")
    spec = SandboxSpec(image=verifier_image, max_life_s=600)
    stop = threading.Event()
    ws = [
        Worker(
            f"b{i}",
            list(CAPS),
            LLMAgent(),
            database_url=DB_URL,
            poll_s=0.05,
            run_id=run.id,
            workspace=gws,
            provider=provider,
            exec_backend_factory=lambda wt, claim: SandboxExecutionBackend(wt, attempt_id=claim.attempt.id, spec=spec),
        )
        for i in range(3)
    ]
    threads = [run_worker_thread(w, stop) for w in ws]
    verifier = AcceptanceVerifier(root / "acceptance", image=verifier_image, limits=SandboxLimits(timeout_s=300))
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=verifier, tick_s=0.1, timeout_s=500, workspace=gws)
    finally:
        stop.set()
        wait_all(threads, 10)
    assert final.status is RunStatus.PASSED, (
        final.verdict,
        [(a.status, a.failure_reason) for a in sm.attempts_for_run(conn, run.id)],
    )
    m = metrics.compute(conn, run.id)
    assert m.tasks == 6 and m.attempts >= 6 and m.model_calls >= 6 and m.max_concurrent_attempts >= 2
    traces = conn.execute("SELECT meta FROM artifacts WHERE run_id = %s AND type = 'log'", (run.id,)).fetchall()
    impl = [t["meta"] for t in traces if any(c["name"] == "run_python" for c in t["meta"]["tool_calls"])]
    assert len(impl) == 3 and all(str(t["sandbox"]["image_id"]).startswith("sha256:") for t in impl)
