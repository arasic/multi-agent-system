"""A7 — conflicts are representable and resolved visibly (evaluation A7; antipatterns B8; architecture §6).

Two tasks answer the same design question → two `candidate` artifacts for one output slot → the consuming task must
publish a `decision` (winner, losers, rationale) → the runtime accepts the winner and supersedes the losers, in the same
report transaction. Choosing silently is a contract violation. Deterministic runtime; the *judgement* is the agent's
(stub policy here; the LLM agent's `finish.decisions` in the unit test). No LLM, no key."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from mas.artifacts import store
from mas.db.events import for_run
from mas.models.enums import ArtifactStatus, AttemptStatus, RunStatus
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.orchestrator import state_machine as sm
from mas.orchestrator.contracts import competing_inputs, slot_of
from mas.planner.dag import DagSpec
from mas.verifier.stub import StubVerifier
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.stub import StubAgent
from mas.workers.workspace import GitWorkspace, git_available
from tests.conftest import CAPS, DB_URL, default_budgets

pytestmark = pytest.mark.db
ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = ROOT / "benchmarks" / "forced_disagreement" / "dag.json"


def _dag(decide: str = "ARCH_B") -> DagSpec:
    d = json.loads(DAG_FILE.read_text(encoding="utf-8"))
    for t in d["tasks"]:
        if t["id"] == "IMPL":
            t["meta"] = {"stub": {"decide": decide}}
    return DagSpec.from_dict(d)


def _run(conn, dag, *, workspace=None, budgets=None, workers=2, timeout=90):
    run = runs_mod.create_run_from_dag(conn, dag, budgets=budgets or default_budgets(), capabilities=set(CAPS))
    stop = threading.Event()
    agent = StubAgent({"sleep_s": 0.02})
    ws = [
        Worker(f"w{i}", list(CAPS), agent, database_url=DB_URL, poll_s=0.05, run_id=run.id, workspace=workspace)
        for i in range(workers)
    ]
    ts = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(
            conn, run.id, verifier=StubVerifier(True), workspace=workspace, tick_s=0.1, timeout_s=timeout
        )
    finally:
        stop.set()
        wait_all(ts, 10)
    return final


def _by_key(conn, run_id):
    return {t.key: t for t in sm.tasks_for_run(conn, run_id)}


# ----------------------------------------------------------------------------- representation (pure)


def test_competing_inputs_are_same_slot_from_different_tasks():
    a = SimpleNamespace(id=uuid4(), task_id=uuid4(), type="document", ref="stub:A:1:design.md", meta={"name": "design.md"})
    b = SimpleNamespace(id=uuid4(), task_id=uuid4(), type="document", ref="abc123:docs/design.md", meta={})
    c = SimpleNamespace(id=uuid4(), task_id=a.task_id, type="document", ref="stub:A:1:api.md", meta={"name": "api.md"})
    g = SimpleNamespace(id=uuid4(), task_id=uuid4(), type="git_commit", ref="deadbeef", meta={})
    assert slot_of(a) == "document:design.md" and slot_of(b) == "document:design.md" and slot_of(c) == "document:api.md"
    assert slot_of(g) is None  # commits never compete by name: merges handle them
    comp = competing_inputs([a, b, c, g])
    assert set(comp) == {"document:design.md"} and [x.id for x in comp["document:design.md"]] == [a.id, b.id]
    # two outputs of the SAME task are not a conflict; a lone candidate is not a conflict
    assert competing_inputs([a, c]) == {} and competing_inputs([a]) == {}


# ----------------------------------------------------------------------------- the demonstration (A7)


def test_forced_disagreement_decision_and_supersession(conn):
    final = _run(conn, _dag("ARCH_B"))
    assert final.status is RunStatus.PASSED, final.verdict
    tasks = _by_key(conn, final.id)
    a = store.for_task(conn, tasks["ARCH_A"].id)[0]
    b = store.for_task(conn, tasks["ARCH_B"].id)[0]
    assert a.type == b.type == "document"
    # the winner is accepted, the loser superseded BY the winner — both stay on record (nothing deleted, nothing merged away)
    a, b = sm.get_artifact(conn, a.id), sm.get_artifact(conn, b.id)
    assert b.status is ArtifactStatus.ACCEPTED and a.status is ArtifactStatus.SUPERSEDED and a.superseded_by == b.id
    # the decision is an immutable artifact of the consumer's SUCCESS attempt, naming winner, losers and rationale
    decisions = [x for x in store.for_task(conn, tasks["IMPL"].id) if x.type == "decision"]
    assert len(decisions) == 1
    d = decisions[0]
    assert d.ref == "decision:document:design.md" and d.meta["slot"] == "document:design.md"
    assert d.meta["winner"] == str(b.id) and d.meta["losers"] == [str(a.id)] and "ARCH_B" in d.meta["rationale"]
    assert d.meta["producer"] == "IMPL"
    ev = [e for e in for_run(conn, final.id) if e.type == "artifact.decided"]
    assert len(ev) == 1 and ev[0].payload["winner"] == str(b.id) and ev[0].payload["losers"] == [str(a.id)]
    types = [e.type for e in for_run(conn, final.id)]
    assert "artifact.accepted" in types and "artifact.superseded" in types
    # a downstream reader (INTEG) sees only what is still live: superseded artifacts are not dependency outputs? They
    # remain the producer's outputs (history) — the *decision* is what tells a reader which one won.
    assert sm.get_artifact(conn, a.id).status is ArtifactStatus.SUPERSEDED


def test_choosing_silently_is_a_contract_violation(conn):
    """A consumer that ignores competing inputs (publishes no decision) fails its attempt: output contract unmet
    'decision:<slot>'. With max_attempts=1 the task FAILS and the run ends unrecoverable (no planner)."""
    final = _run(conn, _dag("none"), budgets=default_budgets(max_attempts_per_task=1))
    assert final.status is RunStatus.FAILED
    tasks = _by_key(conn, final.id)
    atts = [a for a in sm.attempts_for_run(conn, final.id) if a.task_id == tasks["IMPL"].id]
    assert len(atts) == 1 and atts[0].status is AttemptStatus.FAILED
    assert "output contract unmet" in atts[0].failure_reason and "decision:document:design.md" in atts[0].failure_reason
    # nothing was decided: both candidates untouched
    for key in ("ARCH_A", "ARCH_B"):
        assert store.for_task(conn, tasks[key].id)[0].status is ArtifactStatus.CANDIDATE


def test_decision_can_only_crown_a_competing_artifact(conn):
    """The runtime validates decisions against the competing set it handed over: a forged winner is rejected as an
    invalid decision (the attempt fails), and the candidates stay untouched."""
    from mas.orchestrator import leases

    dag = _dag("ARCH_B")
    run = runs_mod.create_run_from_dag(conn, dag, budgets=default_budgets(max_attempts_per_task=1), capabilities=set(CAPS))
    # let ARCH_A / ARCH_B complete with stub workers, then claim IMPL by hand and report a forged decision
    stop = threading.Event()
    agent = StubAgent({"sleep_s": 0.02})
    ws = [Worker(f"w{i}", ["architecture"], agent, database_url=DB_URL, poll_s=0.05, run_id=run.id) for i in range(2)]
    ts = [run_worker_thread(w, stop) for w in ws]
    try:
        for _ in range(200):
            scheduler.tick(conn, run.id, verifier=StubVerifier(True))
            tasks = _by_key(conn, run.id)
            if tasks["IMPL"].status.value == "READY":
                break
            threading.Event().wait(0.05)
    finally:
        stop.set()
        wait_all(ts, 10)
    tasks = _by_key(conn, run.id)
    assert tasks["IMPL"].status.value == "READY"
    claim = leases.claim_task(conn, worker_id="me", capabilities=["implementation"], run_id=run.id)
    assert claim is not None and claim.task.key == "IMPL"
    a = store.for_task(conn, tasks["ARCH_A"].id)[0]
    b = store.for_task(conn, tasks["ARCH_B"].id)[0]
    forged = leases.ArtifactSpec(
        type="decision", ref="decision:document:design.md", meta={"slot": "document:design.md", "winner": str(uuid4())}
    )
    commit = leases.ArtifactSpec(type="git_commit", ref="stub:IMPL:1:git_commit", meta={})
    t = leases.report(
        conn, claim.attempt.id, success=True, artifacts=[forged, commit], competing={"document:design.md": [a.id, b.id]}
    )
    att = sm.get_attempt(conn, claim.attempt.id)
    assert (
        att.status is AttemptStatus.FAILED
        and "invalid decision" in att.failure_reason
        and "not one of the competing" in att.failure_reason
    )
    assert t.status.value in ("FAILED", "RETRYABLE")
    assert (
        sm.get_artifact(conn, a.id).status is ArtifactStatus.CANDIDATE
        and sm.get_artifact(conn, b.id).status is ArtifactStatus.CANDIDATE
    )
    assert not [x for x in store.for_attempt(conn, claim.attempt.id) if x.type == "decision"]  # never published


@pytest.mark.skipif(not git_available(), reason="git not on PATH")
def test_forced_disagreement_with_git_worktrees(conn, tmp_path: Path):
    """The same demonstration with real worktrees: both designs are files (`docs/design.md`) from different branches; the
    consumer decides; the winner is accepted, the loser superseded; the run passes."""
    gws = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    final = _run(conn, _dag("ARCH_A"), workspace=gws, timeout=120)
    assert final.status is RunStatus.PASSED, final.verdict
    tasks = _by_key(conn, final.id)
    a = [x for x in store.for_task(conn, tasks["ARCH_A"].id) if x.type == "document"][0]
    b = [x for x in store.for_task(conn, tasks["ARCH_B"].id) if x.type == "document"][0]
    a, b = sm.get_artifact(conn, a.id), sm.get_artifact(conn, b.id)
    assert a.status is ArtifactStatus.ACCEPTED and b.status is ArtifactStatus.SUPERSEDED and b.superseded_by == a.id
    d = [x for x in store.for_task(conn, tasks["IMPL"].id) if x.type == "decision"][0]
    assert d.meta["winner"] == str(a.id) and d.meta["losers"] == [str(b.id)]
