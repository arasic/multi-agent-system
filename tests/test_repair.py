"""Step 13-lite — bounded verifier-driven repair with deterministic no-progress detection (ADR-008 §6–7; A8).

FAIL → amendment (rule 9 protects recorded work) → PASS; a repeated progress fingerprint / repeated amendment → NO_PROGRESS;
`max_replans` is the only repair budget → BUDGET_EXHAUSTED (or NO_PROGRESS when nothing was reduced); every ending is a
verdict with a reason code. Stub workers, stub/scripted verifier, stub planner — no LLM, no key."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from mas.artifacts import store
from mas.db.events import for_run
from mas.models.enums import ArtifactStatus, RunStatus, TaskStatus, VerdictReason
from mas.models.types import Budgets
from mas.orchestrator import progress, scheduler
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import state_machine as sm
from mas.planner.dag import DagSpec
from mas.planner.planner import StubPlanner
from mas.planner.validator import ExistingTask, validate
from mas.verifier.base import CheckResult, CheckStatus, VerificationResult
from mas.verifier.stub import StubVerifier
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.stub import StubAgent
from mas.workers.workspace import GitWorkspace, git_available
from tests.conftest import CAPS, DB_URL, default_budgets, diamond

pytestmark = pytest.mark.db


def _amend(*tasks: dict) -> DagSpec:
    return DagSpec.from_dict({"tasks": list(tasks)})


def _t(id: str, cap: str, deps: list[str], arts: list[str] | None = None, **extra) -> dict:
    d = {
        "id": id,
        "capability": cap,
        "goal": f"repair {id}",
        "depends_on": deps,
        "output_contract": {"artifacts": arts or ["git_commit"]},
    }
    d.update(extra)
    return d


FIX1 = _amend(_t("FIX1", "implementation", ["T5"]), _t("FIX1_integ", "integration", ["FIX1"]))
FIX2 = _amend(_t("FIX2", "implementation", ["T5"], goal="a different repair"), _t("FIX2_integ", "integration", ["FIX2"]))


def _run(conn, *, verifier, planner, budgets=None, workspace=None, workers=3, sleep=0.03, timeout=90):
    """Diamond with stub workers, the given verifier and planner (planner drives amendments after a FAIL)."""
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=budgets or default_budgets(), capabilities=set(CAPS))
    stop = threading.Event()
    agent = StubAgent({"sleep_s": sleep})
    ws = [
        Worker(f"w{i}", list(CAPS), agent, database_url=DB_URL, poll_s=0.05, run_id=run.id, workspace=workspace)
        for i in range(workers)
    ]
    ts = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(
            conn,
            run.id,
            verifier=verifier,
            planner=planner,
            capabilities=set(CAPS),
            workspace=workspace,
            tick_s=0.1,
            timeout_s=timeout,
        )
    finally:
        stop.set()
        wait_all(ts, 10)
    return final


def _events(conn, run_id):
    return [e for e in for_run(conn, run_id)]


def _fingerprints(conn, run_id):
    return [e.payload for e in for_run(conn, run_id) if e.type == "verify.fingerprint"]


# ----------------------------------------------------------------------------- the fingerprint itself (pure)


def test_fingerprint_is_deterministic_and_folds_volatile_detail():
    rep = {
        "status": "FAIL",
        "reason": "one or more acceptance checks failed",
        "checks": [
            {"id": "build", "status": "PASS", "detail": ""},
            {"id": "http", "status": "FAIL", "detail": "expected 201 got 500 after 1.23s at /tmp/x/abc1234def/app.py"},
        ],
    }
    a = progress.failure_fingerprint(rep, integration_hash="tree:aaa", accepted=[("git_commit", "x")])
    b = progress.failure_fingerprint(dict(rep), integration_hash="tree:aaa", accepted=[("git_commit", "x")])
    assert a == b and a.value == b.value
    assert a.failing_checks == ("http",)
    assert any(c.startswith("check:http:FAIL:expected <n> got <n> after <t> at <path>") for c in a.failure_classes), (
        a.failure_classes
    )
    # the same kind of failure with other volatile bits (another duration, another path, another number) is the same class
    rep2 = dict(
        rep,
        checks=[rep["checks"][0], dict(rep["checks"][1], detail="expected 201 got 503 after 0.5s at /tmp/y/9f9f9f9f9/app.py")],
    )
    assert progress.failure_fingerprint(rep2, integration_hash="tree:aaa", accepted=[("git_commit", "x")]).value == a.value
    # a different diff, a different failing set, or different accepted artifacts is a different fingerprint
    assert progress.failure_fingerprint(rep, integration_hash="tree:bbb", accepted=[("git_commit", "x")]).value != a.value
    assert progress.failure_fingerprint(rep, integration_hash="tree:aaa", accepted=[]).value != a.value
    rep3 = dict(rep, checks=[dict(rep["checks"][0], status="FAIL"), rep["checks"][1]])
    assert progress.failure_fingerprint(rep3, integration_hash="tree:aaa", accepted=[("git_commit", "x")]).failing_checks == (
        "build",
        "http",
    )
    # amendment hash: structure + goals, ids normalized — renaming tasks does not make it a new amendment
    h1 = progress.amendment_hash([t.to_dict() for t in FIX1.tasks])
    renamed = [dict(t.to_dict(), id=t.id.replace("FIX1", "Z")) for t in FIX1.tasks]
    for t in renamed:
        t["depends_on"] = [d.replace("FIX1", "Z") for d in t["depends_on"]]
    assert progress.amendment_hash(renamed) == h1
    assert progress.amendment_hash([t.to_dict() for t in FIX2.tasks]) != h1


def test_decision_after_fail_is_deterministic():
    fp = progress.failure_fingerprint(
        {"status": "FAIL", "checks": [{"id": "a", "status": "FAIL"}]}, integration_hash="t1", accepted=[]
    )
    prev = [{**fp.as_dict(), "value": fp.value}]
    d = progress.decide_after_fail(fp, previous=prev, replans_used=1, max_replans=5)
    assert d.action == "fail" and d.reason is VerdictReason.NO_PROGRESS and d.payload == {"repeat_of_cycle": 0}
    fp2 = progress.failure_fingerprint(
        {"status": "FAIL", "checks": [{"id": "a", "status": "FAIL"}]}, integration_hash="t2", accepted=[]
    )
    assert progress.decide_after_fail(fp2, previous=prev, replans_used=1, max_replans=2).action == "replan"
    d = progress.decide_after_fail(fp2, previous=prev, replans_used=2, max_replans=2)
    assert d.action == "fail" and d.reason is VerdictReason.NO_PROGRESS and "did not reduce" in d.detail
    fewer = progress.failure_fingerprint({"status": "FAIL", "checks": []}, integration_hash="t3", accepted=[])
    d = progress.decide_after_fail(fewer, previous=prev, replans_used=2, max_replans=2)
    assert d.action == "fail" and d.reason is VerdictReason.BUDGET_EXHAUSTED
    d = progress.decide_after_fail(fp, previous=[], replans_used=0, max_replans=0)
    assert d.action == "fail" and d.reason is VerdictReason.BUDGET_EXHAUSTED and "no repair budget" in d.detail
    assert progress.decide_after_fail(fp, previous=[], replans_used=0, max_replans=1).action == "replan"


# ----------------------------------------------------------------------------- rule 9 (validator, no DB)


def test_rule9_amendment_protections():
    prior = [
        ExistingTask("T1", "COMPLETED"),
        ExistingTask("T2", "COMPLETED", ("T1",)),
        ExistingTask("T5", "COMPLETED", ("T2",)),
        ExistingTask("TX", "FAILED", ("T1",)),
        ExistingTask("T_integrate", "COMPLETED", ("T2",)),
    ]
    r = validate(_amend(_t("T2", "implementation", ["T1"])), existing=prior)
    assert any(e.rule == "9" and "may not alter existing task 'T2' (COMPLETED)" in e.message for e in r.errors)
    r = validate(_amend(_t("FIX", "implementation", ["TX"])), existing=prior)
    assert any(e.rule == "9" and "'TX' is FAILED" in e.message for e in r.errors)
    r = validate(_amend(_t("FIX", "implementation", ["T5"], context_spec={"artifacts_from": ["TX"]})), existing=prior)
    assert any(e.rule == "10" or e.rule == "9" for e in r.errors)  # TX is not a dependency (10); reading FAILED work (9)
    r = validate(_amend(_t("FIX", "implementation", ["T5"], context_spec={"artifacts_from": ["T2"]})), existing=prior)
    assert r.ok, r.errors  # T2 is a transitive dependency through the existing edge T5 → T2
    assert r.auto_added == ["T_integrate_2"]  # a fresh sink id: recorded tasks are never reused
    assert r.dag.by_id()["T_integrate_2"].depends_on == ["FIX"]
    r = validate(_amend(_t("FIX", "implementation", ["NOPE"])), existing=prior)
    assert any(e.rule == "2" for e in r.errors)


# ----------------------------------------------------------------------------- FAIL → repair → PASS


def test_fail_repair_pass_with_one_replan(conn):
    v = StubVerifier(fail_times=1)
    planner = StubPlanner(diamond(), amendments=[FIX1])
    final = _run(conn, verifier=v, planner=planner, budgets=default_budgets(max_replans=1))
    assert final.status is RunStatus.PASSED and final.verdict == "PASS" and final.verdict_reason is None
    assert final.replans_used == 1 and v.calls == 2
    st = {t.key: t.status for t in sm.tasks_for_run(conn, final.id)}
    assert st == {k: TaskStatus.COMPLETED for k in ("T1", "T2", "T3", "T4", "T5", "FIX1", "FIX1_integ")}
    tasks = {t.key: t for t in sm.tasks_for_run(conn, final.id)}
    # the old integration's outputs stay candidates (never accepted, never touched); the new sink's are accepted
    assert [a.status for a in store.outputs_of_task(conn, tasks["T5"].id)] == [ArtifactStatus.CANDIDATE]
    assert [a.status for a in store.outputs_of_task(conn, tasks["FIX1_integ"].id)] == [ArtifactStatus.ACCEPTED]
    types = [e.type for e in _events(conn, final.id)]
    i = types.index
    assert (
        i("verify.failed")
        < i("verify.fingerprint")
        < i("run.replanning")
        < i("plan.validated", i("run.replanning"))
        < i("run.running", i("run.replanning"))
        < i("verify.passed")
    )
    fps = _fingerprints(conn, final.id)
    assert len(fps) == 1 and fps[0]["decision"] == "replan" and fps[0]["cycle"] == 0
    plans = conn.execute(
        "SELECT ref, meta FROM artifacts WHERE run_id = %s AND type = 'plan' ORDER BY created_at", (final.id,)
    ).fetchall()
    assert [p["ref"] for p in plans] == [f"plan:{final.id}:1", f"plan:{final.id}:r1"]
    assert plans[1]["meta"]["amendment"] is True and plans[1]["meta"]["amendment_hash"]
    # the planner saw the amendment protocol: existing tasks with outputs, the failure report, no earlier amendments
    req = planner.requests[-1]
    assert req.amendment and req.replan == 1 and req.previous_amendments == ()
    assert {t["key"] for t in req.existing_tasks} == {"T1", "T2", "T3", "T4", "T5"}
    assert all(t["status"] == "COMPLETED" for t in req.existing_tasks)
    assert req.failure_report and req.failure_report["status"] == "FAIL" and "scripted failure" in req.failure_report["reason"]
    assert req.remaining["replans"] == 0
    n_ver = conn.execute(
        "SELECT count(*) AS n FROM artifacts WHERE run_id = %s AND type = 'verification'", (final.id,)
    ).fetchone()["n"]
    assert n_ver == 2


# ----------------------------------------------------------------------------- no progress / budget


def test_repair_budget_exhausted_without_reduction_is_no_progress(conn):
    """Stub verifier keeps failing identically (no checks → nothing to reduce); opaque refs differ per cycle so the
    fingerprint itself does not repeat; the window closes → NO_PROGRESS."""
    planner = StubPlanner(diamond(), amendments=[FIX1])
    final = _run(conn, verifier=StubVerifier(passed=False), planner=planner, budgets=default_budgets(max_replans=1))
    assert final.status is RunStatus.FAILED and final.verdict_reason == VerdictReason.NO_PROGRESS.value
    assert "did not reduce" in final.verdict and final.replans_used == 1
    fps = _fingerprints(conn, final.id)
    assert [f["decision"] for f in fps] == ["replan", "fail"] and fps[1]["reason"] == "NO_PROGRESS"
    assert fps[0]["value"] != fps[1]["value"]  # different integration ref → not a repeat; exhaustion decided it


def test_repair_budget_exhausted_with_reduction_is_budget_exhausted(conn):
    first = VerificationResult.fail(
        "checks failed", checks=(CheckResult("a", CheckStatus.FAIL, "boom"), CheckResult("b", CheckStatus.FAIL, "bang"))
    )
    second = VerificationResult.fail(
        "checks failed", checks=(CheckResult("a", CheckStatus.FAIL, "boom"), CheckResult("b", CheckStatus.PASS))
    )
    planner = StubPlanner(diamond(), amendments=[FIX1])
    final = _run(conn, verifier=StubVerifier(script=[first, second]), planner=planner, budgets=default_budgets(max_replans=1))
    assert final.status is RunStatus.FAILED and final.verdict_reason == VerdictReason.BUDGET_EXHAUSTED.value
    assert "2 -> 1" in final.verdict and final.replans_used == 1
    fps = _fingerprints(conn, final.id)
    assert fps[0]["failing_checks"] == ["a", "b"] and fps[1]["failing_checks"] == ["a"]


def test_no_repair_budget_is_a_verdict_without_replanning(conn):
    planner = StubPlanner(diamond(), amendments=[FIX1])
    final = _run(conn, verifier=StubVerifier(passed=False), planner=planner, budgets=default_budgets(max_replans=0))
    assert final.status is RunStatus.FAILED and final.verdict_reason == VerdictReason.BUDGET_EXHAUSTED.value
    assert "no repair budget" in final.verdict and final.replans_used == 0 and planner.amendment_calls == 0
    assert "run.replanning" not in [e.type for e in _events(conn, final.id)]


def test_repeated_amendment_is_no_progress_through_the_driver(conn):
    """The planner keeps proposing the same repair (renamed or not): rejected as data, and when the plan-attempt budget
    is gone the run ends NO_PROGRESS — the planner cannot loop the run."""
    # cycle 2: the same repair under new ids (a literal re-send would collide on ids: rule 9 "may not alter", INVALID_PLAN)
    renamed = _amend(
        _t("Z1", "implementation", ["T5"], goal="repair FIX1"), _t("Z1_integ", "integration", ["Z1"], goal="repair FIX1_integ")
    )
    planner = StubPlanner(diamond(), amendments=[FIX1, renamed])  # the last amendment repeats on every later call
    final = _run(
        conn,
        verifier=StubVerifier(passed=False),
        planner=planner,
        budgets=default_budgets(max_replans=2, max_plan_attempts=2),
    )
    assert final.status is RunStatus.FAILED and final.verdict_reason == VerdictReason.NO_PROGRESS.value
    assert "repeats an earlier amendment" in final.verdict and final.replans_used == 2
    rejected = [e for e in _events(conn, final.id) if e.type == "plan.rejected"]
    assert len(rejected) == 2 and all(e.payload["kind"] == "amendment" for e in rejected)
    assert planner.amendment_calls == 3  # cycle 1 accepted, cycle 2: two rejected repeats


def test_amendment_rejections_return_to_the_planner_as_data(conn):
    """A first amendment that alters recorded work (rule 9) goes back to the planner with the errors; the next one is
    installed; the run then passes."""
    bad = _amend(_t("T2", "implementation", ["T1"]), _t("BAD_integ", "integration", ["T2"]))
    planner = StubPlanner(diamond(), amendments=[bad, FIX1])
    final = _run(conn, verifier=StubVerifier(fail_times=1), planner=planner, budgets=default_budgets(max_replans=1))
    assert final.status is RunStatus.PASSED and final.replans_used == 1
    assert planner.amendment_calls == 2
    assert any("[9] amendment may not alter existing task 'T2'" in e for e in planner.requests[-1].validation_errors)


# ----------------------------------------------------------------------------- repeated fingerprint (git: identical tree)


@pytest.mark.skipif(not git_available(), reason="git not on PATH")
def test_repeated_progress_fingerprint_stops_the_run(conn, tmp_path: Path):
    """Cycle 1's repair changes the tree; cycle 2's amendment only re-integrates the same commit → identical tree hash,
    identical failure → the fingerprint repeats → NO_PROGRESS, before max_replans (3) is spent."""
    gws = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    a1 = _amend(_t("FIX1", "implementation", ["T5"]), _t("INTEG1", "integration", ["FIX1"]))
    a2 = _amend(_t("INTEG2", "integration", ["INTEG1"], goal="re-integrate without changes"))
    planner = StubPlanner(diamond(), amendments=[a1, a2])
    final = _run(
        conn,
        verifier=StubVerifier(passed=False),
        planner=planner,
        budgets=default_budgets(max_replans=3),
        workspace=gws,
        timeout=120,
    )
    assert final.status is RunStatus.FAILED and final.verdict_reason == VerdictReason.NO_PROGRESS.value, final.verdict
    assert "same progress fingerprint as cycle 1" in final.verdict and final.replans_used == 2
    fps = _fingerprints(conn, final.id)
    assert [f["decision"] for f in fps] == ["replan", "replan", "fail"]
    assert fps[1]["integration_hash"].startswith("tree:") and fps[1]["integration_hash"] == fps[2]["integration_hash"]
    assert fps[0]["integration_hash"] != fps[1]["integration_hash"]  # cycle 1's repair did change the tree
    assert fps[2]["repeat_of_cycle"] == 1 and fps[2]["value"] == fps[1]["value"]


# ----------------------------------------------------------------------------- reason codes elsewhere


def test_verdict_reason_codes_on_other_endings(conn):
    from mas.planner.dag import DagSpec as _D

    # invalid plan at install (rule 8, unfundable) → INVALID_PLAN
    dag = _D.from_dict(
        {
            "tasks": [
                {
                    "id": f"T{i}",
                    "capability": "implementation",
                    "goal": "x",
                    "depends_on": [],
                    "output_contract": {"artifacts": ["git_commit"]},
                }
                for i in range(1, 8)
            ]
        }
    )
    with pytest.raises(runs_mod.InvalidDag):
        runs_mod.create_run_from_dag(
            conn, dag, budgets=default_budgets(max_tokens=100_000, max_attempt_tokens=50_000), capabilities=set(CAPS)
        )
    row = conn.execute("SELECT verdict_reason FROM runs ORDER BY created_at DESC LIMIT 1").fetchone()
    assert row["verdict_reason"] == "INVALID_PLAN"
    # a policy-only rejection → POLICY_DENIED
    dag = _D.from_dict(
        {
            "tasks": [
                {
                    "id": "T1",
                    "capability": "implementation",
                    "goal": "x",
                    "depends_on": [],
                    "output_contract": {"artifacts": ["git_commit"]},
                    "tools": ["network"],
                }
            ]
        }
    )
    with pytest.raises(runs_mod.InvalidDag):
        runs_mod.create_run_from_dag(conn, dag, budgets=default_budgets(), capabilities=set(CAPS))
    row = conn.execute("SELECT verdict_reason FROM runs ORDER BY created_at DESC LIMIT 1").fetchone()
    assert row["verdict_reason"] == "POLICY_DENIED"
    # budget abort → BUDGET_EXHAUSTED
    run = runs_mod.create_run_from_dag(
        conn, diamond(), budgets=Budgets(max_wallclock_s=1, max_attempt_tokens=1), capabilities=set(CAPS)
    )
    with conn.transaction():
        conn.execute("UPDATE runs SET created_at = now() - interval '5 seconds' WHERE id = %s", (run.id,))
    r = scheduler.tick(conn, run.id, verifier=StubVerifier(True))
    assert r.status is RunStatus.ABORTED and r.verdict_reason == "BUDGET_EXHAUSTED"
