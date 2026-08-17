"""Step 11 — the provider-backed planner and the deterministic driver around it (fake providers, DB, no key).

The planner proposes exactly one typed outcome per call; `runs.plan_run` validates, records, and decides — questions →
AWAITING_INPUT, contract → validated + AWAITING_INPUT for the human's approval, DAG → validated + installed. Rejections go
back to the planner as data within `max_plan_attempts`; the planner cannot freeze contracts, change budgets or run
state; its calls are metered (role=planner), budgeted and deadline-bound. Task-shape metadata is advisory."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from mas import metrics
from mas.models.enums import RunStatus
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.orchestrator import state_machine as sm
from mas.planner import contracts as contract_mod
from mas.planner.dag import ContractProposal, DagSpec, Questions, parse_plan
from mas.planner.llm import LLMPlanner, PlannerLimits, PlannerOutputError, parse_output
from mas.planner.planner import PlanRequest
from mas.planner.validator import validate_shape
from mas.providers import from_spec
from mas.providers.base import ProviderUnavailable
from mas.providers.fake import FakeProvider
from mas.providers.telemetry import MemorySink
from mas.verifier.stub import StubVerifier
from mas.workers.llm import LLMAgent
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.workspace import GitWorkspace
from tests.conftest import CAPS, DB_URL, default_budgets

pytestmark = pytest.mark.db
ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads((ROOT / "acceptance" / "url_shortener_contract" / "contract.json").read_text(encoding="utf-8"))
DAG = json.loads((ROOT / "benchmarks" / "url_shortener" / "dag.json").read_text(encoding="utf-8"))


def _contract_json(**over):
    d = {
        "kind": "contract",
        "requirements": ["POST /shorten returns 201", "GET /<code> redirects", "stats available", "survives restart"],
        "assumptions": ["sqlite storage"],
        "exclusions": ["auth"],
        "quality": {"tests_required": True},
        "contract": CONTRACT,
    }
    d.update(over)
    return json.dumps(d)


def _dag_json(**over):
    d = {
        "kind": "dag",
        "assumptions": ["python 3.12"],
        "shape": {"estimated_width": 3, "suggested_mode": "parallel_centralized_mas"},
        "tasks": DAG["tasks"],
    }
    d.update(over)
    return json.dumps(d)


def _planner(script, **kw) -> tuple[LLMPlanner, MemorySink, FakeProvider]:
    inner = FakeProvider(script, input_tokens=400, output_tokens=200, model="fake-planner")
    sink = MemorySink()
    return LLMPlanner(inner, sink_factory=lambda run_id: sink, limits=kw.pop("limits", None)), sink, inner


def _adhoc(conn, goal="Build a small URL-shortener HTTP service with persistent storage", **budgets):
    return runs_mod.create_run(conn, goal=goal, budgets=default_budgets(**budgets))


# ----------------------------------------------------------------------------- parsing (deterministic)


def test_parse_output_is_exactly_one_typed_outcome():
    assert isinstance(parse_output('{"kind": "questions", "questions": ["Which DB?"], "context": "storage"}'), Questions)
    assert isinstance(parse_output(_contract_json()), ContractProposal)
    dag = parse_output("```json\n" + _dag_json() + "\n```")
    assert isinstance(dag, DagSpec) and dag.shape["estimated_width"] == 3 and dag.assumptions == ["python 3.12"]
    assert isinstance(parse_output("Sure! " + _dag_json() + " Let me know."), DagSpec)  # one tolerated prose wrap
    for bad in [
        "",
        "not json",
        "[1,2]",
        '{"kind": "poem"}',
        '{"kind": "dag", "tasks": []}',
        '{"kind": "questions", "questions": [" "]}',
        '{"foo": 1}',
    ]:
        with pytest.raises((ValueError, TypeError)):
            parse_output(bad)
    with pytest.raises(ValueError):
        parse_output(_dag_json(shape={"suggested_mode": "swarm"}))
    with pytest.raises(ValueError):
        parse_plan({"kind": "contract", "tasks": [], "questions": []}) and parse_plan({"kind": "nope"})


def test_shape_metadata_is_validated_but_advisory():
    assert validate_shape(None) == [] and validate_shape({}) == []
    ok = {
        "estimated_width": 4,
        "dependency_density": 0.3,
        "critical_path_ratio": 0.5,
        "overlapping_outputs": ["src/app.py"],
        "coupling_risk": "low",
        "integration_risk": "medium",
        "suggested_mode": "single_agent",
        "rationale": "x",
    }
    assert validate_shape(ok) == []
    errs = validate_shape(
        {"estimated_width": 0, "dependency_density": 2, "coupling_risk": "huge", "suggested_mode": "swarm", "extra": 1}
    )
    assert len(errs) == 5 and all(e.rule == "shape" for e in errs)


# ----------------------------------------------------------------------------- planner: typed outcomes, retries as data


def test_planner_returns_one_outcome_and_retries_malformed_output_as_data():
    planner, sink, inner = _planner(
        ["I think we should...", "still no json", _contract_json()], limits=PlannerLimits(max_parse_retries=2)
    )
    req = PlanRequest(goal="g", capabilities=frozenset(CAPS), needs_contract=True, run_id=None)
    out = planner.plan(req)
    assert isinstance(out, ContractProposal) and len(inner.calls) == 3
    assert planner.rounds[-1]["calls"] == 3 and len(planner.rounds[-1]["parse_errors"]) == 2
    # the parse errors went back to the model as data
    assert "not a valid plan object" in inner.calls[2]["messages"][-1]["content"]
    assert [r.role for r in sink.records] == ["planner"] * 3 and all(r.status == "ok" for r in sink.records)
    # over the parse-retry budget → typed error (the driver turns it into a run verdict)
    planner, _, _ = _planner(["nope", "nope", "nope"], limits=PlannerLimits(max_parse_retries=1))
    with pytest.raises(PlannerOutputError, match="malformed"):
        planner.plan(req)
    planner, _, _ = _planner([{"text": "", "stop_reason": "refusal"}])
    with pytest.raises(PlannerOutputError, match="refused"):
        planner.plan(req)


def test_planner_calls_are_budgeted_and_deadline_bound():
    planner, sink, inner = _planner([ProviderUnavailable("down")])
    with pytest.raises(ProviderUnavailable):
        planner.plan(PlanRequest(goal="g", capabilities=frozenset(CAPS), run_id=None))
    assert sink.records[0].status == "error" and not sink.records[0].priced
    # remaining wall-clock too small → refuses before any call
    planner, sink, inner = _planner([_dag_json()])
    with pytest.raises(PlannerOutputError, match="wall-clock"):
        planner.plan(PlanRequest(goal="g", capabilities=frozenset(CAPS), deadline_s=5.0))
    assert inner.calls == []
    # the token budget of the round is capped by the run's remaining tokens
    planner, sink, inner = _planner(["nope", "nope", "nope"], limits=PlannerLimits(max_parse_retries=5, max_calls_per_round=10))
    with pytest.raises(Exception) as ei:  # AttemptBudgetExceeded (600 tokens/call, 1000 left → 2 calls)
        planner.plan(PlanRequest(goal="g", capabilities=frozenset(CAPS), remaining={"tokens": 1000}))
    assert "budget" in str(ei.value) and len(inner.calls) == 2
    # the brief carries what the planner needs to know
    planner, _, inner = _planner([_dag_json()])
    planner.plan(
        PlanRequest(
            goal="G",
            capabilities=frozenset(["implementation"]),
            tool_registry={"implementation": ("filesystem", "shell")},
            benchmark="url_shortener",
            validation_errors=("[1] cycle",),
            plan_attempt=2,
        )
    )
    brief = inner.calls[0]["messages"][1]["content"]
    assert "G" in brief and "implementation: filesystem, shell" in brief and "REJECTED" in brief and "[1] cycle" in brief
    assert "url_shortener" in brief and inner.calls[0]["messages"][0]["role"] == "system"


# ----------------------------------------------------------------------------- driver: the contract gate


def test_driver_requires_a_contract_before_a_dag_for_adhoc_goals(conn):
    run = _adhoc(conn, max_plan_attempts=3)
    planner, sink, inner = _planner([_dag_json(), _dag_json(), _contract_json()])
    r = runs_mod.plan_run(conn, run.id, planner, capabilities=set(CAPS))
    # the DAG was rejected twice with the reason as data, the contract accepted on the third try
    assert r.status is RunStatus.AWAITING_INPUT
    assert "no acceptance contract yet" in inner.calls[1]["messages"][1]["content"]
    assert planner.rounds[1]["plan_attempt"] == 2 and inner.calls[1]["messages"][1]["content"].count("REJECTED") == 1
    prop = contract_mod.pending_proposal(conn, run.id)
    assert prop and prop["proposal"]["requirements"][0].startswith("POST /shorten") and prop["planner"] == "llm"
    evs = [e.type for e in __import__("mas.db.events", fromlist=["for_run"]).for_run(conn, run.id)]
    assert evs.count("plan.rejected") == 2 and "contract.proposed" in evs
    assert not (ROOT / "acceptance" / contract_mod.benchmark_id_for(run.id)).exists()  # proposing writes nothing


def test_driver_fails_the_run_after_max_plan_attempts_with_a_verdict(conn):
    run = _adhoc(conn, max_plan_attempts=2)
    planner, _, inner = _planner([_dag_json()])  # keeps returning a DAG before any contract exists
    r = runs_mod.plan_run(conn, run.id, planner, capabilities=set(CAPS))
    assert r.status is RunStatus.FAILED and "max_plan_attempts (2)" in (r.verdict or "") and len(inner.calls) == 2


def test_driver_rejects_unmappable_contracts_as_data(conn):
    run = _adhoc(conn, max_plan_attempts=3)
    bad = _contract_json(
        contract={"protocol_version": 1, "checks": [{"id": "x", "type": "llm_judgement", "prompt": "is it good?"}]}
    )
    planner, _, inner = _planner([bad, _contract_json()])
    r = runs_mod.plan_run(conn, run.id, planner, capabilities=set(CAPS))
    assert r.status is RunStatus.AWAITING_INPUT
    assert "unmappable acceptance criteria" in inner.calls[1]["messages"][1]["content"]


def test_approve_freezes_the_contract_and_the_verifier_request_pins_it(conn, tmp_path):
    run = _adhoc(conn)
    planner, _, _ = _planner([_contract_json()])
    assert runs_mod.plan_run(conn, run.id, planner, capabilities=set(CAPS)).status is RunStatus.AWAITING_INPUT
    frozen = contract_mod.approve(conn, run.id, acceptance_root=tmp_path / "acceptance", approved_by="tester")
    assert frozen.benchmark == contract_mod.benchmark_id_for(run.id)
    suite = frozen.suite_dir
    manifest = json.loads((suite / "suite.json").read_text())
    assert manifest["command"] == ["python", "/opt/mas/adapters/runner.py", "/acceptance/contract.json"]
    assert manifest["expected_checks"] == [c["id"] for c in CONTRACT["checks"]] and manifest["protocol_version"] == 1
    assert json.loads((suite / "contract.json").read_text())["checks"][0]["id"] == "compiles"
    r = sm.get_run(conn, run.id)
    assert r.status is RunStatus.PLANNING and r.benchmark == frozen.benchmark
    art = contract_mod.approved(conn, run.id)
    assert art["sha256"] == frozen.sha256 and art["suite_sha256"] == frozen.suite_sha256 and art["approved_by"] == "tester"
    assert art["edited"] is False and art["check_ids"][-1] == "survives_restart"
    # the verification request pins the frozen suite digest (ADR-007)
    req = scheduler._verification_request(conn, r, None)
    assert req.benchmark == frozen.benchmark and req.expected_suite_sha256 == frozen.suite_sha256
    # a second approval is refused; the contract is immutable
    with pytest.raises(contract_mod.InvalidProposal):
        contract_mod.approve(conn, run.id, acceptance_root=tmp_path / "acceptance")
    # the real verifier accepts the frozen suite as a valid contract suite (host-side load, no Docker needed)
    from mas.verifier.acceptance import AcceptanceVerifier

    v = AcceptanceVerifier(tmp_path / "acceptance", image="unused")
    assert v.suite_digest(frozen.benchmark) == frozen.suite_sha256


def test_approve_with_an_edited_contract_and_rejects_bad_edits(conn, tmp_path):
    run = _adhoc(conn)
    planner, _, _ = _planner([_contract_json()])
    runs_mod.plan_run(conn, run.id, planner, capabilities=set(CAPS))
    with pytest.raises(contract_mod.InvalidProposal, match="unmappable"):
        contract_mod.approve(
            conn,
            run.id,
            acceptance_root=tmp_path / "a",
            contract_doc={"protocol_version": 1, "checks": [{"id": "x", "type": "vibes"}]},
        )
    edited = json.loads(json.dumps(CONTRACT))
    edited["checks"] = edited["checks"][:2]  # the human narrows the contract to build + tests
    edited.pop("service")
    frozen = contract_mod.approve(conn, run.id, acceptance_root=tmp_path / "a", contract_doc=edited)
    assert contract_mod.approved(conn, run.id)["edited"] is True and frozen.suite_sha256
    assert json.loads((frozen.suite_dir / "suite.json").read_text())["expected_checks"] == ["compiles", "tests_pass"]


# ----------------------------------------------------------------------------- the whole gate, offline


def test_gate_goal_to_verdict_with_questions_contract_approval_dag_workers(conn, tmp_path):
    """goal → planner asks → answer → contract → approve → DAG (validated) → workers (fake:builder) → integration →
    verifier → verdict. Every planner call metered as role=planner; the plan + shape on record; verdict PASS."""
    run = _adhoc(conn, max_questions=2)
    script = [
        json.dumps({"kind": "questions", "questions": ["Which storage engine?"], "context": "persistence"}),
        _contract_json(assumptions=["sqlite as answered"]),
        _dag_json(),
    ]
    planner, sink, inner = _planner(script)
    caps = set(CAPS)
    # 1. asks
    assert runs_mod.plan_run(conn, run.id, planner, capabilities=caps).status is RunStatus.AWAITING_INPUT
    assert runs_mod.pending_questions(conn, run.id) == ["Which storage engine?"]
    runs_mod.answer(conn, run.id, "sqlite", by="tester")
    # 2. proposes the contract with the answer in hand
    assert runs_mod.plan_run(conn, run.id, planner, capabilities=caps).status is RunStatus.AWAITING_INPUT
    assert "A: sqlite" in inner.calls[1]["messages"][1]["content"]
    assert contract_mod.pending_proposal(conn, run.id) is not None
    # 3. approved → frozen; 4. planner produces the DAG, driver validates and installs it
    contract_mod.approve(conn, run.id, acceptance_root=tmp_path / "acceptance")
    r = runs_mod.plan_run(conn, run.id, planner, capabilities=caps)
    assert r.status is RunStatus.RUNNING
    assert "Frozen acceptance contract" in inner.calls[2]["messages"][1]["content"]
    tasks = sm.tasks_for_run(conn, run.id)
    assert {t.key for t in tasks} == {"T1", "T2", "T3", "T4", "T5", "T6"}
    plan = conn.execute("SELECT meta FROM artifacts WHERE run_id = %s AND type = 'plan'", (run.id,)).fetchone()["meta"]
    assert plan["shape"]["suggested_mode"] == "parallel_centralized_mas" and plan["dag"]["tasks"][0]["id"] == "T1"
    # the suggested mode did NOT change how the run executes: max_concurrency is the configured budget
    assert sm.get_run(conn, run.id).budgets.max_concurrency == default_budgets().max_concurrency
    # 5. workers build it (fake:builder), integration, verifier (stub here; the frozen suite is real and loadable)
    gws = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    provider = from_spec("fake:builder")
    stop = threading.Event()
    ws = [
        Worker(f"w{i}", list(CAPS), LLMAgent(), database_url=DB_URL, poll_s=0.05, run_id=run.id, workspace=gws, provider=provider)
        for i in range(3)
    ]
    threads = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=StubVerifier(True), tick_s=0.1, timeout_s=120, workspace=gws)
    finally:
        stop.set()
        wait_all(threads, 10)
    assert final.status is RunStatus.PASSED, final.verdict
    m = metrics.compute(conn, run.id)
    planner_rows = [r for r in sink.records if r.role == "planner"]
    assert len(planner_rows) == 3 and m.questions == 1
    db_planner = conn.execute(
        "SELECT count(*) AS n FROM model_calls WHERE run_id = %s AND role = 'planner'", (run.id,)
    ).fetchone()["n"]
    assert db_planner == 0  # MemorySink here; the CLI wires a DbSink — the worker rows are in the DB
    assert m.model_calls >= 6 and m.per_model  # worker calls recorded
    evs = [e.type for e in __import__("mas.db.events", fromlist=["for_run"]).for_run(conn, run.id)]
    for must in (
        "plan.questions",
        "plan.answered",
        "contract.proposed",
        "contract.approved",
        "plan.validated",
        "plan.assumptions",
        "run.passed",
    ):
        assert must in evs, must


def test_fake_planner_double_and_orchestrator_tick_drive_the_gate(conn, tmp_path):
    """The offline demo double `fake:planner` through the orchestrator's own tick (the service path)."""
    from mas.providers.telemetry import DbSink

    run = _adhoc(conn)
    planner = LLMPlanner(from_spec("fake:planner"), sink_factory=lambda rid: DbSink(conn))
    r = scheduler.tick(conn, run.id, planner=planner, capabilities=set(CAPS), verifier=StubVerifier(True))
    assert r.status is RunStatus.AWAITING_INPUT and contract_mod.pending_proposal(conn, run.id)
    contract_mod.approve(conn, run.id, acceptance_root=tmp_path / "acceptance")
    r = scheduler.tick(conn, run.id, planner=planner, capabilities=set(CAPS), verifier=StubVerifier(True))
    assert r.status is RunStatus.RUNNING and len(sm.tasks_for_run(conn, run.id)) == 6
    rows = conn.execute("SELECT role, status FROM model_calls WHERE run_id = %s ORDER BY id", (run.id,)).fetchall()
    assert [r["role"] for r in rows] == ["planner", "planner"] and all(r["status"] == "ok" for r in rows)
    m = metrics.compute(conn, run.id)
    assert m.per_model and any(k.endswith("/planner") for k in m.per_model)


# ----------------------------------------------------------------------------- rule 8 through the driver


def _wide_dag_json(n: int, **over):
    tasks = [
        {
            "id": f"T{i}",
            "capability": "implementation",
            "goal": f"part {i}",
            "depends_on": [],
            "output_contract": {"artifacts": ["git_commit"]},
        }
        for i in range(1, n + 1)
    ]
    return _dag_json(tasks=tasks, **over)


def test_rule8_rejections_return_to_the_planner_as_data_then_a_fitting_plan_installs(conn):
    """The run can fund 4 attempts (400k / 100k); a 6-task plan (+ auto integration = 7) is rejected with the allocation
    arithmetic as data, the planner shrinks it, the 3-task plan (+ integration = 4) installs."""
    b = default_budgets(max_tokens=400_000, max_attempt_tokens=100_000)
    run = runs_mod.create_run(conn, goal="benchmark run", benchmark="url_shortener", budgets=b)
    planner, _, inner = _planner([_wide_dag_json(6), _wide_dag_json(3)])
    r = runs_mod.plan_run(conn, run.id, planner, capabilities=set(CAPS))
    assert r.status is RunStatus.RUNNING, r.verdict
    retry_brief = inner.calls[1]["messages"][1]["content"]
    want = "[8] 7 open tasks x max_attempt_tokens 100000 = 700000 tokens needed, 400000 remain: at most 4 new tasks fit"
    assert want in retry_brief
    assert '"max_attempt_tokens": 100000' in retry_brief  # the allocation unit is in the planner's budget brief
    evs = [e for e in __import__("mas.db.events", fromlist=["for_run"]).for_run(conn, run.id) if e.type == "plan.rejected"]
    assert len(evs) == 1 and evs[0].payload["errors"][0].startswith("[8]")
    assert len(sm.tasks_for_run(conn, run.id)) == 4


def test_rule8_estimates_above_the_allocation_and_exhausted_budgets_end_in_a_verdict(conn):
    run = runs_mod.create_run(
        conn,
        goal="benchmark run",
        benchmark="url_shortener",
        budgets=default_budgets(max_tokens=400_000, max_attempt_tokens=100_000, max_plan_attempts=2),
    )
    tasks = json.loads(_wide_dag_json(2))["tasks"]
    tasks[0]["estimate"] = {"tokens": 150_000, "seconds": 5}  # more than one attempt can ever get
    planner, _, inner = _planner([_dag_json(tasks=tasks), _dag_json(tasks=tasks)])
    r = runs_mod.plan_run(conn, run.id, planner, capabilities=set(CAPS))
    assert r.status is RunStatus.FAILED and "[8]" in (r.verdict or "") and "cannot finish within one attempt" in r.verdict
    assert "estimate.tokens 150000 exceeds the per-attempt allocation" in inner.calls[1]["messages"][1]["content"]
    # a run whose token budget is already spent cannot install any plan: verdict, no tasks. (With the LLM planner the
    # metered planner call itself is refused first - also a verdict; the stub planner shows rule 8 doing it.)
    from mas.planner.planner import StubPlanner

    spent = default_budgets(max_tokens=1000, max_plan_attempts=1)
    run2 = runs_mod.create_run(conn, goal="x", benchmark="url_shortener", budgets=spent)
    with conn.transaction():
        conn.execute("UPDATE runs SET tokens_used = 1000 WHERE id = %s", (run2.id,))
    r2 = runs_mod.plan_run(conn, run2.id, StubPlanner(DagSpec.from_dict(json.loads(_wide_dag_json(1)))), capabilities=set(CAPS))
    assert r2.status is RunStatus.FAILED and "0 remain" in (r2.verdict or "") and sm.tasks_for_run(conn, run2.id) == []
    planner3, _, _ = _planner([_wide_dag_json(1)])
    run3 = runs_mod.create_run(conn, goal="x", benchmark="url_shortener", budgets=spent)
    with conn.transaction():
        conn.execute("UPDATE runs SET tokens_used = 1000 WHERE id = %s", (run3.id,))
    r3 = runs_mod.plan_run(conn, run3.id, planner3, capabilities=set(CAPS))
    assert r3.status is RunStatus.FAILED and "AttemptBudgetExceeded" in (r3.verdict or "")


def test_rule8_file_dags_are_checked_against_the_run_at_install(conn):
    """`mas run --dag` goes through install_dag -> the same allocation check; an unfundable plan is a FAILED verdict."""
    dag = DagSpec.from_dict(json.loads(_wide_dag_json(6)))
    with pytest.raises(runs_mod.InvalidDag) as ei:
        runs_mod.create_run_from_dag(
            conn, dag, budgets=default_budgets(max_tokens=400_000, max_attempt_tokens=100_000), capabilities=set(CAPS)
        )
    assert "[8]" in str(ei.value)
    row = conn.execute("SELECT status, verdict FROM runs ORDER BY created_at DESC LIMIT 1").fetchone()
    assert row["status"] == "FAILED" and "[8]" in row["verdict"]
