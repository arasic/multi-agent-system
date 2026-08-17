"""Deterministic DAG validator (no DB). Rules per docs/architecture.md §8 — A2."""

from mas.models.types import Budgets
from mas.planner.dag import DagSpec
from mas.planner.validator import AUTO_INTEGRATION_ID, validate
from tests.conftest import diamond


def rules(result) -> set[str]:
    return {e.rule for e in result.errors}


def test_valid_diamond_ok():
    r = validate(diamond())
    assert r.ok, r.errors
    assert r.auto_added == []


def test_auto_appends_integration_sink_when_missing():
    r = validate(diamond(integration=False))
    assert r.ok, r.errors
    assert r.auto_added == [AUTO_INTEGRATION_ID]
    it = r.dag.by_id()[AUTO_INTEGRATION_ID]
    assert it.capability == "integration"
    assert set(it.depends_on) == {"T2", "T3", "T4"}


def test_missing_integration_rejected_when_auto_disabled():
    r = validate(diamond(integration=False), auto_integration=False)
    assert "6" in rules(r)


def test_cycle_rejected():
    d = diamond()
    d.by_id()["T1"].depends_on.append("T5")  # T5 → T1 → T5
    r = validate(d)
    assert "1" in rules(r)


def test_missing_dependency_rejected():
    d = diamond()
    d.by_id()["T2"].depends_on.append("T99")
    assert "2" in rules(validate(d))


def test_duplicate_ids_rejected():
    d = diamond()
    d.tasks.append(d.tasks[1])
    assert "duplicate" in rules(validate(d))


def test_missing_output_contract_rejected():
    d = diamond()
    d.by_id()["T3"].output_contract = {}
    assert "5" in rules(validate(d))


def test_unknown_capability_rejected_when_registry_given():
    d = diamond()
    d.by_id()["T3"].capability = "quantum"
    assert "3" in rules(validate(d, capabilities={"architecture", "implementation", "integration"}))
    assert validate(d).ok  # without a registry, rule 3 is not applied


def test_more_than_one_integration_rejected():
    d = diamond()
    d.by_id()["T4"].capability = "integration"
    assert "6" in rules(validate(d))


def test_integration_must_be_sink():
    d = diamond()
    d.tasks.append(
        DagSpec.from_dict(
            {
                "tasks": [
                    {
                        "id": "T6",
                        "capability": "testing",
                        "goal": "",
                        "depends_on": ["T5"],
                        "output_contract": {"artifacts": ["git_commit"]},
                    }
                ]
            }
        ).tasks[0]
    )
    assert "6" in rules(validate(d))


def test_task_not_reaching_integration_rejected():
    d = diamond()
    d.tasks.append(
        DagSpec.from_dict(
            {
                "tasks": [
                    {
                        "id": "T9",
                        "capability": "testing",
                        "goal": "orphan",
                        "depends_on": [],
                        "output_contract": {"artifacts": ["git_commit"]},
                    }
                ]
            }
        ).tasks[0]
    )
    r = validate(d)
    assert any(e.rule == "6" and e.task_id == "T9" for e in r.errors)


def test_max_tasks_budget():
    assert "7" in rules(validate(diamond(), budgets=Budgets(max_tasks=3)))
    assert validate(diamond(), budgets=Budgets(max_tasks=5)).ok
    assert "7" in rules(validate(diamond(), budgets=Budgets(max_tasks=5), existing_task_count=1))


def test_empty_dag_rejected():
    assert "empty" in rules(validate(DagSpec()))


def test_task_max_attempts_cannot_exceed_run_retry_budget():
    """P1: a per-task max_attempts override must stay within [1, max_attempts_per_task]."""
    d = diamond()
    d.by_id()["T3"].max_attempts = 99
    r = validate(d, budgets=Budgets(max_attempts_per_task=3))
    assert any(e.rule == "7" and e.task_id == "T3" and "max_attempts 99" in e.message for e in r.errors)
    d.by_id()["T3"].max_attempts = 0
    assert any(e.rule == "7" and e.task_id == "T3" for e in validate(d, budgets=Budgets(max_attempts_per_task=3)).errors)
    d.by_id()["T3"].max_attempts = 3
    assert validate(d, budgets=Budgets(max_attempts_per_task=3)).ok
    d.by_id()["T3"].max_attempts = None
    assert validate(d, budgets=Budgets(max_attempts_per_task=1)).ok  # no override → inherits the run budget


def test_rule4_tools_default_filled_and_bounded_by_capability():
    """Rule 4: requested tools ⊆ allowed(capability); unknown/forbidden rejected; default filled when omitted."""
    from mas.planner.capabilities import DEFAULT_CAPABILITY_TOOLS

    r = validate(diamond())
    assert r.ok
    for t in r.dag.tasks:  # defaults filled from the registry
        assert t.tools == sorted(DEFAULT_CAPABILITY_TOOLS[t.capability])
    d = diamond()
    d.by_id()["T1"].tools = ["filesystem", "git"]  # architecture may not commit
    r = validate(d)
    assert any(e.rule == "4" and e.task_id == "T1" and "'git' not allowed" in e.message for e in r.errors)
    d = diamond()
    d.by_id()["T2"].tools = ["filesystem", "laser"]
    assert any(e.rule == "4" and "'laser' is not available" in e.message for e in validate(d).errors)
    d = diamond()
    d.by_id()["T2"].tools = ["network"]
    assert any(e.rule == "4" and "prohibited by policy" in e.message for e in validate(d).errors)
    d = diamond()
    d.by_id()["T2"].tools = ["filesystem", "python"]  # a subset is fine
    assert validate(d).ok and validate(d).dag.by_id()["T2"].tools == ["filesystem", "python"]


def test_synthesized_integration_task_capability_is_validated():
    """Gap: the auto-appended integration sink must also have a registered worker, else the run would stall on a
    READY task nobody can claim."""
    r = validate(diamond(integration=False), capabilities={"architecture", "implementation"})
    assert r.auto_added == ["T_integrate"]
    assert any(e.rule == "3" and e.task_id == "T_integrate" for e in r.errors)
    assert validate(diamond(integration=False), capabilities={"architecture", "implementation", "integration"}).ok


# ----------------------------------------------------------------------------- rule 8: deterministic budget allocation


def test_rule8_tokens_one_attempt_per_open_task_at_the_run_allocation():
    from mas.planner.validator import Remaining

    d = diamond()  # 5 tasks
    b = Budgets(max_attempt_tokens=100_000)
    assert validate(d, budgets=b, remaining=Remaining(tokens=500_000)).ok
    r = validate(d, budgets=b, remaining=Remaining(tokens=499_999))
    assert rules(r) == {"8"} and "at most 4 new tasks fit" in str(r.errors[0])
    # existing open tasks (re-plan) still need funding; the auto-appended integration sink counts too
    assert "8" in rules(validate(d, budgets=b, remaining=Remaining(tokens=500_000, open_tasks=1)))
    r = validate(diamond(integration=False), budgets=b, remaining=Remaining(tokens=400_000))
    assert "8" in rules(r) and r.auto_added == [AUTO_INTEGRATION_ID]
    # no `remaining` (offline validation of a DAG file) → not checked
    assert validate(d, budgets=b).ok


def test_rule8_time_uses_observed_attempts_and_estimates_which_only_tighten():
    from mas.planner.validator import Remaining, critical_path_s

    d = diamond()  # chain T1 -> {T2,T3,T4} -> T5: critical path 3 tasks, 5 tasks total
    b = Budgets(max_attempt_tokens=1, max_concurrency=4)
    # nothing known yet: only "some wall-clock left" is required
    assert validate(d, budgets=b, remaining=Remaining(wallclock_s=1.0)).ok
    assert "no wall-clock left" in str(validate(d, budgets=b, remaining=Remaining(wallclock_s=0)).errors[0])
    # this run's observed mean attempt duration floors every task: 3-task critical path x 10s = 30s
    assert validate(d, budgets=b, remaining=Remaining(wallclock_s=30, observed_attempt_s=10)).ok
    r = validate(d, budgets=b, remaining=Remaining(wallclock_s=29, observed_attempt_s=10))
    assert rules(r) == {"8"} and "critical path ['T1', 'T2', 'T5']" in str(r.errors[0]) and "observed mean" in str(r.errors[0])
    # throughput bound: 5 tasks x 10s / concurrency 1 = 50s > 45s (the chain alone, 30s, would fit)
    r = validate(
        d, budgets=Budgets(max_attempt_tokens=1, max_concurrency=1), remaining=Remaining(wallclock_s=45, observed_attempt_s=10)
    )
    assert rules(r) == {"8"} and "over max_concurrency 1" in str(r.errors[0])
    # planner estimates can only make it stricter: an optimistic 1s estimate does not beat the observed 10s
    d.by_id()["T2"].estimate = {"seconds": 1}
    assert "8" in rules(validate(d, budgets=b, remaining=Remaining(wallclock_s=29, observed_attempt_s=10)))
    # ...a pessimistic one bites even with no history
    d.by_id()["T2"].estimate = {"seconds": 30}
    r = validate(d, budgets=b, remaining=Remaining(wallclock_s=29))
    assert "8" in rules(r) and "planner estimates" in str(r.errors[0])
    assert critical_path_s(d.tasks, {"T2": 30.0}) == (30.0, ["T1", "T2", "T5"])


def test_rule8_estimates_are_validated_and_must_fit_one_attempt():
    from mas.planner.validator import Remaining

    b = Budgets(max_attempt_tokens=50_000, max_attempt_runtime_s=100)
    d = diamond()
    d.by_id()["T1"].estimate = {"tokens": 60_000}
    r = validate(d, budgets=b, remaining=Remaining(tokens=10_000_000, wallclock_s=1e6))
    assert [e.task_id for e in r.errors] == ["T1"] and "cannot finish within one attempt" in str(r.errors[0])
    d.by_id()["T1"].estimate = {"seconds": 101}
    assert "max_attempt_runtime_s=100" in str(validate(d, budgets=b).errors[0])
    for bad in ({"tokens": -1}, {"tokens": 1.5}, {"seconds": float("nan")}, {"minutes": 3}, {"tokens": True}, "cheap"):
        d.by_id()["T1"].estimate = bad if isinstance(bad, dict) else {"_invalid": bad}
        assert "8" in rules(validate(d, budgets=b)), bad
    d.by_id()["T1"].estimate = {"tokens": 50_000, "seconds": 100}
    assert validate(d, budgets=b, remaining=Remaining(tokens=250_000, wallclock_s=100)).ok
    # cost: enforced at run time; here only "already exhausted" is a plan-time rejection
    assert "cost budget exhausted" in str(validate(d, budgets=b, remaining=Remaining(cost_usd=0.0)).errors[0])
    assert validate(d, budgets=b, remaining=Remaining(cost_usd=0.01)).ok
