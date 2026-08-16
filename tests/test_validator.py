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
