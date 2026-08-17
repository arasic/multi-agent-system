from mas.evaluation import (
    CONFIGS,
    SingleAgentRepairPlanner,
    dag_for_config,
    effective_concurrency,
    normalize_config,
    single_agent_dag,
)
from mas.planner.dag import DagSpec
from mas.planner.planner import PlanRequest


def test_frozen_config_policy_is_executable():
    assert CONFIGS == ("A", "B", "C", "D")
    assert normalize_config("b") == "B"
    assert [effective_concurrency(c, 8) for c in CONFIGS] == [1, 1, 1, 8]


def test_single_agent_configs_are_one_solve_plus_system_integration():
    dag = single_agent_dag("implement all adapters", "adapters_4")
    assert [(t.id, t.capability, t.depends_on) for t in dag.tasks] == [
        ("SOLVE", "solve", []),
        ("INTEGRATE", "integration", ["SOLVE"]),
    ]
    original = DagSpec.from_dict(
        {
            "goal": "wide",
            "benchmark": "adapters_4",
            "tasks": [
                {"id": "X", "capability": "implementation", "goal": "x", "output_contract": {"artifacts": ["git_commit"]}},
                {
                    "id": "I",
                    "capability": "integration",
                    "goal": "i",
                    "depends_on": ["X"],
                    "output_contract": {"artifacts": ["git_commit"]},
                },
            ],
        }
    )
    assert len(dag_for_config(original, "A").tasks) == 2
    assert dag_for_config(original, "C") is original and dag_for_config(original, "D") is original


def test_single_agent_transform_preserves_only_stub_fixture_files():
    original = DagSpec.from_dict(
        {
            "goal": "fixture",
            "benchmark": "adapters_1",
            "tasks": [
                {
                    "id": "X",
                    "capability": "implementation",
                    "goal": "x",
                    "output_contract": {"artifacts": ["git_commit"]},
                    "meta": {"stub": {"files": {"adapters/a01.py": "def transform(x): return x"}}},
                },
                {
                    "id": "I",
                    "capability": "integration",
                    "goal": "i",
                    "depends_on": ["X"],
                    "output_contract": {"artifacts": ["git_commit"]},
                    "meta": {"stub": {"files": {"test_adapters.py": "def test_ok(): pass"}}},
                },
            ],
        }
    )
    solve = dag_for_config(original, "A").by_id()["SOLVE"]
    assert set(solve.meta["stub"]["files"]) == {"adapters/a01.py", "test_adapters.py"}


def test_single_agent_repair_stays_one_solve_plus_integration():
    req = PlanRequest(
        goal="fix it",
        capabilities=frozenset({"solve", "integration"}),
        benchmark="adapters_1",
        amendment=True,
        replan=1,
        existing_tasks=({"key": "INTEGRATE", "capability": "integration", "status": "COMPLETED"},),
        failure_report={"failing": ["adapter_behavior"]},
    )
    dag = SingleAgentRepairPlanner().plan(req)
    assert [(t.id, t.capability) for t in dag.tasks] == [("SOLVE_R1", "solve"), ("INTEGRATE_R1", "integration")]
    assert dag.tasks[0].depends_on == ["INTEGRATE"] and "adapter_behavior" in dag.tasks[0].goal
