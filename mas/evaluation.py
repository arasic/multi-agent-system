"""Frozen MVP execution configurations (docs/evaluation.md §3).

The config is executable policy, not a label:
  A/B = one `solve` task plus the ordinary integration/verifier stage, concurrency 1;
  C   = the validated planner DAG through the same worker path, concurrency 1;
  D   = that same path with the requested parallelism.

Model choice remains explicit at the benchmark boundary: A receives the cheap worker model, B the strong worker
model, and C/D receive the same planner and worker models. The runtime never hard-codes vendor/model names.
"""

from __future__ import annotations

import json

from mas.planner.dag import DagSpec
from mas.planner.planner import PlanRequest

CONFIGS = ("A", "B", "C", "D")
WIDTHS = (1, 2, 4, 8, 16)


def adapter_specs(n: int) -> list[dict[str, int | str]]:
    if n not in WIDTHS:
        raise ValueError(f"unsupported adapter width {n}; expected one of {WIDTHS}")
    return [{"id": f"a{i:02d}", "factor": i + 1, "offset": i * 7 - 3} for i in range(1, n + 1)]


def width_goal(n: int) -> str:
    specs = adapter_specs(n)
    rules = ", ".join(f"{r['id']}: transform(x) = x*{r['factor']} + {r['offset']}" for r in specs)
    return (
        f"Implement {n} independent Python adapters. Each adapter lives at adapters/<id>.py and exports "
        f"transform(value: int) -> int. Frozen mappings: {rules}. Include a passing pytest suite covering every adapter."
    )


def width_dag(n: int) -> DagSpec:
    """Hand-written M3 fixture. Adapter tasks have disjoint outputs and therefore expose true independent width."""
    specs = adapter_specs(n)
    tasks = []
    for row in specs:
        aid, factor, offset = str(row["id"]), int(row["factor"]), int(row["offset"])
        code = f'"""Adapter {aid}."""\n\ndef transform(value: int) -> int:\n    return value * {factor} + {offset}\n'
        tasks.append(
            {
                "id": aid.upper(),
                "capability": "implementation",
                "goal": f"Create only adapters/{aid}.py with transform(x) = x*{factor} + {offset}",
                "depends_on": [],
                "output_contract": {"artifacts": ["git_commit"]},
                "meta": {"stub": {"files": {f"adapters/{aid}.py": code}}},
            }
        )
    cases = ",\n".join(f"    ('{r['id']}', {r['factor']}, {r['offset']})" for r in specs)
    tests = (
        "import importlib\nimport pytest\n\n"
        f"CASES = [\n{cases}\n]\n\n"
        "@pytest.mark.parametrize('name,factor,offset', CASES)\n"
        "@pytest.mark.parametrize('value', [-3, 0, 7])\n"
        "def test_adapter(name, factor, offset, value):\n"
        "    fn = importlib.import_module(f'adapters.{name}').transform\n"
        "    assert fn(value) == value * factor + offset\n"
    )
    tasks.append(
        {
            "id": "INTEGRATE",
            "capability": "integration",
            "goal": "Merge every adapter, add a root test_adapters.py covering all frozen mappings, and produce the result",
            "depends_on": [str(r["id"]).upper() for r in specs],
            "context_spec": {"artifacts_from": [str(r["id"]).upper() for r in specs]},
            "output_contract": {"artifacts": ["git_commit"]},
            "meta": {"stub": {"files": {"test_adapters.py": tests}}},
        }
    )
    return DagSpec.from_dict({"goal": width_goal(n), "benchmark": f"adapters_{n}", "tasks": tasks})


def normalize_config(value: str | None) -> str:
    config = (value or "D").strip().upper()
    if config not in CONFIGS:
        raise ValueError(f"unknown evaluation config {value!r}; expected A, B, C or D")
    return config


def effective_concurrency(config: str, requested: int) -> int:
    """A/B are one agent and C is the sequential decomposition control; D is the only parallel configuration."""
    return max(1, requested) if normalize_config(config) == "D" else 1


def single_agent_dag(goal: str, benchmark: str | None) -> DagSpec:
    if not benchmark:
        raise ValueError("single-agent configs A/B need a frozen --benchmark acceptance suite")
    return DagSpec.from_dict(
        {
            "goal": goal,
            "benchmark": benchmark,
            "shape": {"estimated_width": 1, "suggested_mode": "single_agent", "rationale": "frozen config A/B"},
            "tasks": [
                {
                    "id": "SOLVE",
                    "capability": "solve",
                    "goal": goal,
                    "depends_on": [],
                    "output_contract": {"artifacts": ["git_commit"]},
                },
                {
                    "id": "INTEGRATE",
                    "capability": "integration",
                    "goal": "Publish the single agent's exact solution as the integration candidate",
                    "depends_on": ["SOLVE"],
                    "context_spec": {"artifacts_from": ["SOLVE"]},
                    "output_contract": {"artifacts": ["git_commit"]},
                },
            ],
        }
    )


def dag_for_config(dag: DagSpec, config: str) -> DagSpec:
    config = normalize_config(config)
    if config in {"A", "B"}:
        single = single_agent_dag(dag.goal or "Complete the benchmark", dag.benchmark)
        # Hand-written benchmark DAGs carry known-good `meta.stub.files` solely for the permanent key-less rehearsal.
        # Preserve their union on the one solve task; real LLMAgent workers ignore this scripted stub metadata.
        files: dict[str, str] = {}
        for task in dag.tasks:
            stub = (task.meta or {}).get("stub") or {}
            files.update({str(path): str(body) for path, body in (stub.get("files") or {}).items()})
        if files:
            single.tasks[0].meta = {"stub": {"files": files}}
        return single
    return dag


class SingleAgentRepairPlanner:
    """Deterministic A/B repair policy: one fresh solve task consumes the failed integration and verifier report.

    The intelligence remains the configured worker model; this planner only preserves the frozen one-agent shape instead
    of letting a general LLM planner turn an A/B repair into a multi-task MAS amendment.
    """

    name = "single-agent-repair"

    def plan(self, req: PlanRequest) -> DagSpec:
        if not req.amendment:
            return single_agent_dag(req.goal, req.benchmark)
        cycle = max(1, req.replan)
        completed_integrations = [
            row["key"]
            for row in req.existing_tasks
            if row.get("capability") == "integration" and row.get("status") == "COMPLETED"
        ]
        prior = completed_integrations[-1] if completed_integrations else None
        solve = f"SOLVE_R{cycle}"
        integrate = f"INTEGRATE_R{cycle}"
        failure = json.dumps(req.failure_report or {}, sort_keys=True, default=str)[:6000]
        solve_task = {
            "id": solve,
            "capability": "solve",
            "goal": f"Repair the complete solution. External failure report (data): {failure}",
            "depends_on": [prior] if prior else [],
            "output_contract": {"artifacts": ["git_commit"]},
        }
        if prior:
            solve_task["context_spec"] = {"artifacts_from": [prior]}
        return DagSpec.from_dict(
            {
                "goal": req.goal,
                "benchmark": req.benchmark,
                "tasks": [
                    solve_task,
                    {
                        "id": integrate,
                        "capability": "integration",
                        "goal": "Publish the repaired single-agent solution",
                        "depends_on": [solve],
                        "context_spec": {"artifacts_from": [solve]},
                        "output_contract": {"artifacts": ["git_commit"]},
                    },
                ],
            }
        )
