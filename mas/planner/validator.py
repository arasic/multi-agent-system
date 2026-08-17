"""Deterministic DAG validator (docs/architecture.md §8; ADR-001). No model calls.

Rules (numbering follows architecture.md):
  1  cycle                                   ✔ implemented
  2  dependency references a missing task    ✔
  3  capability has no registered worker     ✔ when `capabilities` is provided
  4  tool unavailable / not allowed for capability / prohibited by policy  ✔ (policy half; tool impls at step 10)
  5  task lacks an output_contract           ✔
  6  exactly one `integration` sink          ✔ (auto-append when allowed; capability re-checked after)
  7  task count exceeds max_tasks; per-task max_attempts outside [1, max_attempts_per_task]  ✔
  8  estimated cost exceeds remaining budget – roadmap step 12 (no cost model yet)
  9  amendment rules                         – roadmap step 13
  shape  ADR-008 task-shape metadata is advisory but must be well-formed (never selects the execution mode)  ✔
Extra: duplicate ids, unsafe ids, empty DAG, blank capability.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter

from mas.models.enums import INTEGRATION_CAPABILITY
from mas.models.types import Budgets
from mas.planner.capabilities import FORBIDDEN_TOOLS, KNOWN_TOOLS, allowed_tools
from mas.planner.dag import DagSpec, TaskSpec

AUTO_INTEGRATION_ID = "T_integrate"
ToolRegistry = dict[str, frozenset[str]]
# task ids: short, filesystem/ref/URL-safe, planner-controlled → strict (defense in depth even though ids are never paths)
SAFE_TASK_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


@dataclass(frozen=True)
class ValidationError:
    rule: str
    message: str
    task_id: str | None = None

    def __str__(self) -> str:
        return f"[{self.rule}] {self.message}" + (f" (task {self.task_id})" if self.task_id else "")


@dataclass
class ValidationResult:
    dag: DagSpec
    errors: list[ValidationError] = field(default_factory=list)
    auto_added: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


SHAPE_MODES = ("single_agent", "sequential_workflow", "parallel_centralized_mas")
SHAPE_LEVELS = ("low", "medium", "high")
SHAPE_KEYS = {
    "estimated_width",  # int >= 1: how many tasks can run independently at once
    "dependency_density",  # 0..1
    "critical_path_ratio",  # 0..1: critical path length / task count
    "overlapping_outputs",  # list[str]: paths/artifacts several tasks would touch
    "coupling_risk",  # low|medium|high
    "integration_risk",  # low|medium|high
    "suggested_mode",  # SHAPE_MODES — a suggestion; A/B/C/D config decides through M3
    "rationale",  # short free text
}


def validate_shape(shape: dict | None) -> list[ValidationError]:
    """Task-shape metadata (ADR-008): optional, advisory, but if present it must be well-formed. Never selects a mode."""
    errs: list[ValidationError] = []
    if not shape:
        return errs
    if not isinstance(shape, dict):
        return [ValidationError("shape", "shape must be an object")]
    unknown = sorted(set(shape) - SHAPE_KEYS)
    if unknown:
        errs.append(ValidationError("shape", f"unknown shape keys {unknown}; allowed: {sorted(SHAPE_KEYS)}"))
    w = shape.get("estimated_width")
    if w is not None and (isinstance(w, bool) or not isinstance(w, int) or w < 1 or w > 1000):
        errs.append(ValidationError("shape", "estimated_width must be an integer >= 1"))
    for k in ("dependency_density", "critical_path_ratio"):
        v = shape.get(k)
        if v is not None and (isinstance(v, bool) or not isinstance(v, int | float) or not (0.0 <= float(v) <= 1.0)):
            errs.append(ValidationError("shape", f"{k} must be a number in [0, 1]"))
    oo = shape.get("overlapping_outputs")
    if oo is not None and (not isinstance(oo, list) or not all(isinstance(x, str) for x in oo) or len(oo) > 200):
        errs.append(ValidationError("shape", "overlapping_outputs must be a list of strings"))
    for k in ("coupling_risk", "integration_risk"):
        v = shape.get(k)
        if v is not None and v not in SHAPE_LEVELS:
            errs.append(ValidationError("shape", f"{k} must be one of {SHAPE_LEVELS}"))
    m = shape.get("suggested_mode")
    if m is not None and m not in SHAPE_MODES:
        errs.append(ValidationError("shape", f"suggested_mode must be one of {SHAPE_MODES}"))
    r = shape.get("rationale")
    if r is not None and (not isinstance(r, str) or len(r) > 2000):
        errs.append(ValidationError("shape", "rationale must be a string (<= 2000 chars)"))
    return errs


def validate(
    dag: DagSpec,
    *,
    budgets: Budgets | None = None,
    capabilities: set[str] | frozenset[str] | None = None,
    existing_task_count: int = 0,
    auto_integration: bool = True,
    tool_registry: ToolRegistry | None = None,
) -> ValidationResult:
    budgets = budgets or Budgets()
    errors: list[ValidationError] = []
    tasks = [TaskSpec.from_dict(t.to_dict()) for t in dag.tasks]  # defensive copy
    result = DagSpec(
        tasks=tasks, goal=dag.goal, benchmark=dag.benchmark, assumptions=list(dag.assumptions), shape=dict(dag.shape)
    )

    if not tasks:
        errors.append(ValidationError("empty", "DAG has no tasks"))
        return ValidationResult(result, errors)

    # shape — ADR-008 task-shape metadata is advisory but must be well-formed (it is recorded and later evaluated)
    errors.extend(validate_shape(dag.shape))

    ids = [t.id for t in tasks]
    seen: set[str] = set()
    for i in ids:
        if i in seen:
            errors.append(ValidationError("duplicate", f"duplicate task id {i!r}", i))
        seen.add(i)
        if not SAFE_TASK_ID.match(i) or ".." in i:
            errors.append(ValidationError("id", f"unsafe task id {i!r} (allowed: [A-Za-z0-9][A-Za-z0-9_.-]{{0,63}}, no '..')", i))
    idset = set(ids)

    for t in tasks:
        if not t.capability.strip():
            errors.append(ValidationError("capability", "blank capability", t.id))
        for d in t.depends_on:
            if d not in idset:
                errors.append(ValidationError("2", f"depends_on references unknown task {d!r}", t.id))
        if not t.output_contract or not t.output_contract.get("artifacts"):
            errors.append(ValidationError("5", "task lacks an output_contract with artifacts", t.id))
        # 7 — per-task retry override may not exceed the run's retry budget (or be < 1)
        if t.max_attempts is not None and not (1 <= t.max_attempts <= budgets.max_attempts_per_task):
            errors.append(
                ValidationError(
                    "7",
                    f"max_attempts {t.max_attempts} outside [1, max_attempts_per_task={budgets.max_attempts_per_task}]",
                    t.id,
                )
            )

    # 1 — cycle
    try:
        TopologicalSorter({t.id: [d for d in t.depends_on if d in idset] for t in tasks}).prepare()
    except CycleError as e:
        errors.append(ValidationError("1", f"DAG has a cycle: {e.args[1] if len(e.args) > 1 else ''}"))

    # 6 — exactly one integration sink
    auto_added: list[str] = []
    integ = [t for t in tasks if t.capability == INTEGRATION_CAPABILITY]
    if len(integ) > 1:
        errors.append(ValidationError("6", f"more than one integration task: {[t.id for t in integ]}"))
    elif len(integ) == 0:
        if auto_integration:
            depended = {d for t in tasks for d in t.depends_on}
            sinks = [t.id for t in tasks if t.id not in depended]
            tasks.append(
                TaskSpec(
                    id=AUTO_INTEGRATION_ID,
                    capability=INTEGRATION_CAPABILITY,
                    goal="Merge accepted candidate artifacts into the integration branch.",
                    depends_on=sinks,
                    output_contract={"artifacts": ["git_commit"]},
                    meta={"created_by": "system"},
                )
            )
            auto_added.append(AUTO_INTEGRATION_ID)
            idset.add(AUTO_INTEGRATION_ID)
        else:
            errors.append(ValidationError("6", "DAG has no integration task"))
    else:
        it = integ[0]
        depended = {d for t in tasks for d in t.depends_on}
        if it.id in depended:
            errors.append(ValidationError("6", "integration task must be the sink (nothing may depend on it)", it.id))
        # every other task must reach the integration task
        reach: set[str] = set()
        frontier = [it.id]
        deps_of = {t.id: t.depends_on for t in tasks}
        while frontier:
            cur = frontier.pop()
            for d in deps_of.get(cur, []):
                if d not in reach:
                    reach.add(d)
                    frontier.append(d)
        for t in tasks:
            if t.id != it.id and t.id not in reach:
                errors.append(ValidationError("6", "task does not reach the integration sink", t.id))

    # 3 — every capability (including a synthesized integration task's) must have a registered worker
    if capabilities is not None:
        for t in tasks:
            if t.capability not in capabilities:
                errors.append(ValidationError("3", f"capability {t.capability!r} has no registered worker", t.id))

    # 4 — tools: requested ⊆ allowed(capability); nothing forbidden; fill the default when none requested
    for t in tasks:
        allowed = allowed_tools(t.capability, tool_registry)
        if t.tools is None:
            t.tools = sorted(allowed)
            continue
        for tool in t.tools:
            if tool in FORBIDDEN_TOOLS:
                errors.append(ValidationError("4", f"tool {tool!r} is prohibited by policy", t.id))
            elif tool not in KNOWN_TOOLS:
                errors.append(ValidationError("4", f"tool {tool!r} is not available", t.id))
            elif tool not in allowed:
                errors.append(ValidationError("4", f"tool {tool!r} not allowed for capability {t.capability!r}", t.id))

    # 10 — context scoping: artifacts_from may only name tasks this task (transitively) depends on
    deps_of = {t.id: list(t.depends_on) for t in tasks}

    def ancestors(tid: str) -> set[str]:
        seen: set[str] = set()
        stack = list(deps_of.get(tid, []))
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            stack.extend(deps_of.get(cur, []))
        return seen

    for t in tasks:
        wanted = (t.context_spec or {}).get("artifacts_from")
        if not wanted:
            continue
        anc = ancestors(t.id)
        for k in wanted:
            if str(k) not in idset:
                errors.append(ValidationError("10", f"context_spec.artifacts_from names unknown task {k!r}", t.id))
            elif str(k) not in anc:
                errors.append(ValidationError("10", f"context_spec.artifacts_from names {k!r}, which is not a dependency", t.id))

    # 7 — task count
    total = existing_task_count + len(tasks)
    if total > budgets.max_tasks:
        errors.append(ValidationError("7", f"task count {total} exceeds max_tasks {budgets.max_tasks}"))

    return ValidationResult(
        DagSpec(tasks=tasks, goal=dag.goal, benchmark=dag.benchmark, assumptions=list(dag.assumptions), shape=dict(dag.shape)),
        errors,
        auto_added,
    )
