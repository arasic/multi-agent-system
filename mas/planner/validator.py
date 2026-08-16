"""Deterministic DAG validator (docs/architecture.md §8; ADR-001). No model calls.

Rules (numbering follows architecture.md):
  1  cycle                                   ✔ implemented
  2  dependency references a missing task    ✔
  3  capability has no registered worker     ✔ when `capabilities` is provided
  4  tool unavailable / permission prohibited  – roadmap step 12 (tool layer does not exist yet)
  5  task lacks an output_contract           ✔
  6  exactly one `integration` sink          ✔ (auto-append when allowed)
  7  task count exceeds max_tasks            ✔
  8  estimated cost exceeds remaining budget – roadmap step 12 (no cost model yet)
  9  amendment rules                         – roadmap step 13
Extra: duplicate ids, empty DAG, blank capability.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter

from mas.models.enums import INTEGRATION_CAPABILITY
from mas.models.types import Budgets
from mas.planner.dag import DagSpec, TaskSpec

AUTO_INTEGRATION_ID = "T_integrate"


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


def validate(
    dag: DagSpec,
    *,
    budgets: Budgets | None = None,
    capabilities: set[str] | frozenset[str] | None = None,
    existing_task_count: int = 0,
    auto_integration: bool = True,
) -> ValidationResult:
    budgets = budgets or Budgets()
    errors: list[ValidationError] = []
    tasks = [TaskSpec.from_dict(t.to_dict()) for t in dag.tasks]  # defensive copy
    result = DagSpec(tasks=tasks, goal=dag.goal, benchmark=dag.benchmark)

    if not tasks:
        errors.append(ValidationError("empty", "DAG has no tasks"))
        return ValidationResult(result, errors)

    ids = [t.id for t in tasks]
    seen: set[str] = set()
    for i in ids:
        if i in seen:
            errors.append(ValidationError("duplicate", f"duplicate task id {i!r}", i))
        seen.add(i)
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

    # 7 — task count
    total = existing_task_count + len(tasks)
    if total > budgets.max_tasks:
        errors.append(ValidationError("7", f"task count {total} exceeds max_tasks {budgets.max_tasks}"))

    return ValidationResult(DagSpec(tasks=tasks, goal=dag.goal, benchmark=dag.benchmark), errors, auto_added)
