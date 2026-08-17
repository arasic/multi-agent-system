"""Deterministic DAG validator (docs/architecture.md §8; ADR-001). No model calls.

Rules (numbering follows architecture.md):
  1  cycle                                   ✔ implemented
  2  dependency references a missing task    ✔
  3  capability has no registered worker     ✔ when `capabilities` is provided
  4  tool unavailable / not allowed for capability / prohibited by policy  ✔ (policy half; tool impls at step 10)
  5  task lacks an output_contract           ✔
  6  exactly one `integration` sink          ✔ (auto-append when allowed; capability re-checked after)
  7  task count exceeds max_tasks; per-task max_attempts outside [1, max_attempts_per_task]  ✔
  8  budget allocation: the plan must fit what the run has left  ✔ when `remaining` is provided (the driver always does)
       tokens   — every open task (this plan's + existing non-terminal ones) must be fundable for one attempt at the
                  run's per-attempt allocation `max_attempt_tokens`: open × allocation ≤ remaining tokens. System-owned:
                  the meter hands exactly this allocation to attempts; planner estimates play no part.
       time     — no per-attempt time allocation exists (the runtime cap is a timeout), so time is checked from what
                  is *known*: this run's shortest successful attempt so far (a lower bound, never the mean) and the
                  planner's own per-task estimates, which may only tighten: weighted critical path ≤ remaining
                  wall-clock and total work / max_concurrency ≤ remaining wall-clock; remaining wall-clock must be > 0.
       estimate — optional `estimate: {"tokens": int, "seconds": number}` per task is validated; an estimate above the
                  per-attempt allocation (tokens) or the per-attempt runtime cap (seconds) is infeasible → rejected.
       cost     — no price model here (model names never reach the validator); the run's cost budget is enforced at
                  run time; a plan is rejected only when the cost budget is already exhausted.
  9  amendment rules (step 13-lite)         ✔ when `existing` is provided (a re-plan): a new task id may not collide with
       any existing task (COMPLETED work is never removed or altered — nor is any other recorded task); an amendment may
       depend on / read from existing tasks only if they are COMPLETED; exactly one *new* integration sink (the old one
       is history); `max_replans` and repeated-amendment detection are the driver's (`runs.plan_run`).
  shape  ADR-008 task-shape metadata is advisory but must be well-formed (never selects the execution mode)  ✔
Extra: duplicate ids, unsafe ids, empty DAG, blank capability.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter
from typing import Any

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


@dataclass(frozen=True)
class Remaining:
    """What the run has left at plan time, measured by the driver (rule 8). `None` fields are not checked — hand-written
    DAG files validated offline have no run; the driver always supplies the full picture."""

    tokens: int | None = None
    wallclock_s: float | None = None
    cost_usd: float | None = None
    open_tasks: int = 0  # existing non-terminal tasks (re-plan): they still need funding alongside the new ones
    observed_attempt_s: float | None = None  # lower bound: shortest SUCCESS attempt so far (else shortest settled); None = none


@dataclass(frozen=True)
class ExistingTask:
    """A task already recorded for the run (re-plan). Amendments may build on it only if it is COMPLETED."""

    key: str
    status: str
    depends_on: tuple[str, ...] = ()


ESTIMATE_KEYS = {"tokens", "seconds"}


def _num(v: Any) -> float | None:
    if isinstance(v, bool) or not isinstance(v, int | float) or not math.isfinite(v) or v < 0:
        return None
    return float(v)


def validate_estimate(t: TaskSpec, budgets: Budgets) -> list[ValidationError]:
    """Rule 8, per task: the planner's optional cost estimate must be well-formed and feasible within one attempt."""
    est = t.estimate or {}
    if not isinstance(est, dict) or "_invalid" in est:
        return [ValidationError("8", "estimate must be an object {tokens: int, seconds: number}", t.id)]
    errs: list[ValidationError] = []
    unknown = sorted(set(est) - ESTIMATE_KEYS)
    if unknown:
        errs.append(ValidationError("8", f"unknown estimate keys {unknown}; allowed: {sorted(ESTIMATE_KEYS)}", t.id))
    if "tokens" in est:
        tok = _num(est["tokens"])
        if tok is None or int(tok) != tok:
            errs.append(ValidationError("8", "estimate.tokens must be a non-negative integer", t.id))
        elif tok > budgets.max_attempt_tokens:
            errs.append(
                ValidationError(
                    "8",
                    f"estimate.tokens {int(tok)} exceeds the per-attempt allocation max_attempt_tokens="
                    f"{budgets.max_attempt_tokens}: the task cannot finish within one attempt - split it",
                    t.id,
                )
            )
    if "seconds" in est:
        sec = _num(est["seconds"])
        if sec is None:
            errs.append(ValidationError("8", "estimate.seconds must be a non-negative number", t.id))
        elif sec > budgets.max_attempt_runtime_s:
            errs.append(
                ValidationError(
                    "8",
                    f"estimate.seconds {sec:g} exceeds max_attempt_runtime_s={budgets.max_attempt_runtime_s}: "
                    "the task cannot finish within one attempt - split it",
                    t.id,
                )
            )
    return errs


def estimated_seconds(t: TaskSpec) -> float:
    return _num((t.estimate or {}).get("seconds")) or 0.0


def critical_path_s(tasks: list[TaskSpec], weight: dict[str, float]) -> tuple[float, list[str]]:
    """Longest weighted dependency chain (seconds, task ids). Assumes the graph is acyclic (rule 1 ran first)."""
    idset = {t.id for t in tasks}
    deps = {t.id: [d for d in t.depends_on if d in idset] for t in tasks}
    order = list(TopologicalSorter(deps).static_order())
    best: dict[str, float] = {}
    prev: dict[str, str | None] = {}
    for tid in order:
        pick: str | None = None
        pick_w = -1.0
        for d in deps[tid]:
            if best.get(d, 0.0) > pick_w:
                pick, pick_w = d, best.get(d, 0.0)
        best[tid] = max(pick_w, 0.0) + weight.get(tid, 0.0)
        prev[tid] = pick
    if not best:
        return 0.0, []
    end = max(best, key=lambda k: (best[k], k))
    path: list[str] = []
    cur: str | None = end
    while cur is not None:
        path.append(cur)
        cur = prev[cur]
    return best[end], list(reversed(path))


def validate_budget(tasks: list[TaskSpec], budgets: Budgets, remaining: Remaining, *, acyclic: bool) -> list[ValidationError]:
    """Rule 8 - deterministic budget allocation (see module docstring). No planner estimate can loosen this check."""
    errs: list[ValidationError] = []
    for t in tasks:
        errs.extend(validate_estimate(t, budgets))
    # tokens: one attempt at the run's per-attempt allocation for every open task
    if remaining.tokens is not None:
        open_n = remaining.open_tasks + len(tasks)
        need = open_n * budgets.max_attempt_tokens
        if need > remaining.tokens:
            fit = max(0, remaining.tokens // max(1, budgets.max_attempt_tokens) - remaining.open_tasks)
            errs.append(
                ValidationError(
                    "8",
                    f"{open_n} open tasks x max_attempt_tokens {budgets.max_attempt_tokens} = {need} tokens needed, "
                    f"{remaining.tokens} remain: at most {fit} new tasks fit (or lower the per-attempt allocation)",
                )
            )
    # cost: no price model here; reject only when the run's cost budget is already gone
    if remaining.cost_usd is not None and remaining.cost_usd <= 0:
        errs.append(ValidationError("8", f"cost budget exhausted ({remaining.cost_usd:.4f} USD remain)"))
    # time: known lower bounds only - observed attempt duration in this run, planner estimates (tighten only)
    if remaining.wallclock_s is not None:
        if remaining.wallclock_s <= 0:
            errs.append(ValidationError("8", "no wall-clock left"))
        elif acyclic:
            floor = float(remaining.observed_attempt_s or 0.0)
            weight = {t.id: max(floor, estimated_seconds(t)) for t in tasks}
            total = sum(weight.values())
            path_s, path = critical_path_s(tasks, weight)
            basis = f"shortest observed attempt {floor:.1f}s" if floor else "planner estimates"
            if path_s > remaining.wallclock_s:
                errs.append(
                    ValidationError(
                        "8",
                        f"critical path {path} needs at least {path_s:.0f}s ({basis}) but {remaining.wallclock_s:.0f}s remain",
                    )
                )
            elif total / max(1, budgets.max_concurrency) > remaining.wallclock_s:
                errs.append(
                    ValidationError(
                        "8",
                        f"{len(tasks)} tasks need at least {total:.0f}s of work ({basis}) over max_concurrency "
                        f"{budgets.max_concurrency} but {remaining.wallclock_s:.0f}s remain",
                    )
                )
    return errs


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
    remaining: Remaining | None = None,
    existing: list[ExistingTask] | tuple[ExistingTask, ...] | None = None,
) -> ValidationResult:
    """`existing` marks an *amendment* (re-plan): `dag` holds only new tasks, which may depend on existing COMPLETED
    tasks; rule 9 protects everything already recorded; the amendment gets its own integration sink."""
    budgets = budgets or Budgets()
    errors: list[ValidationError] = []
    tasks = [TaskSpec.from_dict(t.to_dict()) for t in dag.tasks]  # defensive copy
    prior = {e.key: e for e in (existing or ())}
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
    # 9 — an amendment never removes or alters recorded tasks (COMPLETED work above all): ids are new, and it may only
    # build on COMPLETED tasks
    for i in ids:
        if i in prior:
            errors.append(ValidationError("9", f"amendment may not alter existing task {i!r} ({prior[i].status})", i))
    known = idset | set(prior)

    for t in tasks:
        if not t.capability.strip():
            errors.append(ValidationError("capability", "blank capability", t.id))
        for d in t.depends_on:
            if d not in known:
                errors.append(ValidationError("2", f"depends_on references unknown task {d!r}", t.id))
            elif d in prior and d not in idset and prior[d].status != "COMPLETED":
                errors.append(
                    ValidationError("9", f"amendment may only build on COMPLETED tasks; {d!r} is {prior[d].status}", t.id)
                )
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
    acyclic = True
    try:
        TopologicalSorter({t.id: [d for d in t.depends_on if d in idset] for t in tasks}).prepare()
    except CycleError as e:
        acyclic = False
        errors.append(ValidationError("1", f"DAG has a cycle: {e.args[1] if len(e.args) > 1 else ''}"))

    # 6 — exactly one integration sink (among the *new* tasks: on an amendment the old sink is COMPLETED history)
    auto_added: list[str] = []
    integ = [t for t in tasks if t.capability == INTEGRATION_CAPABILITY]
    if len(integ) > 1:
        errors.append(ValidationError("6", f"more than one integration task: {[t.id for t in integ]}"))
    elif len(integ) == 0:
        if auto_integration:
            depended = {d for t in tasks for d in t.depends_on}
            sinks = [t.id for t in tasks if t.id not in depended]
            auto_id = AUTO_INTEGRATION_ID
            n = 1
            while auto_id in known:  # amendments get a fresh sink id; recorded tasks are never reused
                n += 1
                auto_id = f"{AUTO_INTEGRATION_ID}_{n}"
            tasks.append(
                TaskSpec(
                    id=auto_id,
                    capability=INTEGRATION_CAPABILITY,
                    goal="Merge accepted candidate artifacts into the integration branch.",
                    depends_on=sinks,
                    output_contract={"artifacts": ["git_commit"]},
                    meta={"created_by": "system"},
                )
            )
            auto_added.append(auto_id)
            idset.add(auto_id)
            known.add(auto_id)
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

    # 10 — context scoping: artifacts_from may only name tasks this task (transitively) depends on (existing edges count)
    deps_of = {k: list(e.depends_on) for k, e in prior.items()}
    deps_of.update({t.id: list(t.depends_on) for t in tasks})

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
            if str(k) not in known:
                errors.append(ValidationError("10", f"context_spec.artifacts_from names unknown task {k!r}", t.id))
            elif str(k) not in anc:
                errors.append(ValidationError("10", f"context_spec.artifacts_from names {k!r}, which is not a dependency", t.id))
            elif str(k) in prior and prior[str(k)].status != "COMPLETED":
                st = prior[str(k)].status
                errors.append(ValidationError("9", f"amendment may only read from COMPLETED tasks; {k!r} is {st}", t.id))

    # 7 — task count
    total = existing_task_count + len(tasks)
    if total > budgets.max_tasks:
        errors.append(ValidationError("7", f"task count {total} exceeds max_tasks {budgets.max_tasks}"))

    # 8 — budget allocation (after auto-integration: the synthesized sink needs funding too)
    if remaining is not None:
        errors.extend(validate_budget(tasks, budgets, remaining, acyclic=acyclic))
    else:
        for t in tasks:
            errors.extend(validate_estimate(t, budgets))

    return ValidationResult(
        DagSpec(tasks=tasks, goal=dag.goal, benchmark=dag.benchmark, assumptions=list(dag.assumptions), shape=dict(dag.shape)),
        errors,
        auto_added,
    )
