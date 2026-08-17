"""Planner contract (docs/architecture.md §8, ADR-006).

`plan()` returns exactly one typed outcome: a DagSpec, a Questions batch, or a ContractProposal (ADR-007, ad-hoc
goals). It receives the goal, the registered capabilities, the Q&A history, remaining budgets, the frozen contract (if
any) and the previous rejection's errors. Output always goes through the deterministic driver (`runs.plan_run`):
validator for DAGs, `ask_questions` for questions, `contracts.propose` for proposals — the planner has no authority
(ADR-001). Provider-backed planner: `mas/planner/llm.py`. StubPlanner lets the whole flow be tested without a model.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from mas.planner.dag import QA, ContractProposal, DagSpec, Questions


@dataclass(frozen=True)
class PlanRequest:
    goal: str
    capabilities: frozenset[str]
    qa: tuple[QA, ...] = ()
    remaining: dict[str, Any] = field(default_factory=dict)  # budgets left (tasks, questions, tokens, plan_attempts, …)
    plan_attempt: int = 1  # increments when the validator rejected the previous output
    validation_errors: tuple[str, ...] = ()  # why the previous output was rejected, if any — returned to the planner as data
    run_id: Any = None  # telemetry / deadline context for provider-backed planners (never authority)
    benchmark: str | None = None  # set when a suite exists (benchmark run, or a frozen ad-hoc contract)
    needs_contract: bool = False  # ad-hoc goal without a frozen contract yet: the only acceptable non-question outcome
    contract: dict[str, Any] | None = None  # the frozen contract (definition of done), once approved
    deadline_s: float | None = None  # remaining run wall-clock, seconds
    tool_registry: dict[str, tuple[str, ...]] = field(default_factory=dict)  # capability → tool families it may use


class Planner(Protocol):
    name: str

    def plan(self, req: PlanRequest) -> DagSpec | Questions | ContractProposal: ...


class StubPlanner:
    """Scripted planner: asks the given question batches first (one per call), then returns the DAG."""

    name = "stub"

    def __init__(self, dag: DagSpec, questions: list[list[str]] | None = None, contract: ContractProposal | None = None):
        self.dag = dag
        self.questions = [list(q) for q in (questions or [])]
        self.contract = contract  # proposed when the run has no frozen contract yet (ad-hoc goal)
        self.requests: list[PlanRequest] = []

    def plan(self, req: PlanRequest) -> DagSpec | Questions | ContractProposal:
        self.requests.append(req)
        asked = len(req.qa)
        if asked < len(self.questions):
            return Questions(
                questions=self.questions[asked], context=f"stub planner needs input ({asked + 1}/{len(self.questions)})"
            )
        if req.needs_contract and self.contract is not None:
            return self.contract
        return self.dag
