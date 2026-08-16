"""Planner contract (docs/architecture.md §8, ADR-006).

`plan()` returns either a DagSpec or a Questions batch. It receives the goal, the registered capabilities,
the Q&A history so far, and remaining budgets. Output always goes through mas.planner.validator (DAG) or
runs.ask_questions (Questions) — the planner has no authority (ADR-001).

The LLM planner arrives at roadmap step 11. StubPlanner lets the whole flow be tested without a model.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from mas.planner.dag import QA, DagSpec, Questions


@dataclass(frozen=True)
class PlanRequest:
    goal: str
    capabilities: frozenset[str]
    qa: tuple[QA, ...] = ()
    remaining: dict[str, Any] = field(default_factory=dict)  # budgets left (tasks, questions, tokens, …)
    plan_attempt: int = 1  # increments when the validator rejected the previous DAG
    validation_errors: tuple[str, ...] = ()  # why the previous DAG was rejected, if any


class Planner(Protocol):
    name: str

    def plan(self, req: PlanRequest) -> DagSpec | Questions: ...


class StubPlanner:
    """Scripted planner: asks the given question batches first (one per call), then returns the DAG."""

    name = "stub"

    def __init__(self, dag: DagSpec, questions: list[list[str]] | None = None):
        self.dag = dag
        self.questions = [list(q) for q in (questions or [])]
        self.requests: list[PlanRequest] = []

    def plan(self, req: PlanRequest) -> DagSpec | Questions:
        self.requests.append(req)
        asked = len(req.qa)
        if asked < len(self.questions):
            return Questions(
                questions=self.questions[asked], context=f"stub planner needs input ({asked + 1}/{len(self.questions)})"
            )
        return self.dag
