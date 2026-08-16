"""LLM planner — roadmap step 11. Not implemented in M1.

Contract (see docs/architecture.md §8): given goal, constraints, registered capabilities/tools and remaining
budgets, return a DagSpec (typed JSON). On re-plan, additionally receives the current DAG, the artifact index
and the failure report, and returns an amendment. Output always goes through mas.planner.validator.
"""

from __future__ import annotations

from mas.planner.dag import DagSpec


class Planner:
    def plan(self, goal: str, *, capabilities: set[str]) -> DagSpec:  # pragma: no cover - placeholder
        raise NotImplementedError("LLM planner arrives at roadmap step 11; use a hand-written DAG file for now")
