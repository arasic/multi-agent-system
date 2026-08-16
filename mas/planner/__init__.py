"""Planning: the LLM planner (step 11, not yet) and the deterministic DAG validator.

- dag.py        typed DAG spec (what the planner emits, what a hand-written DAG file contains)
- validator.py  deterministic rules (docs/architecture.md §8). Structural subset now; full set at roadmap step 12.
- planner.py    LLM planner — roadmap step 11 (placeholder).
"""

from mas.planner.dag import DagSpec, TaskSpec
from mas.planner.validator import ValidationError, validate

__all__ = ["DagSpec", "TaskSpec", "ValidationError", "validate"]
