"""Planning: planner contract (+ StubPlanner), typed outputs, deterministic DAG validator, capability→tools policy.

- dag.py           DagSpec / TaskSpec (planner output & hand-written DAG file), Questions, QA, parse_plan
- planner.py       Planner protocol, PlanRequest, StubPlanner (LLM planner: roadmap step 11)
- validator.py     deterministic rules (docs/architecture.md §8) — 1,2,3,4,5,6,7 now; 8,9 at steps 12/13
- capabilities.py  capability → allowed tools registry (rule 4)
"""

from mas.planner.dag import QA, DagSpec, Questions, TaskSpec, parse_plan
from mas.planner.planner import Planner, PlanRequest, StubPlanner
from mas.planner.validator import ValidationError, validate

__all__ = [
    "QA",
    "DagSpec",
    "PlanRequest",
    "Planner",
    "Questions",
    "StubPlanner",
    "TaskSpec",
    "ValidationError",
    "parse_plan",
    "validate",
]
