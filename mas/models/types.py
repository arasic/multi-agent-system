"""Row-shaped dataclasses. Thin: they mirror the tables, no behaviour."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from mas.models.enums import ArtifactStatus, AttemptStatus, RunStatus, TaskStatus


@dataclass(frozen=True)
class Budgets:
    """Hard limits for a run. Enforced by the orchestrator, never requested from agents."""

    max_concurrency: int = 4
    max_tasks: int = 50
    max_attempts_per_task: int = 3
    max_replans: int = 2
    max_plan_attempts: int = 3
    max_tokens: int = 2_000_000
    max_cost_usd: float = 20.0
    max_wallclock_s: int = 3600
    max_attempt_runtime_s: int = 600
    # Per-attempt token allocation: what the meter hands every attempt (capped by what the run has left). Also the
    # unit of validator rule 8 — a plan is admissible only if the run can fund one such attempt for every open task.
    max_attempt_tokens: int = 200_000
    lease_s: int = 30
    max_questions: int = 3  # clarifying-question batches the planner may ask (ADR-006)
    deadline_at: datetime | None = None


@dataclass
class Run:
    id: UUID
    goal: str
    status: RunStatus
    benchmark: str | None = None
    config: str | None = None
    base_ref: str | None = None
    pool: str = "default"
    budgets: Budgets = field(default_factory=Budgets)
    tokens_used: int = 0
    cost_used_usd: float = 0.0
    replans_used: int = 0
    tasks_created: int = 0
    questions_asked: int = 0
    verdict: str | None = None
    verdict_reason: str | None = None  # VerdictReason code for non-passing terminal runs (ADR-008 §6)
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> Run:
        return cls(
            id=r["id"],
            goal=r["goal"],
            status=RunStatus(r["status"]),
            benchmark=r.get("benchmark"),
            config=r.get("config"),
            base_ref=r.get("base_ref"),
            pool=r.get("pool") or "default",
            budgets=Budgets(
                max_concurrency=r["max_concurrency"],
                max_tasks=r["max_tasks"],
                max_attempts_per_task=r["max_attempts_per_task"],
                max_replans=r["max_replans"],
                max_plan_attempts=r["max_plan_attempts"],
                max_tokens=r["max_tokens"],
                max_cost_usd=float(r["max_cost_usd"]),
                max_wallclock_s=r["max_wallclock_s"],
                max_attempt_runtime_s=r["max_attempt_runtime_s"],
                max_attempt_tokens=r.get("max_attempt_tokens", 200_000),
                lease_s=r["lease_s"],
                max_questions=r.get("max_questions", 3),
                deadline_at=r.get("deadline_at"),
            ),
            tokens_used=r["tokens_used"],
            cost_used_usd=float(r["cost_used_usd"]),
            replans_used=r["replans_used"],
            tasks_created=r["tasks_created"],
            questions_asked=r.get("questions_asked", 0),
            verdict=r.get("verdict"),
            verdict_reason=r.get("verdict_reason"),
            created_at=r.get("created_at"),
            started_at=r.get("started_at"),
            finished_at=r.get("finished_at"),
        )


@dataclass
class Task:
    id: UUID
    run_id: UUID
    key: str
    goal: str
    capability: str
    status: TaskStatus
    input_contract: dict[str, Any] = field(default_factory=dict)
    output_contract: dict[str, Any] = field(default_factory=dict)
    context_spec: dict[str, Any] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)
    tools: list[str] = field(default_factory=list)  # allow-list for this task's agent (validator rule 4)
    max_attempts: int = 3
    created_by: str = "planner"
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> Task:
        return cls(
            id=r["id"],
            run_id=r["run_id"],
            key=r["key"],
            goal=r["goal"],
            capability=r["capability"],
            status=TaskStatus(r["status"]),
            input_contract=r.get("input_contract") or {},
            output_contract=r.get("output_contract") or {},
            context_spec=r.get("context_spec") or {},
            meta=r.get("meta") or {},
            tools=list(r.get("tools") or []),
            max_attempts=r["max_attempts"],
            created_by=r["created_by"],
            created_at=r.get("created_at"),
            updated_at=r.get("updated_at"),
        )


@dataclass
class Attempt:
    id: UUID
    task_id: UUID
    attempt_number: int
    status: AttemptStatus
    worker_id: str | None = None
    lease_until: datetime | None = None
    workspace_ref: str | None = None
    model: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    failure_reason: str | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> Attempt:
        return cls(
            id=r["id"],
            task_id=r["task_id"],
            attempt_number=r["attempt_number"],
            status=AttemptStatus(r["status"]),
            worker_id=r.get("worker_id"),
            lease_until=r.get("lease_until"),
            workspace_ref=r.get("workspace_ref"),
            model=r.get("model"),
            input_tokens=r.get("input_tokens", 0),
            output_tokens=r.get("output_tokens", 0),
            cost_usd=float(r.get("cost_usd", 0) or 0),
            started_at=r.get("started_at"),
            finished_at=r.get("finished_at"),
            failure_reason=r.get("failure_reason"),
        )


@dataclass
class Artifact:
    id: UUID
    run_id: UUID
    type: str
    ref: str
    status: ArtifactStatus
    task_id: UUID | None = None
    attempt_id: UUID | None = None
    superseded_by: UUID | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> Artifact:
        return cls(
            id=r["id"],
            run_id=r["run_id"],
            type=r["type"],
            ref=r["ref"],
            status=ArtifactStatus(r["status"]),
            task_id=r.get("task_id"),
            attempt_id=r.get("attempt_id"),
            superseded_by=r.get("superseded_by"),
            meta=r.get("meta") or {},
            created_at=r.get("created_at"),
        )


@dataclass
class Event:
    id: int
    run_id: UUID
    type: str
    ts: datetime
    task_id: UUID | None = None
    attempt_id: UUID | None = None
    worker_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> Event:
        return cls(
            id=r["id"],
            run_id=r["run_id"],
            type=r["type"],
            ts=r["ts"],
            task_id=r.get("task_id"),
            attempt_id=r.get("attempt_id"),
            worker_id=r.get("worker_id"),
            payload=r.get("payload") or {},
        )
