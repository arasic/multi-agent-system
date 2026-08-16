"""Status enums. Values match the PostgreSQL enum types in mas/db/migrations.

The *allowed transitions* live in mas/orchestrator/state_machine.py — this module only names the states.
"""

from __future__ import annotations

from enum import StrEnum


class RunStatus(StrEnum):
    CREATED = "CREATED"
    PLANNING = "PLANNING"
    AWAITING_INPUT = "AWAITING_INPUT"  # planner asked clarifying questions; waiting for an answer (ADR-006)
    RUNNING = "RUNNING"
    VERIFYING = "VERIFYING"
    REPLANNING = "REPLANNING"
    PASSED = "PASSED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"

    @property
    def terminal(self) -> bool:
        return self in RUN_TERMINAL


class TaskStatus(StrEnum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    RETRYABLE = "RETRYABLE"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"
    CANCELLED = "CANCELLED"

    @property
    def terminal(self) -> bool:
        return self in TASK_TERMINAL


class AttemptStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    ABANDONED = "ABANDONED"
    CANCELLED = "CANCELLED"

    @property
    def terminal(self) -> bool:
        return self is not AttemptStatus.RUNNING


class ArtifactStatus(StrEnum):
    CANDIDATE = "candidate"
    ACCEPTED = "accepted"
    SUPERSEDED = "superseded"
    REJECTED = "rejected"


RUN_TERMINAL = frozenset({RunStatus.PASSED, RunStatus.FAILED, RunStatus.ABORTED})
TASK_TERMINAL = frozenset({TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.BLOCKED, TaskStatus.CANCELLED})
TASK_UPSTREAM_BLOCKING = frozenset({TaskStatus.FAILED, TaskStatus.BLOCKED, TaskStatus.CANCELLED})

INTEGRATION_CAPABILITY = "integration"
SOLVE_CAPABILITY = "solve"  # single-agent configs A/B: one task with the whole goal
