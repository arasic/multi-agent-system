"""Agent contract. An agent is a bounded execution unit: it gets a task, scoped inputs, a workspace, and returns a report.

It never sets state (I-2), never calls the verifier (I-3), never talks to other agents (I-8).
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from mas.models.types import Artifact, Attempt, Run, Task


@dataclass(frozen=True)
class ArtifactOut:
    """An artifact the agent wants published. In a git workspace, file artifacts use `ref="path:<relpath>"`;
    the runtime commits the worktree and rewrites them to `<sha>:<relpath>`. Agents never mint `git_commit`
    artifacts themselves — the runtime does, from the commit it made."""

    type: str
    ref: str
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class AgentResult:
    success: bool
    failure_reason: str | None = None
    artifacts: list[ArtifactOut] = field(default_factory=list)
    usage: dict[str, Any] = field(default_factory=dict)  # model, input_tokens, output_tokens, cost_usd
    new_work_required: str | None = None
    simulate_death: bool = False  # test hook: worker drops the attempt without reporting (crash simulation)


@dataclass
class TaskContext:
    run: Run
    task: Task
    attempt: Attempt
    inputs: list[Artifact]  # outputs of dependency tasks named by context_spec (never "the whole repo")
    workspace: Path | None
    cancel: threading.Event  # set when the attempt is no longer RUNNING (reaped/cancelled) — stop cooperatively
    tools: list[str] = field(default_factory=list)  # allow-list validated by rule 4; the tool layer binds names to impls
    paths: list[str] = field(
        default_factory=list
    )  # context_spec.paths — the only paths the agent should read (tool layer enforces)
    conflicts: list[str] = field(default_factory=list)  # unresolved merge conflicts left by input assembly (agent must resolve)


class Agent(Protocol):
    name: str

    def execute(self, ctx: TaskContext) -> AgentResult: ...
