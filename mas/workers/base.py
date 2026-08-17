"""Agent contract. An agent is a bounded execution unit: it gets a task, scoped inputs, a workspace, and returns a report.

It never sets state (I-2), never calls the verifier (I-3), never talks to other agents (I-8).
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from mas.models.types import Artifact, Attempt, Run, Task
from mas.providers.base import ModelProvider


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
    # A7: competing candidate inputs for the same output slot (e.g. two `document:design.md` from two tasks). The agent
    # MUST publish one `decision` artifact per slot: ArtifactOut(type="decision", ref="decision:<slot>",
    # meta={"slot", "winner": <artifact id>, "rationale"}); the runtime accepts the winner and supersedes the losers.
    competing: dict[str, list[Artifact]] = field(default_factory=dict)
    # The model for this attempt: a MeteredProvider (telemetry, pricing, per-attempt call budget, deadline, cancel)
    # handed over by the runtime, or None when the worker has no model (stub agents). Agents never build providers.
    model: ModelProvider | None = None
    # Attempt runtime deadline (time.monotonic()); tools and model calls are clamped to it. None = unbounded (tests).
    deadline: float | None = None
    # Confined execution for command tools (mas/workers/execution.py), created and closed by the runtime — the
    # sandbox dies with the attempt (settlement, cancellation, timeout, worker death). None = no command tools.
    exec_backend: Any = None


class Agent(Protocol):
    name: str

    def execute(self, ctx: TaskContext) -> AgentResult: ...
