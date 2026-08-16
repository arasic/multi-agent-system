"""Disposable workers (invariant I-6, I-7).

- base.py       Agent protocol, TaskContext, AgentResult
- runtime.py    Worker loop: claim → attempt → workspace → context → agent → publish → report; heartbeat thread
- stub.py       StubAgent: scripted deterministic behaviour so the substrate is testable without an LLM
- workspace.py  git worktree per attempt — roadmap step 6 (placeholder)
"""

from mas.workers.base import Agent, AgentResult, ArtifactOut, TaskContext
from mas.workers.runtime import Worker
from mas.workers.stub import StubAgent

__all__ = ["Agent", "AgentResult", "ArtifactOut", "StubAgent", "TaskContext", "Worker"]
