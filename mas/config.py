"""Runtime settings. Environment variables only; no config files in the MVP."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    database_url: str = field(default_factory=lambda: _env("MAS_DATABASE_URL", "postgresql://mas:mas@localhost:5432/mas"))
    worker_capabilities: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            c.strip()
            for c in _env("MAS_WORKER_CAPABILITIES", "architecture,implementation,testing,integration,solve").split(",")
            if c.strip()
        )
    )
    worker_poll_s: float = field(default_factory=lambda: float(_env("MAS_WORKER_POLL_S", "0.5")))
    orchestrator_tick_s: float = field(default_factory=lambda: float(_env("MAS_ORCHESTRATOR_TICK_S", "0.5")))
    # Pool served by long-running services (compose worker/orchestrator). In-process `mas run` uses local:<pid>.
    pool: str = field(default_factory=lambda: _env("MAS_POOL", "default"))
    # Workspaces (ADR-002): "git" = bare repo per run + worktree per attempt; "none" = no filesystem (stub/unit tests)
    workspace: str = field(default_factory=lambda: _env("MAS_WORKSPACE", "git"))
    repo_root: str = field(default_factory=lambda: _env("MAS_REPO_ROOT", ".mas/repos"))
    worktree_root: str = field(default_factory=lambda: _env("MAS_WORKTREE_ROOT", ".mas/worktrees"))
    keep_worktrees: bool = field(default_factory=lambda: _env("MAS_KEEP_WORKTREES", "0") in {"1", "true", "yes"})
    acceptance_root: str = field(default_factory=lambda: _env("MAS_ACCEPTANCE_ROOT", "acceptance"))
    verifier_image: str = field(default_factory=lambda: _env("MAS_VERIFIER_IMAGE", "mas-verifier:latest"))
    verifier_timeout_s: int = field(default_factory=lambda: int(_env("MAS_VERIFIER_TIMEOUT_S", "30")))
    verifier_cpus: float = field(default_factory=lambda: float(_env("MAS_VERIFIER_CPUS", "1.0")))
    verifier_memory_mb: int = field(default_factory=lambda: int(_env("MAS_VERIFIER_MEMORY_MB", "256")))
    verifier_pids: int = field(default_factory=lambda: int(_env("MAS_VERIFIER_PIDS", "128")))


def settings() -> Settings:
    return Settings()
