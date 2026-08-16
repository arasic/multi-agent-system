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
    verifier_timeout_s: int = field(default_factory=lambda: int(_env("MAS_VERIFIER_TIMEOUT_S", "300")))
    verifier_cpus: float = field(default_factory=lambda: float(_env("MAS_VERIFIER_CPUS", "1.0")))
    verifier_memory_mb: int = field(default_factory=lambda: int(_env("MAS_VERIFIER_MEMORY_MB", "256")))
    verifier_pids: int = field(default_factory=lambda: int(_env("MAS_VERIFIER_PIDS", "128")))
    # Models (step 9). Specs are "<provider>:<model>" — provider ∈ {anthropic, openai, fake}; empty = no model for the role.
    # Model names and prices live HERE and in mas/providers/, nowhere else (CLAUDE.md, docs/models.md).
    model_planner: str = field(default_factory=lambda: _env("MAS_MODEL_PLANNER", ""))
    model_worker: str = field(default_factory=lambda: _env("MAS_MODEL_WORKER", ""))
    model_reviewer: str = field(default_factory=lambda: _env("MAS_MODEL_REVIEWER", ""))
    # JSON: {"<model id or prefix>": {"input": $/Mtok, "output": $/Mtok, "cache_read": ..., "cache_write": ...} | [in, out]}
    model_prices: str = field(default_factory=lambda: _env("MAS_MODEL_PRICES", ""))
    provider_timeout_s: float = field(default_factory=lambda: float(_env("MAS_PROVIDER_TIMEOUT_S", "600")))
    provider_max_retries: int = field(default_factory=lambda: int(_env("MAS_PROVIDER_MAX_RETRIES", "2")))
    anthropic_effort: str = field(default_factory=lambda: _env("MAS_ANTHROPIC_EFFORT", ""))  # low|medium|high|xhigh|max
    anthropic_thinking: bool = field(default_factory=lambda: _env("MAS_ANTHROPIC_THINKING", "1") not in {"0", "false", "no"})
    anthropic_fallbacks: str = field(default_factory=lambda: _env("MAS_ANTHROPIC_FALLBACKS", ""))  # "default" = beta opt-in
    openai_base_url: str = field(default_factory=lambda: _env("MAS_OPENAI_BASE_URL", "https://api.openai.com/v1"))
    openai_api_key: str = field(default_factory=lambda: _env("MAS_OPENAI_API_KEY", _env("OPENAI_API_KEY", "")))
    openai_max_tokens_field: str = field(default_factory=lambda: _env("MAS_OPENAI_MAX_TOKENS_FIELD", "max_completion_tokens"))
    # Per-attempt call budget enforced by the metered provider (bounded loops, antipatterns E1); the run's remaining
    # token budget caps it further.
    attempt_max_calls: int = field(default_factory=lambda: int(_env("MAS_ATTEMPT_MAX_CALLS", "40")))
    attempt_max_tokens: int = field(default_factory=lambda: int(_env("MAS_ATTEMPT_MAX_TOKENS", "300000")))


def settings() -> Settings:
    return Settings()
