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


def settings() -> Settings:
    return Settings()
