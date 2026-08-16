from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from mas.db.connection import Conn
from mas.models.types import Run


@dataclass(frozen=True)
class VerificationResult:
    passed: bool
    report: dict[str, Any] = field(default_factory=dict)


class Verifier(Protocol):
    """Runs the fixed acceptance suite against the run's integration artifact. Deterministic."""

    name: str

    def verify(self, conn: Conn, run: Run) -> VerificationResult: ...
