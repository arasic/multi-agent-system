"""Stub verifier for LLM-free substrate tests. Real acceptance-suite verifier lands at roadmap step 7."""

from __future__ import annotations

from typing import Any

from mas.db.connection import Conn
from mas.models.types import Run
from mas.verifier.base import VerificationResult


class StubVerifier:
    name = "stub"

    def __init__(self, passed: bool = True, report: dict[str, Any] | None = None):
        self.passed = passed
        self.report = report or {}
        self.calls = 0

    def verify(self, conn: Conn, run: Run) -> VerificationResult:
        self.calls += 1
        return VerificationResult(passed=self.passed, report={"verifier": self.name, "call": self.calls, **self.report})
