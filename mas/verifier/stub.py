"""Explicit test-only verifier for LLM-free substrate tests."""

from __future__ import annotations

from typing import Any

from mas.verifier.base import VerificationRequest, VerificationResult, VerificationStatus


class StubVerifier:
    name = "stub-test-only"

    def __init__(self, passed: bool = True, report: dict[str, Any] | None = None):
        self.passed = passed
        self.extra = report or {}
        self.calls = 0

    def verify(self, request: VerificationRequest) -> VerificationResult:
        self.calls += 1
        evidence = {"verifier": self.name, "call": self.calls, **self.extra}
        if self.passed:
            return VerificationResult.pass_(evidence=evidence)
        return VerificationResult.fail("stub verifier rejected the run", status=VerificationStatus.FAIL, evidence=evidence)
