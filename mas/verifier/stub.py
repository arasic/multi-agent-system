"""Explicit test-only verifier for LLM-free substrate tests."""

from __future__ import annotations

from typing import Any

from mas.verifier.base import VerificationRequest, VerificationResult, VerificationStatus


class StubVerifier:
    name = "stub-test-only"

    def __init__(
        self,
        passed: bool = True,
        report: dict[str, Any] | None = None,
        *,
        fail_times: int = 0,
        script: list[VerificationResult] | None = None,
    ):
        """`passed`: the standing answer. `fail_times`: FAIL that many verifications first, then `passed` (the bounded
        repair demo: FAIL → repair → PASS). `script`: explicit results per call, in order (the last one repeats)."""
        self.passed = passed
        self.extra = report or {}
        self.fail_times = fail_times
        self.script = list(script or [])
        self.calls = 0

    def verify(self, request: VerificationRequest) -> VerificationResult:
        self.calls += 1
        if self.script:
            return self.script[min(self.calls, len(self.script)) - 1]
        evidence = {"verifier": self.name, "call": self.calls, **self.extra}
        if self.calls <= self.fail_times:
            return VerificationResult.fail(
                f"stub verifier rejected the run (scripted failure {self.calls}/{self.fail_times})",
                status=VerificationStatus.FAIL,
                evidence=evidence,
            )
        if self.passed:
            return VerificationResult.pass_(evidence=evidence)
        return VerificationResult.fail("stub verifier rejected the run", status=VerificationStatus.FAIL, evidence=evidence)
