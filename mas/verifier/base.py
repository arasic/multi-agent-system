"""Verifier boundary.

The orchestrator resolves database state into a read-only request. A verifier never
receives a database connection and therefore cannot publish artifacts or transition a
run. Only the orchestrator may turn returned evidence into a verdict.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID


class VerificationStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"
    INVALID = "INVALID"


class CheckStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"


@dataclass(frozen=True)
class CheckResult:
    id: str
    status: CheckStatus
    detail: str = ""
    duration_ms: int | None = None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"id": self.id, "status": self.status.value, "detail": self.detail}
        if self.duration_ms is not None:
            out["duration_ms"] = self.duration_ms
        return out


@dataclass(frozen=True)
class VerificationRequest:
    run_id: UUID
    benchmark: str | None
    repository: Path | None
    commit_sha: str | None
    # ADR-007: sha256 of the approved, frozen acceptance suite; when set, the suite on disk must hash to it
    expected_suite_sha256: str | None = None


@dataclass(frozen=True)
class VerificationResult:
    status: VerificationStatus
    checks: tuple[CheckResult, ...] = ()
    reason: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.status is VerificationStatus.PASS

    @property
    def report(self) -> dict[str, Any]:
        out: dict[str, Any] = {"status": self.status.value, "checks": [c.as_dict() for c in self.checks]}
        if self.reason:
            out["reason"] = self.reason
        out.update(self.evidence)
        return out

    @classmethod
    def pass_(cls, *, checks: tuple[CheckResult, ...] = (), evidence: dict[str, Any] | None = None):
        return cls(VerificationStatus.PASS, checks=checks, evidence=evidence or {})

    @classmethod
    def fail(
        cls,
        reason: str,
        *,
        status: VerificationStatus = VerificationStatus.FAIL,
        checks: tuple[CheckResult, ...] = (),
        evidence: dict[str, Any] | None = None,
    ):
        if status is VerificationStatus.PASS:
            raise ValueError("a failed verification cannot have PASS status")
        return cls(status, checks=checks, reason=reason, evidence=evidence or {})


class Verifier(Protocol):
    """Runs a fixed acceptance suite against one exact integration commit."""

    name: str

    def verify(self, request: VerificationRequest) -> VerificationResult: ...


class MissingVerifier:
    """Production-safe default: absence of a configured verifier can never pass a run."""

    name = "missing"

    def verify(self, request: VerificationRequest) -> VerificationResult:
        return VerificationResult.fail(
            "no acceptance verifier was configured",
            status=VerificationStatus.INVALID,
            evidence={"run_id": str(request.run_id)},
        )


class DeferredVerification:
    """Marker verifier for the orchestrator *service*: the orchestrator moves a run to VERIFYING and leaves it there
    for a separate verifier service (`mas verify --watch`) that has sandbox (Docker) access. It never produces a
    verdict itself; if no verifier service ever comes, the run's budgets end it (I-4) — nothing passes by default."""

    name = "deferred"

    def verify(self, request: VerificationRequest) -> VerificationResult:  # pragma: no cover - never called by design
        return VerificationResult.fail("verification is deferred to the verifier service", status=VerificationStatus.INVALID)
