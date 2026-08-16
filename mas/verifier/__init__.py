"""External verification (ADR-003). A stage run by the orchestrator, never a task, never an LLM.

- base.py        Verifier protocol + VerificationResult
- stub.py        StubVerifier for LLM-free substrate tests
- acceptance.py  fixed acceptance-suite verifier — roadmap step 7 (not yet)
"""

from mas.verifier.base import VerificationResult, Verifier
from mas.verifier.stub import StubVerifier

__all__ = ["StubVerifier", "VerificationResult", "Verifier"]
