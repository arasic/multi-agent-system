"""Trusted acceptance adapters (ADR-007 §4a): the only executable checks an approved contract can compile to.

- schema.py  typed criterion types + `parse_contract()`; unmappable → InvalidContract. Stdlib-only; also runs
             inside the verifier image (host and sandbox validate with the same code).
- runner.py  the in-sandbox executor (baked into the image at /opt/mas/adapters/runner.py).

Four adapters — build_succeeds, tests_required, http_status, restart_persists — and no generic DSL.
"""

from mas.verifier.adapters.schema import (
    CHECK_TYPES,
    PROTOCOL_VERSION,
    BuildSucceeds,
    Check,
    Contract,
    HttpExpect,
    HttpRequest,
    HttpStatus,
    InvalidContract,
    RestartPersists,
    Service,
    TestsRequired,
    parse_contract,
)

# The one command a contract-based suite may declare: the trusted runner over the contract next to it.
TRUSTED_RUNNER_COMMAND = ["python", "/opt/mas/adapters/runner.py", "/acceptance/contract.json"]

__all__ = [
    "CHECK_TYPES",
    "PROTOCOL_VERSION",
    "TRUSTED_RUNNER_COMMAND",
    "BuildSucceeds",
    "Check",
    "Contract",
    "HttpExpect",
    "HttpRequest",
    "HttpStatus",
    "InvalidContract",
    "RestartPersists",
    "Service",
    "TestsRequired",
    "parse_contract",
]
