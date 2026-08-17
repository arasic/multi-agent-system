"""Acceptance-contract lifecycle for ad-hoc goals (ADR-007) — deterministic, no model calls.

    planner proposes (ContractProposal)  ──▶  validated against the trusted adapters' schema (fail closed)
        ──▶ `contract_proposal` artifact + run AWAITING_INPUT (the one human approval per goal)
        ──▶ `mas approve <run> [--contract edited.json]`  ──▶ freeze:
                acceptance/<benchmark-id>/{contract.json, suite.json} written (trusted runner command, expected_checks
                = the contract's check ids), suite digest computed, `acceptance_contract` artifact (accepted, immutable,
                meta.sha256 + meta.suite_sha256), runs.benchmark set, event `contract.approved`, run back to PLANNING
        ──▶ the verifier loads it like any contract suite; the run's verification request pins the suite digest.

The planner never writes the suite files, never freezes, never changes budgets or run state — this module and the CLI
do, on the human's word. Workers see the frozen suite read-only (I-3).
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from mas.artifacts import store
from mas.db.connection import Conn
from mas.db.events import emit
from mas.models.enums import RunStatus
from mas.orchestrator import state_machine as sm
from mas.planner.dag import ContractProposal
from mas.verifier.adapters import TRUSTED_RUNNER_COMMAND
from mas.verifier.adapters.schema import InvalidContract, parse_contract

log = logging.getLogger(__name__)

MAX_REQUIREMENTS = 50
MAX_TEXT = 500
DEFAULT_SUITE_TIMEOUT_S = 240
BENCHMARK_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


class InvalidProposal(ValueError):
    def __init__(self, errors: list[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


# ----------------------------------------------------------------------------- validation (deterministic)


def validate_proposal(p: ContractProposal) -> list[str]:
    """Everything the human is asked to approve must already be executable by trusted code (fail closed)."""
    errs: list[str] = []
    if not p.requirements:
        errs.append("requirements must list at least one behavioural statement")
    if len(p.requirements) > MAX_REQUIREMENTS:
        errs.append(f"too many requirements (> {MAX_REQUIREMENTS})")
    for lst, name in ((p.requirements, "requirements"), (p.assumptions, "assumptions"), (p.exclusions, "exclusions")):
        for x in lst:
            if not isinstance(x, str) or not x.strip():
                errs.append(f"{name} entries must be non-empty strings")
                break
            if len(x) > MAX_TEXT:
                errs.append(f"{name} entry longer than {MAX_TEXT} chars")
                break
    try:
        contract = parse_contract(p.contract_dict())
    except InvalidContract as e:
        errs.append(f"unmappable acceptance criteria: {e}")
        return errs
    per_check = sum(int(getattr(c, "timeout_s", 0)) for c in contract.checks)
    if per_check > DEFAULT_SUITE_TIMEOUT_S:
        errs.append(f"sum of per-check timeouts ({per_check}s) exceeds the suite timeout ({DEFAULT_SUITE_TIMEOUT_S}s)")
    return errs


def suite_manifest(contract_doc: dict[str, Any], *, timeout_s: int = DEFAULT_SUITE_TIMEOUT_S) -> dict[str, Any]:
    """The suite.json the verifier requires for a contract-based suite: trusted runner, the contract's check ids."""
    contract = parse_contract(contract_doc)
    return {
        "protocol_version": 1,
        "command": list(TRUSTED_RUNNER_COMMAND),
        "expected_checks": list(contract.check_ids),
        "timeout_s": int(timeout_s),
    }


def benchmark_id_for(run_id: UUID) -> str:
    return f"adhoc-{str(run_id).replace('-', '')[:12]}"


# ----------------------------------------------------------------------------- DB side


def propose(conn: Conn, run_id: UUID, proposal: ContractProposal, *, planner: str = "planner") -> Any:
    """Record a valid proposal and park the run for the human's approval. Caller validated it (or we raise)."""
    errs = validate_proposal(proposal)
    if errs:
        raise InvalidProposal(errs)
    with conn.transaction():
        run = sm.lock_run(conn, run_id)
        if run.status not in {RunStatus.PLANNING, RunStatus.REPLANNING}:
            raise sm.IllegalTransition("run", run_id, run.status, RunStatus.AWAITING_INPUT)
        n = (
            1
            + conn.execute(
                "SELECT count(*) AS n FROM artifacts WHERE run_id = %s AND type = 'contract_proposal'", (run_id,)
            ).fetchone()["n"]
        )  # type: ignore[index]
        doc = proposal.to_dict()
        store.publish(
            conn,
            run_id=run_id,
            type="contract_proposal",
            ref=f"contract_proposal:{run_id}:{n}",
            meta={"proposal": doc, "planner": planner, "sha256": _sha(doc)},
        )
        emit(
            conn,
            run_id,
            "contract.proposed",
            payload={"n": n, "requirements": proposal.requirements, "checks": [c.get("id") for c in proposal.checks]},
        )
        return sm.transition_run(conn, run_id, RunStatus.AWAITING_INPUT, payload={"contract_proposal": n})


def pending_proposal(conn: Conn, run_id: UUID) -> dict[str, Any] | None:
    """The latest proposal awaiting approval, or None (approved already, or the run is not waiting on one)."""
    if approved(conn, run_id) is not None:
        return None
    row = conn.execute(
        "SELECT meta FROM artifacts WHERE run_id = %s AND type = 'contract_proposal' ORDER BY created_at DESC, id DESC LIMIT 1",
        (run_id,),
    ).fetchone()
    return dict(row["meta"]) if row else None


def approved(conn: Conn, run_id: UUID) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT meta FROM artifacts WHERE run_id = %s AND type = 'acceptance_contract' AND status = 'accepted' "
        "ORDER BY created_at DESC, id DESC LIMIT 1",
        (run_id,),
    ).fetchone()
    return dict(row["meta"]) if row else None


@dataclass(frozen=True)
class Frozen:
    benchmark: str
    suite_dir: Path
    sha256: str
    suite_sha256: str


def approve(
    conn: Conn,
    run_id: UUID,
    *,
    acceptance_root: Path,
    contract_doc: dict[str, Any] | None = None,  # None → the pending proposal as proposed; else the human's edited version
    approved_by: str = "human",
    suite_timeout_s: int = DEFAULT_SUITE_TIMEOUT_S,
) -> Frozen:
    """Freeze the definition of done. Writes the suite directory, publishes the immutable artifact, points the run's
    benchmark at it and returns the run to PLANNING. Idempotent refusal if a contract is already frozen."""
    if approved(conn, run_id) is not None:
        raise InvalidProposal(["a contract is already frozen for this run"])
    pending = pending_proposal(conn, run_id)
    if contract_doc is None:
        if pending is None:
            raise InvalidProposal(["nothing to approve: no contract proposal pending"])
        contract_doc = dict(pending["proposal"]["contract"])
    proposal_meta = pending or {}
    try:
        contract = parse_contract(contract_doc)
    except InvalidContract as e:
        raise InvalidProposal([f"unmappable acceptance criteria: {e}"]) from e
    per_check = sum(int(getattr(c, "timeout_s", 0)) for c in contract.checks)
    if per_check > suite_timeout_s:
        raise InvalidProposal([f"sum of per-check timeouts ({per_check}s) exceeds the suite timeout ({suite_timeout_s}s)"])
    benchmark = benchmark_id_for(run_id)
    if not BENCHMARK_ID.match(benchmark):
        raise InvalidProposal([f"bad benchmark id {benchmark!r}"])
    root = Path(acceptance_root).resolve()
    suite_dir = root / benchmark
    if suite_dir.exists():
        raise InvalidProposal([f"suite directory already exists: {suite_dir}"])
    suite_dir.mkdir(parents=True, exist_ok=False)
    (suite_dir / "contract.json").write_text(json.dumps(contract_doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    manifest = suite_manifest(contract_doc, timeout_s=suite_timeout_s)
    (suite_dir / "suite.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    from mas.verifier.acceptance import _hash_tree  # the verifier's own digest → what the run pins

    suite_sha = _hash_tree(suite_dir)
    sha = _sha(contract_doc)
    with conn.transaction():
        run = sm.lock_run(conn, run_id)
        if run.status is not RunStatus.AWAITING_INPUT:
            raise sm.IllegalTransition("run", run_id, run.status, RunStatus.PLANNING)
        conn.execute("UPDATE runs SET benchmark = %s WHERE id = %s", (benchmark, run_id))
        art = store.publish(
            conn,
            run_id=run_id,
            type="acceptance_contract",
            ref=f"acceptance_contract:{run_id}",
            meta={
                "benchmark": benchmark,
                "sha256": sha,
                "suite_sha256": suite_sha,
                "approved_by": approved_by,
                "edited": pending is not None and contract_doc != pending["proposal"].get("contract"),
                "requirements": (proposal_meta.get("proposal") or {}).get("requirements", []),
                "exclusions": (proposal_meta.get("proposal") or {}).get("exclusions", []),
                "check_ids": list(contract.check_ids),
            },
        )
        store.accept(conn, art.id)
        emit(
            conn,
            run_id,
            "contract.approved",
            payload={
                "benchmark": benchmark,
                "sha256": sha,
                "suite_sha256": suite_sha,
                "by": approved_by,
                "checks": list(contract.check_ids),
            },
        )
        sm.transition_run(conn, run_id, RunStatus.PLANNING, payload={"contract": sha[:12]})
    return Frozen(benchmark=benchmark, suite_dir=suite_dir, sha256=sha, suite_sha256=suite_sha)


def _sha(doc: Any) -> str:
    return hashlib.sha256(json.dumps(doc, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
