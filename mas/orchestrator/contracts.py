"""Output-contract check: did the attempt publish what the task promised?

Contract shape (planner/hand-written DAG):
    "output_contract": {"artifacts": ["git_commit", "document:design.md"]}
Each entry is "<type>" or "<type>:<name>". A name matches artifact.meta["name"] or a ref suffix.
"""

from __future__ import annotations

from uuid import UUID

from mas.artifacts import store
from mas.db.connection import Conn
from mas.models.types import Artifact, Task


def required_artifacts(task: Task) -> list[tuple[str, str | None]]:
    out: list[tuple[str, str | None]] = []
    for entry in task.output_contract.get("artifacts", []) or []:
        if not isinstance(entry, str) or not entry:
            continue
        typ, _, name = entry.partition(":")
        out.append((typ, name or None))
    return out


def missing_outputs(conn: Conn, task: Task, attempt_id: UUID, *, competing: dict[str, list[UUID]] | None = None) -> list[str]:
    """What the attempt promised but did not publish: the output contract's artifacts, and — when its inputs held
    competing candidates for the same slot (A7) — one `decision` artifact per competing slot."""
    published = store.for_attempt(conn, attempt_id)
    missing: list[str] = []
    for typ, name in required_artifacts(task):
        ok = any(a.type == typ and (name is None or a.meta.get("name") == name or a.ref.endswith(name)) for a in published)
        if not ok:
            missing.append(f"{typ}:{name}" if name else typ)
    for slot in sorted(competing or {}):
        if not any(a.type == DECISION_TYPE and a.meta.get("slot") == slot for a in published):
            missing.append(f"decision:{slot}")
    return missing


DECISION_TYPE = "decision"
_SLOTLESS = frozenset({"git_commit", DECISION_TYPE, "verification", "log", "plan", "assumptions", "question", "answer"})


def slot_of(a: Artifact) -> str | None:
    """The output slot an artifact occupies — `<type>:<name>` for named file artifacts (documents and the like);
    None for commits, decisions and other run-level records (those never compete by name: merges handle commits)."""
    if a.type in _SLOTLESS:
        return None
    name = a.meta.get("name") if isinstance(a.meta, dict) else None
    if not name:
        ref = a.ref
        if ":" in ref:
            ref = ref.split(":", 1)[1]
        name = ref.rsplit("/", 1)[-1] if ref else None
    return f"{a.type}:{name}" if name else None


def competing_inputs(inputs: list[Artifact]) -> dict[str, list[Artifact]]:
    """A7 conflict representation (architecture §6): two candidate artifacts from *different* tasks for the same slot
    ARE a conflict. Returns slot -> the competing artifacts (input order). The consuming task's agent must publish a
    `decision` (winner, losers, rationale) for every slot; the runtime applies it — winner accepted, losers superseded."""
    groups: dict[str, list[Artifact]] = {}
    for a in inputs:
        slot = slot_of(a)
        if slot is None:
            continue
        groups.setdefault(slot, []).append(a)
    return {slot: arts for slot, arts in groups.items() if len({a.task_id for a in arts}) > 1}
