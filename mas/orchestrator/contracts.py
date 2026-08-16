"""Output-contract check: did the attempt publish what the task promised?

Contract shape (planner/hand-written DAG):
    "output_contract": {"artifacts": ["git_commit", "document:design.md"]}
Each entry is "<type>" or "<type>:<name>". A name matches artifact.meta["name"] or a ref suffix.
"""

from __future__ import annotations

from uuid import UUID

from mas.artifacts import store
from mas.db.connection import Conn
from mas.models.types import Task


def required_artifacts(task: Task) -> list[tuple[str, str | None]]:
    out: list[tuple[str, str | None]] = []
    for entry in task.output_contract.get("artifacts", []) or []:
        if not isinstance(entry, str) or not entry:
            continue
        typ, _, name = entry.partition(":")
        out.append((typ, name or None))
    return out


def missing_outputs(conn: Conn, task: Task, attempt_id: UUID) -> list[str]:
    published = store.for_attempt(conn, attempt_id)
    missing: list[str] = []
    for typ, name in required_artifacts(task):
        ok = any(a.type == typ and (name is None or a.meta.get("name") == name or a.ref.endswith(name)) for a in published)
        if not ok:
            missing.append(f"{typ}:{name}" if name else typ)
    return missing
