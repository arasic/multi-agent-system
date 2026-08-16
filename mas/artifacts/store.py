"""Artifact store. Content is immutable (DB trigger enforces); only status/superseded_by change, via the state machine."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from mas.db.connection import Conn, Jsonb
from mas.db.events import emit
from mas.models.enums import ArtifactStatus
from mas.models.types import Artifact
from mas.orchestrator import state_machine as sm


def publish(
    conn: Conn,
    *,
    run_id: UUID,
    type: str,
    ref: str,
    task_id: UUID | None = None,
    attempt_id: UUID | None = None,
    meta: dict[str, Any] | None = None,
) -> Artifact:
    row = conn.execute(
        """
        INSERT INTO artifacts (run_id, task_id, attempt_id, type, ref, meta)
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING *
        """,
        (run_id, task_id, attempt_id, type, ref, Jsonb(meta or {})),
    ).fetchone()
    assert row is not None
    emit(
        conn,
        run_id,
        "artifact.published",
        task_id=task_id,
        attempt_id=attempt_id,
        payload={"artifact_id": str(row["id"]), "type": type, "ref": ref},
    )
    return Artifact.from_row(row)


def accept(conn: Conn, artifact_id: UUID) -> Artifact:
    return sm.transition_artifact(conn, artifact_id, ArtifactStatus.ACCEPTED)


def reject(conn: Conn, artifact_id: UUID, *, reason: str | None = None) -> Artifact:
    return sm.transition_artifact(conn, artifact_id, ArtifactStatus.REJECTED, payload={"reason": reason})


def supersede(conn: Conn, old_id: UUID, new_id: UUID) -> Artifact:
    return sm.transition_artifact(conn, old_id, ArtifactStatus.SUPERSEDED, superseded_by=new_id)


def for_task(conn: Conn, task_id: UUID, *, statuses: list[ArtifactStatus] | None = None) -> list[Artifact]:
    if statuses:
        rows = conn.execute(
            "SELECT * FROM artifacts WHERE task_id = %s AND status = ANY(%s) ORDER BY created_at",
            (task_id, [s.value for s in statuses]),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM artifacts WHERE task_id = %s ORDER BY created_at", (task_id,)).fetchall()
    return [Artifact.from_row(r) for r in rows]


def for_attempt(conn: Conn, attempt_id: UUID) -> list[Artifact]:
    rows = conn.execute("SELECT * FROM artifacts WHERE attempt_id = %s ORDER BY created_at", (attempt_id,)).fetchall()
    return [Artifact.from_row(r) for r in rows]


def outputs_of_task(conn: Conn, task_id: UUID) -> list[Artifact]:
    """The task's outputs: candidate/accepted artifacts published by its SUCCESS attempt(s).

    Artifacts from FAILED/TIMEOUT/ABANDONED/CANCELLED attempts are hints, never outputs (ADR-002).
    Used for downstream context and for what the verifier accepts on PASS.
    """
    rows = conn.execute(
        """
        SELECT ar.* FROM artifacts ar
        JOIN attempts at ON at.id = ar.attempt_id
        WHERE ar.task_id = %s AND at.status = 'SUCCESS' AND ar.status IN ('candidate','accepted')
        ORDER BY ar.created_at
        """,
        (task_id,),
    ).fetchall()
    return [Artifact.from_row(r) for r in rows]


def outputs_of_dependencies(conn: Conn, task_id: UUID) -> list[Artifact]:
    """Outputs (see outputs_of_task) of every task this task depends on."""
    rows = conn.execute(
        """
        SELECT ar.* FROM task_dependencies d
        JOIN artifacts ar ON ar.task_id = d.depends_on_task_id
        JOIN attempts at ON at.id = ar.attempt_id
        WHERE d.task_id = %s AND at.status = 'SUCCESS' AND ar.status IN ('candidate','accepted')
        ORDER BY ar.created_at
        """,
        (task_id,),
    ).fetchall()
    return [Artifact.from_row(r) for r in rows]


def accepted_for_run(conn: Conn, run_id: UUID) -> list[Artifact]:
    rows = conn.execute(
        "SELECT * FROM artifacts WHERE run_id = %s AND status = 'accepted' ORDER BY created_at", (run_id,)
    ).fetchall()
    return [Artifact.from_row(r) for r in rows]
