"""Append-only event log (invariant I-12). Emit inside the same transaction as the change it records."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from mas.db.connection import Conn, Jsonb
from mas.models.types import Event


def emit(
    conn: Conn,
    run_id: UUID,
    type: str,
    *,
    task_id: UUID | None = None,
    attempt_id: UUID | None = None,
    worker_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> int:
    row = conn.execute(
        """
        INSERT INTO events (run_id, task_id, attempt_id, worker_id, type, payload)
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """,
        (run_id, task_id, attempt_id, worker_id, type, Jsonb(payload or {})),
    ).fetchone()
    assert row is not None
    return int(row["id"])


def for_run(conn: Conn, run_id: UUID) -> list[Event]:
    rows = conn.execute("SELECT * FROM events WHERE run_id = %s ORDER BY id", (run_id,)).fetchall()
    return [Event.from_row(r) for r in rows]
