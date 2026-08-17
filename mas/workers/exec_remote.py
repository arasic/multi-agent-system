"""RemoteExecutionBackend — the worker side of the execution-runner path (docs/architecture.md §10b).

    worker (no docker.sock)  ──INSERT exec_requests (ids + bounded command)──▶  Postgres  ◀──claim/lease/result── mas execute
                             ◀──poll status/result──────────────────────────

The worker never sends a path: only run/task/attempt ids, the tool family the command needs, and the bounded command.
The trusted runner (`mas/workers/exec_runner.py`) validates the attempt (must be RUNNING) and the family (against the
persisted task allow-list), derives the worktree from the ids itself, runs the command in the attempt's sandbox and
writes back a bounded result. Cancellation flows both ways: the worker's cancel event marks the request cancelled (the
runner kills the command); an attempt that stops being RUNNING makes the runner cancel and close the sandbox.

Typed outcomes the worker can see beyond a normal exit: TIMED_OUT (runner/container-side timeout), CANCELLED,
ABANDONED (the runner died mid-command; never replayed — side effects unknown), ERROR (refused/failed by the runner:
attempt not running, family not granted, oversize request, no runner picked it up in time, ...).
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from typing import Any
from uuid import UUID

from mas.db.connection import Conn, Jsonb, connect
from mas.workers.execution import CommandResult

log = logging.getLogger(__name__)

TERMINAL = ("done", "error", "cancelled", "abandoned")
MAX_COMMAND_CHARS = 16_000
MAX_ARGV_ITEMS = 64


class RemoteExecutionBackend:
    name = "sandbox-remote"
    confined = True  # the runner executes in a SandboxExecutionBackend; this side has no execution at all

    def __init__(
        self,
        database_url: str | None,
        *,
        run_id: UUID,
        task_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        poll_s: float = 0.2,
        pickup_grace_s: float = 30.0,  # how long a request may sit unclaimed before the worker gives up (no runner)
        finish_grace_s: float = 15.0,  # slack beyond timeout_s before a leased request is considered lost by the worker
    ):
        self.database_url = database_url
        self.run_id = run_id
        self.task_id = task_id
        self.attempt_id = attempt_id
        self.worker_id = worker_id
        self.poll_s = poll_s
        self.pickup_grace_s = pickup_grace_s
        self.finish_grace_s = finish_grace_s
        self._conn: Conn | None = None
        self._lock = threading.Lock()
        self.commands = 0
        self.session_info: dict[str, Any] = {}

    # ------------------------------------------------------------------ plumbing

    @property
    def conn(self) -> Conn:
        if self._conn is None or self._conn.closed:
            self._conn = connect(self.database_url)
        return self._conn

    def _submit(self, *, family: str, kind: str, command: str | None, argv: list[str] | None, timeout_s: float) -> int:
        row = self.conn.execute(
            """
            INSERT INTO exec_requests (run_id, task_id, attempt_id, worker_id, family, kind, command, argv, timeout_s)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """,
            (
                self.run_id,
                self.task_id,
                self.attempt_id,
                self.worker_id,
                family,
                kind,
                command,
                Jsonb(argv) if argv is not None else None,
                timeout_s,
            ),
        ).fetchone()
        assert row is not None
        return int(row["id"])

    def _wait(self, req_id: int, *, timeout_s: float, cancel: threading.Event | None) -> CommandResult:
        t0 = time.monotonic()
        while True:
            row = self.conn.execute(
                "SELECT status, result, output, started_at FROM exec_requests WHERE id = %s", (req_id,)
            ).fetchone()
            if row is None:
                return CommandResult(exit_code=None, output="", duration_s=time.monotonic() - t0, error="request vanished")
            status = row["status"]
            if status in TERMINAL:
                res = _from_row(status, row["result"], row["output"])
                self.conn.execute("UPDATE exec_requests SET output = NULL, consumed_at = now() WHERE id = %s", (req_id,))
                return res
            elapsed = time.monotonic() - t0
            if cancel is not None and cancel.is_set():
                self._cancel(req_id)
                return CommandResult(exit_code=None, output="", duration_s=elapsed, cancelled=True)
            if status == "pending" and elapsed > self.pickup_grace_s:
                self._cancel(req_id)
                return CommandResult(
                    exit_code=None, output="", duration_s=elapsed, error="no execution runner picked up the command in time"
                )
            if status == "leased" and elapsed > timeout_s + self.finish_grace_s + self.pickup_grace_s:
                self._cancel(req_id)
                return CommandResult(
                    exit_code=None, output="", duration_s=elapsed, error="execution runner did not report a result in time"
                )
            time.sleep(self.poll_s)

    def _cancel(self, req_id: int) -> None:
        try:
            self.conn.execute(
                "UPDATE exec_requests SET status = 'cancelled', finished_at = now() "
                "WHERE id = %s AND status IN ('pending', 'leased')",
                (req_id,),
            )
        except Exception:
            log.warning("remote exec: cancel of request %s failed", req_id, exc_info=True)

    # ------------------------------------------------------------------ ExecutionBackend

    def run(self, argv: list[str], *, timeout_s: float, cancel: threading.Event | None = None) -> CommandResult:
        argv = [str(a) for a in argv]
        if not argv or len(argv) > MAX_ARGV_ITEMS or sum(len(a) for a in argv) > MAX_COMMAND_CHARS:
            return CommandResult(exit_code=None, output="", duration_s=0.0, error="argv too large or empty")
        family = "python" if argv[0] == "python" else "shell"
        with self._lock:
            self.commands += 1
            req = self._submit(family=family, kind="argv", command=None, argv=argv, timeout_s=float(timeout_s))
        return self._wait(req, timeout_s=timeout_s, cancel=cancel)

    def run_shell(self, command: str, *, timeout_s: float, cancel: threading.Event | None = None) -> CommandResult:
        if not command or len(command) > MAX_COMMAND_CHARS:
            return CommandResult(exit_code=None, output="", duration_s=0.0, error="command too large or empty")
        with self._lock:
            self.commands += 1
            req = self._submit(family="shell", kind="shell", command=command, argv=None, timeout_s=float(timeout_s))
        return self._wait(req, timeout_s=timeout_s, cancel=cancel)

    def close(self) -> None:
        """Ask the runner to close this attempt's sandbox now (it would also be closed once the attempt settles)."""
        try:
            with self._lock:
                self._submit(family="shell", kind="close", command=None, argv=None, timeout_s=5.0)
        except Exception:
            log.debug("remote exec: close request failed", exc_info=True)
        finally:
            if self._conn is not None and not self._conn.closed:
                self._conn.close()

    def identity(self) -> dict[str, Any]:
        info: dict[str, Any] = {"backend": self.name, "commands": self.commands}
        try:
            row = self.conn.execute(
                "SELECT runner_id, container, image_id FROM exec_sessions WHERE attempt_id = %s", (self.attempt_id,)
            ).fetchone()
            if row:
                info.update(runner_id=row["runner_id"], container=row["container"], image_id=row["image_id"])
        except Exception:
            pass
        return info


def _from_row(status: str, result: dict[str, Any] | None, output: str | None) -> CommandResult:
    r = dict(result or {})
    out = output or ""
    if r.get("output_sha256") and out and hashlib.sha256(out.encode("utf-8", "replace")).hexdigest() != r["output_sha256"]:
        r["error"] = (r.get("error") or "") + " [output hash mismatch]"
    return CommandResult(
        exit_code=r.get("exit_code"),
        output=out,
        duration_s=float(r.get("duration_s") or 0.0),
        timed_out=bool(r.get("timed_out")),
        truncated=bool(r.get("truncated")),
        cancelled=bool(r.get("cancelled")) or status == "cancelled",
        abandoned=bool(r.get("abandoned")) or status == "abandoned",
        error=r.get("error") if r.get("error") else (None if status in ("done", "cancelled") else f"execution {status}"),
    )


__all__ = ["RemoteExecutionBackend", "TERMINAL", "MAX_COMMAND_CHARS", "MAX_ARGV_ITEMS", "json"]
