"""Execution runner — the trusted host-side service that executes workers' command requests in per-attempt sandboxes
(`mas execute --watch`). Same shape as the verifier service: a process WITH Docker, claiming work from Postgres.

Rules (all enforced here, never trusted from the worker):
- a request is served only for an attempt that exists and is RUNNING; the worktree is derived from the ids
  (`worktree_root/<run>/<task>-<attempt_number>`), never accepted from the request;
- the requested tool family must be in the task's persisted allow-list (`tasks.tools`, validator rule 4);
- command / argv sizes and timeouts are capped; the timeout is clamped to the attempt's remaining runtime;
- each request is leased (SKIP LOCKED claim + heartbeat); an expired lease is reaped as `abandoned` with a typed
  error — an interrupted command is NEVER replayed (its side effects are unknown);
- one sandbox session per attempt, owned by one runner at a time (`exec_sessions` lease); a takeover after the owner's
  lease expired starts a fresh container (same name: `docker rm -f` first). Sessions close when the attempt settles,
  expires, is cancelled, or the worker asks (`close` request);
- cancellation: the worker marking the request `cancelled`, or the attempt leaving RUNNING, kills the command;
- persisted per request: bounded status/timing/flags/exit code + output SHA-256 and size; the capped output text is
  in transit only (cleared by the worker once consumed);
- several runners may run at once; two never execute the same request or own the same attempt.
"""

from __future__ import annotations

import hashlib
import json
import logging
import socket
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from mas.db.connection import Conn, Jsonb, connect
from mas.workers.execution import CommandResult, ExecutionError, SandboxExecutionBackend, SandboxSpec

log = logging.getLogger(__name__)

MAX_COMMAND_CHARS = 16_000
MAX_ARGV_ITEMS = 64
MAX_TIMEOUT_S = 900.0
MAX_RESULT_OUTPUT_BYTES = 64_000
ALLOWED_FAMILIES = ("shell", "python")


@dataclass
class _Session:
    attempt_id: UUID
    backend: Any
    worktree: Path
    created: float = field(default_factory=time.monotonic)


class ExecRunner:
    def __init__(
        self,
        database_url: str | None,
        *,
        worktree_root: Path,
        spec: SandboxSpec | None = None,
        runner_id: str | None = None,
        backend_factory: Any = None,  # (worktree: Path, attempt_id: UUID) -> ExecutionBackend; default: sandbox from `spec`
        max_parallel: int = 4,
        lease_s: float = 10.0,
        tick_s: float = 0.2,
        max_result_output_bytes: int = MAX_RESULT_OUTPUT_BYTES,
    ):
        self.database_url = database_url
        self.worktree_root = Path(worktree_root).resolve()
        self.spec = spec or SandboxSpec()
        self.runner_id = runner_id or f"exec-{socket.gethostname()}-{id(self) & 0xFFFF:x}"
        self.backend_factory = backend_factory or (lambda wt, aid: SandboxExecutionBackend(wt, attempt_id=aid, spec=self.spec))
        self.max_parallel = max(1, int(max_parallel))
        self.lease_s = float(lease_s)
        self.tick_s = float(tick_s)
        self.max_result_output_bytes = int(max_result_output_bytes)
        self.sessions: dict[UUID, _Session] = {}
        self._in_flight: dict[int, Future] = {}
        self._active_ids: set[int] = set()
        self._lock = threading.Lock()
        self.stats = {"claimed": 0, "done": 0, "error": 0, "cancelled": 0, "abandoned": 0, "closed_sessions": 0}

    # ------------------------------------------------------------------ helpers

    def worktree_for(self, run_id: UUID, task_id: UUID, attempt_number: int) -> Path:
        """Derived from ids only — the same scheme as GitWorkspace.worktree_path; a worker cannot point us anywhere else."""
        return self.worktree_root / str(run_id) / f"{task_id}-{attempt_number}"

    def _lease_until(self) -> datetime:
        return datetime.now(UTC) + timedelta(seconds=self.lease_s)

    # ------------------------------------------------------------------ claiming (one transaction, session ownership)

    def claim(self, conn: Conn, limit: int = 8) -> list[dict[str, Any]]:
        claimed: list[dict[str, Any]] = []
        with conn.transaction():
            rows = conn.execute(
                "SELECT * FROM exec_requests WHERE status = 'pending' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT %s", (limit,)
            ).fetchall()
            for r in rows:
                if len(claimed) >= limit:
                    break
                own = conn.execute(
                    """
                    INSERT INTO exec_sessions (attempt_id, runner_id, lease_until)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (attempt_id) DO UPDATE
                        SET runner_id = EXCLUDED.runner_id, lease_until = EXCLUDED.lease_until, updated_at = now()
                        WHERE exec_sessions.runner_id = EXCLUDED.runner_id OR exec_sessions.lease_until < now()
                    RETURNING runner_id
                    """,
                    (r["attempt_id"], self.runner_id, self._lease_until()),
                ).fetchone()
                if own is None:
                    continue  # another live runner owns this attempt's session — leave its requests to it
                conn.execute(
                    "UPDATE exec_requests SET status = 'leased', runner_id = %s, lease_until = %s, started_at = now() "
                    "WHERE id = %s",
                    (self.runner_id, self._lease_until(), r["id"]),
                )
                claimed.append(dict(r))
        self.stats["claimed"] += len(claimed)
        return claimed

    # ------------------------------------------------------------------ execution of one request

    def execute(self, conn: Conn, req: dict[str, Any]) -> None:
        req_id = int(req["id"])
        t0 = time.monotonic()
        try:
            res = self._execute(conn, req)
        except Exception as e:  # noqa: BLE001 - never leave a leased request behind
            log.exception("exec runner: request %s crashed", req_id)
            res = CommandResult(
                exit_code=None, output="", duration_s=time.monotonic() - t0, error=f"runner error: {type(e).__name__}: {e}"[:500]
            )
        self._finish(conn, req_id, res)

    def _finish(self, conn: Conn, req_id: int, res: CommandResult) -> None:
        out = res.output or ""
        digest = hashlib.sha256(out.encode("utf-8", "replace")).hexdigest() if out else None
        if len(out.encode("utf-8", "replace")) > self.max_result_output_bytes:
            out = (
                out.encode("utf-8", "replace")[: self.max_result_output_bytes].decode("utf-8", "replace")
                + "\n[output truncated by runner]"
            )
            res.truncated = True
            digest = hashlib.sha256(out.encode("utf-8", "replace")).hexdigest()
        result = res.as_dict()
        result["output_sha256"] = digest
        result["output_bytes"] = len(out.encode("utf-8", "replace"))
        if res.error and not (res.cancelled or res.abandoned):
            status = "error"
        elif res.cancelled:
            status = "cancelled"
        else:
            status = "done"
        cp = conn.execute(
            """
            UPDATE exec_requests SET status = %s, result = %s, output = %s, finished_at = now(), lease_until = NULL
            WHERE id = %s AND status = 'leased' AND runner_id = %s
            """,
            (status, Jsonb(result), out, req_id, self.runner_id),
        )
        if cp.rowcount == 0:
            log.warning("exec runner: request %s was no longer ours when it finished (cancelled or reaped)", req_id)
        else:
            self.stats[status] = self.stats.get(status, 0) + 1

    def _execute(self, conn: Conn, req: dict[str, Any]) -> CommandResult:
        t0 = time.monotonic()
        attempt_id: UUID = req["attempt_id"]
        kind = req["kind"]

        # 1. the attempt must exist and be RUNNING; task/run give us the allow-list, the worktree and the deadline
        row = conn.execute(
            """
            SELECT a.status AS a_status, a.attempt_number, a.started_at, t.id AS task_id, t.run_id, t.tools,
                   r.max_attempt_runtime_s
            FROM attempts a JOIN tasks t ON t.id = a.task_id JOIN runs r ON r.id = t.run_id
            WHERE a.id = %s
            """,
            (attempt_id,),
        ).fetchone()
        if row is None:
            return CommandResult(exit_code=None, output="", duration_s=0.0, error="unknown attempt")
        if kind == "close":
            self._close_session(attempt_id)
            return CommandResult(exit_code=0, output="session closed", duration_s=time.monotonic() - t0)
        if row["a_status"] != "RUNNING":
            self._close_session(attempt_id)
            return CommandResult(exit_code=None, output="", duration_s=0.0, error=f"attempt is {row['a_status']}, not RUNNING")
        if row["task_id"] != req["task_id"] or row["run_id"] != req["run_id"]:
            return CommandResult(exit_code=None, output="", duration_s=0.0, error="request ids do not match the attempt")

        # 2. the family must be granted to the task
        family = str(req["family"])
        granted = set(row["tools"] or [])
        if family not in ALLOWED_FAMILIES or family not in granted:
            return CommandResult(
                exit_code=None, output="", duration_s=0.0, error=f"tool family {family!r} is not granted to this task"
            )

        # 3. bounded command
        argv: list[str] | None = None
        command: str | None = None
        if kind == "argv":
            argv = [str(a) for a in (req["argv"] or [])]
            if not argv or len(argv) > MAX_ARGV_ITEMS or sum(len(a) for a in argv) > MAX_COMMAND_CHARS:
                return CommandResult(exit_code=None, output="", duration_s=0.0, error="argv rejected (empty or too large)")
            if family == "python" and argv[0] != "python":
                return CommandResult(exit_code=None, output="", duration_s=0.0, error="python family requests must run python")
        elif kind == "shell":
            command = str(req["command"] or "")
            if not command.strip() or len(command) > MAX_COMMAND_CHARS:
                return CommandResult(exit_code=None, output="", duration_s=0.0, error="command rejected (empty or too large)")
        else:
            return CommandResult(exit_code=None, output="", duration_s=0.0, error=f"unknown request kind {kind!r}")

        # 4. timeout: the worker's request, capped, then clamped to the attempt's remaining runtime
        try:
            timeout_s = float(req["timeout_s"])
        except (TypeError, ValueError):
            timeout_s = 60.0
        if not (timeout_s > 0) or timeout_s != timeout_s:  # NaN / non-positive
            timeout_s = 60.0
        timeout_s = min(timeout_s, MAX_TIMEOUT_S)
        if row["started_at"] is not None:
            deadline = row["started_at"] + timedelta(seconds=float(row["max_attempt_runtime_s"]))
            remaining = (deadline - datetime.now(UTC)).total_seconds()
            if remaining <= 0.5:
                return CommandResult(exit_code=None, output="", duration_s=0.0, error="attempt runtime exhausted")
            timeout_s = min(timeout_s, remaining)

        # 5. the session's sandbox for this attempt (worktree derived from ids; must exist)
        session = self._session(attempt_id, row["run_id"], row["task_id"], int(row["attempt_number"]))
        if session is None:
            return CommandResult(
                exit_code=None, output="", duration_s=0.0, error="worktree for this attempt does not exist on the runner host"
            )

        # 6. run, with a monitor that cancels when the worker cancels the request or the attempt stops being RUNNING
        cancel = threading.Event()
        stop_monitor = threading.Event()
        monitor = threading.Thread(
            target=self._monitor,
            args=(int(req["id"]), attempt_id, cancel, stop_monitor),
            name=f"exec-monitor-{req['id']}",
            daemon=True,
        )
        monitor.start()
        try:
            if kind == "argv":
                res = session.backend.run(argv or [], timeout_s=timeout_s, cancel=cancel)
            else:
                res = session.backend.run_shell(command or "", timeout_s=timeout_s, cancel=cancel)
        except ExecutionError as e:
            res = CommandResult(exit_code=None, output="", duration_s=time.monotonic() - t0, error=f"sandbox: {e}"[:500])
        finally:
            stop_monitor.set()
            monitor.join(2)
        self._record_session(conn, attempt_id, session)
        return res

    def _monitor(self, req_id: int, attempt_id: UUID, cancel: threading.Event, stop: threading.Event) -> None:
        conn = connect(self.database_url)
        try:
            while not stop.wait(0.5):
                try:
                    r = conn.execute("SELECT status FROM exec_requests WHERE id = %s", (req_id,)).fetchone()
                    a = conn.execute("SELECT status FROM attempts WHERE id = %s", (attempt_id,)).fetchone()
                except Exception:
                    log.debug("exec monitor query failed", exc_info=True)
                    conn.rollback()
                    continue
                if r is None or r["status"] != "leased" or a is None or a["status"] != "RUNNING":
                    cancel.set()
                    return
        finally:
            conn.close()

    # ------------------------------------------------------------------ sessions

    def _session(self, attempt_id: UUID, run_id: UUID, task_id: UUID, attempt_number: int) -> _Session | None:
        with self._lock:
            s = self.sessions.get(attempt_id)
            if s is not None:
                return s
            wt = self.worktree_for(run_id, task_id, attempt_number)
            if not wt.is_dir():
                return None
            backend = self.backend_factory(wt, attempt_id)
            s = _Session(attempt_id=attempt_id, backend=backend, worktree=wt)
            self.sessions[attempt_id] = s
            return s

    def _record_session(self, conn: Conn, attempt_id: UUID, session: _Session) -> None:
        ident = getattr(session.backend, "identity", None)
        info = ident() if callable(ident) else {}
        try:
            conn.execute(
                "UPDATE exec_sessions SET container = %s, image_id = %s, updated_at = now() "
                "WHERE attempt_id = %s AND runner_id = %s",
                (info.get("container"), info.get("image_id"), attempt_id, self.runner_id),
            )
        except Exception:
            log.debug("exec runner: session record failed", exc_info=True)

    def _close_session(self, attempt_id: UUID) -> None:
        with self._lock:
            s = self.sessions.pop(attempt_id, None)
        if s is not None:
            try:
                s.backend.close()
            except Exception:
                log.warning("exec runner: closing sandbox for %s failed", attempt_id, exc_info=True)
            self.stats["closed_sessions"] += 1

    def gc_sessions(self, conn: Conn) -> None:
        """Close sandboxes whose attempt is no longer RUNNING, and sessions we no longer own (lease taken over)."""
        with self._lock:
            ids = list(self.sessions)
        if not ids:
            return
        rows = conn.execute(
            """
            SELECT a.id, a.status, s.runner_id AS owner
            FROM attempts a LEFT JOIN exec_sessions s ON s.attempt_id = a.id
            WHERE a.id = ANY(%s)
            """,
            (ids,),
        ).fetchall()
        known = {r["id"] for r in rows}
        for r in rows:
            if r["status"] != "RUNNING" or r["owner"] != self.runner_id:
                self._close_session(r["id"])
                conn.execute("DELETE FROM exec_sessions WHERE attempt_id = %s AND runner_id = %s", (r["id"], self.runner_id))
        for aid in ids:
            if aid not in known:  # attempt row gone (run deleted)
                self._close_session(aid)

    # ------------------------------------------------------------------ leases and reaping

    def heartbeat(self, conn: Conn) -> None:
        with self._lock:
            active = list(self._active_ids)
            attempts = list(self.sessions)
        until = self._lease_until()
        if active:
            conn.execute(
                "UPDATE exec_requests SET lease_until = %s WHERE id = ANY(%s) AND status = 'leased' AND runner_id = %s",
                (until, active, self.runner_id),
            )
        if attempts:
            conn.execute(
                "UPDATE exec_sessions SET lease_until = %s, updated_at = now() WHERE attempt_id = ANY(%s) AND runner_id = %s",
                (until, attempts, self.runner_id),
            )

    def reap(self, conn: Conn) -> int:
        """Requests whose runner stopped heart-beating: abandoned, with a typed result. Never replayed."""
        result = {
            "exit_code": None,
            "duration_s": 0.0,
            "timed_out": False,
            "truncated": False,
            "cancelled": False,
            "abandoned": True,
            "error": "execution runner died mid-command; the command was not replayed (side effects unknown)",
            "output_sha256": None,
            "output_bytes": 0,
        }
        cp = conn.execute(
            """
            UPDATE exec_requests SET status = 'abandoned', result = %s, finished_at = now(), lease_until = NULL
            WHERE status = 'leased' AND lease_until IS NOT NULL AND lease_until < now()
            """,
            (Jsonb(result),),
        )
        n = cp.rowcount or 0
        if n:
            self.stats["abandoned"] += n
            log.warning("exec runner: reaped %d abandoned request(s)", n)
        # sessions of dead runners: rows only (their containers end by themselves: --rm + max_life_s, and a takeover
        # does `docker rm -f` on the same name first)
        conn.execute("DELETE FROM exec_sessions s WHERE s.lease_until < now() - interval '60 seconds'")
        return n

    # ------------------------------------------------------------------ loops

    def run_once(self, conn: Conn) -> int:
        """Reap, GC, then execute everything currently pending, synchronously (tests / `mas execute --once`)."""
        self.reap(conn)
        self.gc_sessions(conn)
        n = 0
        while True:
            batch = self.claim(conn, limit=8)
            if not batch:
                break
            for req in batch:
                self.execute(conn, req)
                n += 1
        self.gc_sessions(conn)
        return n

    def serve_forever(self, stop: threading.Event | None = None) -> None:
        stop = stop or threading.Event()
        conn = connect(self.database_url)
        hb_stop = threading.Event()

        def _hb() -> None:
            c = connect(self.database_url)
            try:
                while not hb_stop.wait(max(0.2, self.lease_s / 3)):
                    try:
                        self.heartbeat(c)
                    except Exception:
                        log.exception("exec runner heartbeat failed")
                        c.rollback()
            finally:
                c.close()

        hb = threading.Thread(target=_hb, name="exec-heartbeat", daemon=True)
        hb.start()
        try:
            with ThreadPoolExecutor(max_workers=self.max_parallel, thread_name_prefix="exec") as ex:
                while not stop.is_set():
                    for rid, fut in list(self._in_flight.items()):
                        if fut.done():
                            self._in_flight.pop(rid)
                            with self._lock:
                                self._active_ids.discard(rid)
                            if fut.exception() is not None:
                                log.error("exec runner: job %s failed: %r", rid, fut.exception())
                    try:
                        self.reap(conn)
                        self.gc_sessions(conn)
                        free = self.max_parallel - len(self._in_flight)
                        batch = self.claim(conn, limit=free) if free > 0 else []
                    except Exception:
                        log.exception("exec runner: tick failed")
                        conn.rollback()
                        batch = []
                    for req in batch:
                        rid = int(req["id"])
                        with self._lock:
                            self._active_ids.add(rid)
                        self._in_flight[rid] = ex.submit(self._job, req)
                    stop.wait(self.tick_s)
                for fut in self._in_flight.values():
                    try:
                        fut.result(timeout=MAX_TIMEOUT_S)
                    except Exception:
                        log.debug("exec runner: in-flight job failed during drain", exc_info=True)
        finally:
            hb_stop.set()
            hb.join(5)
            for aid in list(self.sessions):
                self._close_session(aid)
            conn.close()

    def _job(self, req: dict[str, Any]) -> None:
        conn = connect(self.database_url)
        try:
            self.execute(conn, req)
        finally:
            conn.close()

    def close_all(self) -> None:
        for aid in list(self.sessions):
            self._close_session(aid)


__all__ = ["ExecRunner", "MAX_COMMAND_CHARS", "MAX_ARGV_ITEMS", "MAX_TIMEOUT_S", "json"]
