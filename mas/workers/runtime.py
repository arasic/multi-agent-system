"""Worker loop (docs/architecture.md §5).

    loop:
        claim READY task  →  attempt + lease
        workspace          (step 6)
        scoped context     (dependency outputs only)
        heartbeat thread   (extends lease; detects reaping/cancel → cancel event)
        agent.execute
        publish artifacts  →  report result   (orchestrator transitions the task)

The worker never decides its task's outcome. It reports; the state machine decides. A stale report
(attempt already reaped/cancelled) is rejected and logged — the classic zombie-worker case.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from mas.artifacts import store
from mas.db.connection import Conn, connect
from mas.orchestrator import leases
from mas.orchestrator.leases import Claim, StaleAttempt
from mas.workers.base import Agent, AgentResult, TaskContext
from mas.workers.workspace import NullWorkspace, Workspace

log = logging.getLogger(__name__)


class _Heartbeat(threading.Thread):
    def __init__(self, database_url: str | None, attempt_id: UUID, lease_s: int, cancel: threading.Event):
        super().__init__(name=f"heartbeat-{attempt_id}", daemon=True)
        self.database_url = database_url
        self.attempt_id = attempt_id
        self.lease_s = lease_s
        self.cancel = cancel
        self._stop = threading.Event()
        self.beats = 0

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        interval = max(0.05, self.lease_s / 3)
        try:
            conn = connect(self.database_url)
        except Exception:
            log.exception("heartbeat: cannot connect")
            self.cancel.set()
            return
        try:
            while not self._stop.wait(interval):
                try:
                    alive = leases.heartbeat(conn, self.attempt_id, self.lease_s)
                except Exception:
                    log.exception("heartbeat failed")
                    conn.rollback()
                    continue
                self.beats += 1
                if not alive:
                    log.warning("attempt %s no longer RUNNING — signalling cancel", self.attempt_id)
                    self.cancel.set()
                    return
        finally:
            conn.close()


@dataclass
class WorkerStats:
    claimed: int = 0
    completed: int = 0
    failed: int = 0
    stale: int = 0
    died: int = 0
    tasks: list[str] = field(default_factory=list)


class Worker:
    def __init__(
        self,
        worker_id: str,
        capabilities: list[str] | tuple[str, ...],
        agent: Agent,
        *,
        database_url: str | None = None,
        poll_s: float = 0.2,
        lease_s: int | None = None,
        run_id: UUID | None = None,
        pools: list[str] | tuple[str, ...] | None = None,
        workspace: Workspace | None = None,
    ):
        self.worker_id = worker_id
        self.capabilities = list(capabilities)
        self.agent = agent
        self.database_url = database_url
        self.poll_s = poll_s
        self.lease_s = lease_s
        self.run_id = run_id  # pin to one run (in-process `mas run`)
        self.pools = list(pools) if pools is not None else None  # serve only these pools (compose services)
        self.workspace = workspace or NullWorkspace()
        self.stats = WorkerStats()
        self._conn: Conn | None = None
        self.dead = threading.Event()  # set by die(): simulate a crash — stop everything, report nothing
        self.current: Claim | None = None  # the claim being processed right now (None when idle)

    # ------------------------------------------------------------------ lifecycle

    @property
    def busy(self) -> bool:
        return self.current is not None

    @property
    def conn(self) -> Conn:
        if self._conn is None or self._conn.closed:
            self._conn = connect(self.database_url)
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()

    def die(self) -> None:
        """Simulate a crash mid-attempt: heartbeat stops, nothing is reported. The reaper must recover the task."""
        self.dead.set()

    def run_forever(self, stop: threading.Event | None = None) -> None:
        stop = stop or threading.Event()
        try:
            while not stop.is_set() and not self.dead.is_set():
                try:
                    did = self.run_once()
                except Exception:
                    log.exception("worker %s loop error", self.worker_id)
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                    did = False
                if not did:
                    stop.wait(self.poll_s)
        finally:
            self.close()

    # ------------------------------------------------------------------ one task

    def run_once(self) -> bool:
        claim = leases.claim_task(
            self.conn,
            worker_id=self.worker_id,
            capabilities=self.capabilities,
            lease_s=self.lease_s,
            run_id=self.run_id,
            pools=self.pools,
        )
        if claim is None:
            return False
        self.stats.claimed += 1
        self.stats.tasks.append(claim.task.key)
        self.current = claim
        try:
            self._process(claim)
        finally:
            self.current = None
        return True

    def _process(self, claim: Claim) -> None:
        lease_s = self.lease_s if self.lease_s is not None else claim.run.budgets.lease_s
        cancel = threading.Event()
        hb = _Heartbeat(self.database_url, claim.attempt.id, lease_s, cancel)
        hb.start()
        ws_path = None
        try:
            ws_path = self.workspace.create(claim.run.id, claim.task.key, claim.attempt.attempt_number)
            inputs = _dependency_outputs(self.conn, claim.task.id)
            ctx = TaskContext(
                run=claim.run, task=claim.task, attempt=claim.attempt, inputs=inputs, workspace=ws_path, cancel=cancel
            )
            try:
                result = self.agent.execute(ctx)
            except Exception as e:  # agent bug → FAILED attempt, never a hung task
                log.exception("agent crashed on %s", claim.task.key)
                result = AgentResult(success=False, failure_reason=f"agent crashed: {e!r}")
        finally:
            hb.stop()

        if result.simulate_death or self.dead.is_set():
            # crash simulation: walk away. Lease expires → reaper → ABANDONED → task READY again.
            self.stats.died += 1
            self.dead.set()
            log.warning("worker %s simulating death on %s#%s", self.worker_id, claim.task.key, claim.attempt.attempt_number)
            return

        self._publish_and_report(claim, result)
        self.workspace.cleanup(ws_path)

    def _publish_and_report(self, claim: Claim, result: AgentResult) -> None:
        conn = self.conn
        try:
            with conn.transaction():
                for a in result.artifacts:
                    store.publish(
                        conn,
                        run_id=claim.run.id,
                        task_id=claim.task.id,
                        attempt_id=claim.attempt.id,
                        type=a.type,
                        ref=a.ref,
                        meta=a.meta,
                    )
            task = leases.report(
                conn,
                claim.attempt.id,
                success=result.success,
                failure_reason=result.failure_reason,
                usage=result.usage or None,
                new_work_required=result.new_work_required,
            )
        except StaleAttempt as e:
            self.stats.stale += 1
            log.warning("worker %s: %s", self.worker_id, e)
            return
        if task.status.value == "COMPLETED":
            self.stats.completed += 1
        else:
            self.stats.failed += 1


def _dependency_outputs(conn: Conn, task_id: UUID) -> list[Any]:
    """Outputs of upstream tasks (store.outputs_of_dependencies): SUCCESS attempts' candidate/accepted artifacts.

    Later (step 6+) context_spec narrows this further (specific artifacts / paths). Never the whole run.
    """
    return store.outputs_of_dependencies(conn, task_id)


def run_worker_thread(worker: Worker, stop: threading.Event) -> threading.Thread:
    t = threading.Thread(target=worker.run_forever, args=(stop,), name=worker.worker_id, daemon=True)
    t.start()
    return t


def wait_all(threads: list[threading.Thread], timeout: float | None = None) -> None:
    deadline = None if timeout is None else time.monotonic() + timeout
    for t in threads:
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        t.join(remaining)
