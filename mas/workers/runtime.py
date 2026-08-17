"""Worker loop (docs/architecture.md §5).

    loop:
        claim READY task  →  attempt + lease
        scoped inputs      (dependency outputs, narrowed by context_spec)
        workspace          (git worktree from base + input assembly; conflicts handed to the agent)
        heartbeat thread   (extends lease; detects reaping/cancel → cancel event)
        agent.execute
        commit worktree    →  git_commit artifact (+ path:→sha: refs)
        publish artifacts  →  report result   (orchestrator transitions the task)

The worker never decides its task's outcome. It reports; the state machine decides. A stale report
(attempt already reaped/cancelled) is rejected and logged — the classic zombie-worker case.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID

from mas.artifacts import store
from mas.config import settings
from mas.db.connection import Conn, connect
from mas.models.types import Task
from mas.orchestrator import leases
from mas.orchestrator.contracts import competing_inputs
from mas.orchestrator.leases import ArtifactSpec, Claim, StaleAttempt
from mas.providers.base import ModelProvider
from mas.providers.pricing import Pricing
from mas.providers.telemetry import CallBudget, DbSink, MeteredProvider
from mas.workers.base import Agent, AgentResult, ArtifactOut, TaskContext
from mas.workers.workspace import NullWorkspace, Workspace, WorkspaceError, WorkspaceHandle, since_for

log = logging.getLogger(__name__)


class _Heartbeat(threading.Thread):
    def __init__(self, database_url: str | None, attempt_id: UUID, lease_s: int, cancel: threading.Event):
        super().__init__(name=f"heartbeat-{attempt_id}", daemon=True)
        self.database_url = database_url
        self.attempt_id = attempt_id
        self.lease_s = lease_s
        self.cancel = cancel
        self._stop = threading.Event()
        self.settled = threading.Event()  # set by the worker right after the report commits
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
            # Renew immediately after establishing the dedicated connection. With short leases, waiting one full
            # interval before the first beat lets connection or OS scheduling latency consume most of the claim's
            # initial lease before workspace/sandbox setup even starts.
            while not self._stop.is_set():
                try:
                    alive = leases.heartbeat(conn, self.attempt_id, self.lease_s)
                except Exception:
                    log.exception("heartbeat failed")
                    conn.rollback()
                else:
                    self.beats += 1
                    if not alive:
                        # settled by our own worker a moment ago? then this is expected, not a reap/cancel
                        if self.settled.is_set() or self._stop.wait(0.2) or self.settled.is_set():
                            return
                        log.warning("attempt %s no longer RUNNING (reaped/cancelled) - signalling cancel", self.attempt_id)
                        self.cancel.set()
                        return
                if self._stop.wait(interval):
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
        provider: ModelProvider | None = None,
        pricing: Pricing | None = None,
        attempt_max_calls: int | None = None,
        attempt_max_tokens: int | None = None,
        exec_backend_factory: Callable[[Path, Claim], Any] | None = None,
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
        # step 9: the worker's model (None = stub agents). Per attempt it is wrapped in a MeteredProvider — telemetry
        # rows in model_calls, pricing from config, a hard per-attempt call/token budget — and handed over as ctx.model.
        self.provider = provider
        self.pricing = pricing if pricing is not None else Pricing.from_json(settings().model_prices)
        self.attempt_max_calls = attempt_max_calls if attempt_max_calls is not None else settings().attempt_max_calls
        # optional worker-side ceiling; the run's own per-attempt allocation (budgets.max_attempt_tokens) always applies
        self.attempt_max_tokens = attempt_max_tokens if attempt_max_tokens is not None else settings().attempt_max_tokens
        self._telemetry_lock = threading.Lock()
        # confined execution for command tools, one backend per attempt: (worktree, claim) -> ExecutionBackend.
        # The runtime creates it after the worktree exists and closes it in every exit path, so the sandbox never
        # outlives the attempt. None = the agent gets no command tools (stub agents; workers without Docker).
        self.exec_backend_factory = exec_backend_factory
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
            token_ceiling=self.attempt_max_tokens,  # optional worker-side ceiling; the run's allocation applies anyway
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

    def _metered(self, claim: Claim, *, cancel: threading.Event, deadline: float) -> MeteredProvider | None:
        """The model handed to the agent for this attempt: telemetry to model_calls on the worker's own connection
        (idle while the agent runs; the heartbeat has its own), priced from config, bounded by the attempt's **reserved
        token allocation** (claimed from the run's unreserved budget — concurrent attempts can never jointly exceed
        what the run has left, overshoot ≤ one call each) and by the attempt's deadline / cancel event: no model call —
        including provider retries — outlives the attempt."""
        if self.provider is None:
            return None
        alloc = claim.attempt.token_allocation
        if alloc is None:  # attempt claimed before migration 0008: fall back to the unreserved-at-claim view
            alloc = min(claim.run.budgets.max_attempt_tokens, max(0, claim.run.budgets.max_tokens - claim.run.tokens_used))
            if self.attempt_max_tokens is not None:
                alloc = min(alloc, self.attempt_max_tokens)
        budget = CallBudget(max_calls=self.attempt_max_calls, max_tokens=int(alloc))
        return MeteredProvider(
            self.provider,
            sink=DbSink(self.conn, self._telemetry_lock),
            pricing=self.pricing,
            role="worker",
            run_id=claim.run.id,
            task_id=claim.task.id,
            attempt_id=claim.attempt.id,
            budget=budget,
            deadline=deadline,
            cancel=cancel,
            call_timeout_s=settings().provider_timeout_s,
        )

    def _process(self, claim: Claim) -> None:
        """One attempt, end to end. The heartbeat runs until the attempt is SETTLED (report committed) — covering
        workspace creation, agent execution, git commit, artifact publication and settlement — so a slow publish can
        never be reaped as ABANDONED. Only a deliberate simulated death stops without reporting."""
        lease_s = self.lease_s if self.lease_s is not None else claim.run.budgets.lease_s
        cancel = threading.Event()
        # the attempt's runtime deadline (the reaper enforces the same limit from the DB side); model calls and tools
        # are clamped to it so nothing keeps working - or spending - after the orchestrator has given up on the attempt
        deadline = time.monotonic() + float(claim.run.budgets.max_attempt_runtime_s)
        hb = _Heartbeat(self.database_url, claim.attempt.id, lease_s, cancel)
        hb.start()
        handle: WorkspaceHandle | None = None
        died = False
        metered: MeteredProvider | None = None
        backend: Any = None
        try:
            inputs = _dependency_outputs(self.conn, claim.task)
            competing = competing_inputs(inputs)  # A7: same slot from different tasks → the agent must decide
            metered = self._metered(claim, cancel=cancel, deadline=deadline)
            try:
                handle = self.workspace.create(claim.run, claim.task, claim.attempt, inputs)
            except WorkspaceError as e:  # cannot even build the workspace → failed attempt, with the reason
                log.exception("workspace creation failed for %s", claim.task.key)
                result = AgentResult(success=False, failure_reason=f"workspace: {e}")
            else:
                if handle is not None and self.exec_backend_factory is not None:
                    try:
                        backend = self.exec_backend_factory(handle.path, claim)
                    except Exception:  # no confined execution → no command tools (fail closed), the attempt still runs
                        log.exception("execution backend unavailable for %s; command tools disabled", claim.task.key)
                        backend = None
                ctx = TaskContext(
                    run=claim.run,
                    task=claim.task,
                    attempt=claim.attempt,
                    inputs=inputs,
                    workspace=handle.path if handle else None,
                    cancel=cancel,
                    tools=list(claim.task.tools),
                    paths=list((claim.task.context_spec or {}).get("paths", []) or []),
                    conflicts=list(handle.conflicts) if handle else [],
                    competing=competing,
                    model=metered,
                    deadline=deadline,
                    exec_backend=backend,
                )
                try:
                    result = self.agent.execute(ctx)
                except Exception as e:  # agent bug → FAILED attempt, never a hung task
                    log.exception("agent crashed on %s", claim.task.key)
                    result = AgentResult(success=False, failure_reason=f"agent crashed: {e!r}")
            if metered is not None and metered.calls:
                # the meter is the source of truth for what this attempt spent (agents do not self-report usage)
                result.usage = metered.usage_dict()

            if result.simulate_death or self.dead.is_set():
                # crash simulation: walk away without reporting. Lease expires → reaper → ABANDONED → task READY.
                # The worktree is left behind on purpose (a real crash could not clean up); pruned on the next attempt.
                died = True
                self.stats.died += 1
                self.dead.set()
                log.warning("worker %s simulating death on %s#%s", self.worker_id, claim.task.key, claim.attempt.attempt_number)
                return

            # commit what the agent left in the worktree; the sha is the git_commit artifact
            outs = list(result.artifacts)
            if handle is not None:
                try:
                    sha = self.workspace.publish(
                        handle,
                        f"{claim.task.key}#{claim.attempt.attempt_number}: {getattr(self.agent, 'name', 'agent')}",
                        since=since_for(claim.task),
                    )
                except WorkspaceError as e:
                    log.exception("publish failed for %s", claim.task.key)
                    sha, result = None, AgentResult(success=False, failure_reason=f"workspace publish: {e}", usage=result.usage)
                outs = _resolve_refs(outs, sha)
                if sha and not any(a.type == "git_commit" and a.ref == sha for a in outs):
                    outs.append(
                        ArtifactOut(
                            type="git_commit",
                            ref=sha,
                            meta={"branch": handle.branch, "base": handle.base_sha, "merged": handle.merged},
                        )
                    )
            for a in outs:  # who produced it (a decision policy may name the producer; also plain provenance)
                a.meta.setdefault("producer", claim.task.key)
            result.artifacts = outs
            self._report(claim, result, competing)  # one transaction: artifacts + decisions + contract + settlement
            hb.settled.set()
        finally:
            hb.stop()  # only now — after settlement — does the lease stop being renewed
            if backend is not None:  # the sandbox dies with the attempt: settlement, failure, cancel or simulated death
                try:
                    backend.close()
                except Exception:
                    log.warning("execution backend close failed for %s", claim.task.key, exc_info=True)
            if not died:
                try:
                    self.workspace.cleanup(handle)
                except Exception:
                    log.warning("workspace cleanup failed for %s", claim.task.key, exc_info=True)

    def _report(self, claim: Claim, result: AgentResult, competing: dict[str, list[Any]] | None = None) -> None:
        try:
            task = leases.report(
                self.conn,
                claim.attempt.id,
                success=result.success,
                artifacts=[ArtifactSpec(type=a.type, ref=a.ref, meta=dict(a.meta)) for a in result.artifacts],
                failure_reason=result.failure_reason,
                usage=result.usage or None,
                new_work_required=result.new_work_required,
                competing={slot: [a.id for a in arts] for slot, arts in (competing or {}).items()},
            )
        except StaleAttempt as e:
            self.stats.stale += 1
            log.warning("worker %s: %s", self.worker_id, e)
            return
        if task.status.value == "COMPLETED":
            self.stats.completed += 1
        else:
            self.stats.failed += 1


def _dependency_outputs(conn: Conn, task: Task | UUID) -> list[Any]:
    """Scoped inputs (invariant I-7/I-9; antipatterns A7, B10): outputs of upstream tasks, narrowed by context_spec.

    - default: outputs (SUCCESS attempts' candidate/accepted artifacts) of *direct* dependencies
    - `context_spec.artifacts_from: [keys]`: only those tasks' outputs (validator checks they are dependencies)
    Never "the whole run".
    """
    if not isinstance(task, Task):
        return store.outputs_of_dependencies(conn, task)
    outs = store.outputs_of_dependencies(conn, task.id)
    wanted = (task.context_spec or {}).get("artifacts_from")
    if not wanted:
        return outs
    keys = {str(k) for k in wanted}
    rows = conn.execute("SELECT id FROM tasks WHERE run_id = %s AND key = ANY(%s)", (task.run_id, list(keys))).fetchall()
    allowed_ids = {r["id"] for r in rows}
    return [a for a in outs if a.task_id in allowed_ids]


def _resolve_refs(outs: list[ArtifactOut], sha: str | None) -> list[ArtifactOut]:
    """Agents in a git workspace name file artifacts as `path:<relpath>`; after the commit they become `<sha>:<relpath>`.
    If nothing was committed, path-refs cannot be resolved and are dropped (the contract check will then fail)."""
    resolved: list[ArtifactOut] = []
    for a in outs:
        if a.ref.startswith("path:"):
            if not sha:
                continue
            resolved.append(ArtifactOut(type=a.type, ref=f"{sha}:{a.ref[len('path:') :]}", meta=a.meta))
        else:
            resolved.append(a)
    return resolved


def run_worker_thread(worker: Worker, stop: threading.Event) -> threading.Thread:
    t = threading.Thread(target=worker.run_forever, args=(stop,), name=worker.worker_id, daemon=True)
    t.start()
    return t


def wait_all(threads: list[threading.Thread], timeout: float | None = None) -> None:
    deadline = None if timeout is None else time.monotonic() + timeout
    for t in threads:
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        t.join(remaining)
