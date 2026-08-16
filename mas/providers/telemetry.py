"""Per-call telemetry and per-attempt call budgets (roadmap step 9).

`MeteredProvider` wraps any `ModelProvider`: every call is timed, priced (Pricing), summed into `total`, and written
through a `Sink` — `DbSink` inserts a row into `model_calls` *immediately, on its own transaction*, so the record
survives a worker that dies mid-attempt (settlement-time `attempts.*_tokens` are the summary; `model_calls` is the
evidence). A `CallBudget` bounds calls and tokens per attempt (antipatterns E1: no unbounded loops). The call-count
limit is strict (checked before each call). Token usage is only known after the provider responds, so it is accounted
after each response and further calls are refused once the limit is reached: overshoot is bounded to one completed
call (itself bounded by that call's `max_tokens`), and settlement then trips the run's token budget if it is exhausted.
`AttemptBudgetExceeded` is terminal for the attempt — never a retry loop.

Agents get a metered provider from the runtime (`TaskContext.model`); they never construct providers themselves.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID

from mas.db.connection import Conn, Jsonb
from mas.providers.base import Completion, ModelProvider, ProviderError, Usage
from mas.providers.pricing import Pricing

log = logging.getLogger(__name__)


@dataclass
class CallRecord:
    provider: str
    model: str
    role: str  # planner | worker | reviewer | ping
    seq: int  # 1-based index of the call within this meter (attempt / planning round)
    started_at: datetime
    duration_ms: int
    status: str  # ok | max_tokens | refusal | error
    run_id: UUID | None = None
    task_id: UUID | None = None
    attempt_id: UUID | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    cost_usd: float = 0.0
    priced: bool = False
    stop_reason: str | None = None
    error: str | None = None
    request_id: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        d = dict(self.__dict__)
        for k in ("run_id", "task_id", "attempt_id"):
            d[k] = str(d[k]) if d[k] is not None else None
        d["started_at"] = self.started_at.isoformat()
        return d


class Sink(Protocol):
    def record(self, rec: CallRecord) -> None: ...


class NullSink:
    def record(self, rec: CallRecord) -> None:
        return None


class MemorySink:
    def __init__(self) -> None:
        self.records: list[CallRecord] = []
        self._lock = threading.Lock()

    def record(self, rec: CallRecord) -> None:
        with self._lock:
            self.records.append(rec)


class DbSink:
    """Writes each call to `model_calls` right away (autocommit connection). Serialised with a lock so an agent that
    calls the model from several threads cannot interleave statements on one psycopg connection. Telemetry failures
    are logged and counted, never raised into the agent — but they are loud (error level)."""

    def __init__(self, conn: Conn, lock: threading.Lock | None = None):
        self.conn = conn
        self._lock = lock or threading.Lock()
        self.failures = 0
        self.written = 0

    def record(self, rec: CallRecord) -> None:
        with self._lock:
            try:
                self.conn.execute(
                    """
                    INSERT INTO model_calls (
                        run_id, task_id, attempt_id, role, provider, model, seq, started_at, duration_ms,
                        input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, cost_usd, priced,
                        status, stop_reason, error, request_id, meta
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        rec.run_id,
                        rec.task_id,
                        rec.attempt_id,
                        rec.role,
                        rec.provider,
                        rec.model,
                        rec.seq,
                        rec.started_at,
                        rec.duration_ms,
                        rec.input_tokens,
                        rec.output_tokens,
                        rec.cache_read_tokens,
                        rec.cache_write_tokens,
                        rec.cost_usd,
                        rec.priced,
                        rec.status,
                        rec.stop_reason,
                        rec.error,
                        rec.request_id,
                        Jsonb(rec.meta or {}),
                    ),
                )
                self.written += 1
            except Exception:
                self.failures += 1
                log.error(
                    "telemetry: failed to record model call (%s %s seq=%s)", rec.provider, rec.model, rec.seq, exc_info=True
                )
                try:
                    self.conn.rollback()
                except Exception:
                    pass


@dataclass(frozen=True)
class CallBudget:
    """Per-meter limits. None = unlimited. `max_calls` is strict; `max_tokens` counts input+output across all calls and is
    enforced after each response (one completed call of overshoot at most)."""

    max_calls: int | None = None
    max_tokens: int | None = None


class AttemptBudgetExceeded(ProviderError):
    """Raised by MeteredProvider instead of making a call once the budget is exhausted (calls: strict; tokens: after the
    response that crossed the line). Not retryable within the attempt."""

    retryable = False


class MeteredProvider:
    """A ModelProvider that records every call. Thread-safe for its counters; the inner provider must be thread-safe
    itself if an agent calls it concurrently."""

    def __init__(
        self,
        inner: ModelProvider,
        *,
        sink: Sink | None = None,
        pricing: Pricing | None = None,
        role: str = "worker",
        run_id: UUID | None = None,
        task_id: UUID | None = None,
        attempt_id: UUID | None = None,
        budget: CallBudget | None = None,
    ):
        self.inner = inner
        self.sink: Sink = sink or NullSink()
        self.pricing = pricing or Pricing()
        self.role = role
        self.run_id = run_id
        self.task_id = task_id
        self.attempt_id = attempt_id
        self.budget = budget or CallBudget()
        self.name = getattr(inner, "name", "unknown")
        self.model = getattr(inner, "model", "")
        self.total = Usage(model=self.model)
        self.calls = 0
        self.errors = 0
        self.records: list[CallRecord] = []
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ budget

    @property
    def remaining_tokens(self) -> int | None:
        if self.budget.max_tokens is None:
            return None
        return max(0, self.budget.max_tokens - self.total.total_tokens)

    @property
    def remaining_calls(self) -> int | None:
        if self.budget.max_calls is None:
            return None
        return max(0, self.budget.max_calls - self.calls)

    def _check_budget(self) -> None:
        rc, rt = self.remaining_calls, self.remaining_tokens
        if rc is not None and rc <= 0:
            raise AttemptBudgetExceeded(f"call budget exhausted ({self.calls}/{self.budget.max_calls} calls)")
        if rt is not None and rt <= 0:
            raise AttemptBudgetExceeded(
                f"token budget exhausted ({self.total.total_tokens}/{self.budget.max_tokens} tokens over {self.calls} calls)"
            )

    # ------------------------------------------------------------------ the call

    def complete(
        self,
        messages: list[dict[str, Any]],
        *,
        max_tokens: int = 4096,
        tools: list[dict[str, Any]] | None = None,
        temperature: float | None = None,
    ) -> Completion:
        with self._lock:
            self._check_budget()
            self.calls += 1
            seq = self.calls
        started = datetime.now(UTC)
        t0 = time.perf_counter()
        try:
            comp = self.inner.complete(messages, max_tokens=max_tokens, tools=tools, temperature=temperature)
        except Exception as e:
            dur = int((time.perf_counter() - t0) * 1000)
            with self._lock:
                self.errors += 1
            self._emit(
                CallRecord(
                    provider=self.name,
                    model=self.model,
                    role=self.role,
                    seq=seq,
                    started_at=started,
                    duration_ms=dur,
                    status="error",
                    run_id=self.run_id,
                    task_id=self.task_id,
                    attempt_id=self.attempt_id,
                    error=f"{type(e).__name__}: {e}"[:2000],
                    meta={"max_tokens": max_tokens, "messages": len(messages), "tools": len(tools or [])},
                )
            )
            raise
        dur = int((time.perf_counter() - t0) * 1000)
        usage = self._price(comp.usage)
        comp.usage = usage
        with self._lock:
            self.total = self.total + usage
        status = "ok"
        if comp.stop_reason == "max_tokens":
            status = "max_tokens"
        elif comp.stop_reason == "refusal":
            status = "refusal"
        self._emit(
            CallRecord(
                provider=self.name,
                model=usage.model or self.model,
                role=self.role,
                seq=seq,
                started_at=started,
                duration_ms=dur,
                status=status,
                run_id=self.run_id,
                task_id=self.task_id,
                attempt_id=self.attempt_id,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cache_read_tokens=usage.cache_read_tokens,
                cache_write_tokens=usage.cache_write_tokens,
                cost_usd=usage.cost_usd,
                priced=usage.priced,
                stop_reason=comp.stop_reason,
                request_id=comp.request_id,
                meta={
                    "max_tokens": max_tokens,
                    "messages": len(messages),
                    "tools": len(tools or []),
                    "tool_calls": len(comp.tool_calls),
                },
            )
        )
        return comp

    def _price(self, usage: Usage) -> Usage:
        """Real providers return unpriced usage; the meter prices it from config. A provider that priced itself
        (FakeProvider, or a gateway that bills) is left alone unless the table knows the model."""
        cost = self.pricing.cost(
            usage.model or self.model,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            cache_read_tokens=usage.cache_read_tokens,
            cache_write_tokens=usage.cache_write_tokens,
        )
        if cost is not None:
            return usage.with_cost(cost)
        return usage  # keeps the provider's own cost/priced flags

    def _emit(self, rec: CallRecord) -> None:
        with self._lock:
            self.records.append(rec)
        try:
            self.sink.record(rec)
        except Exception:
            log.error("telemetry sink failed", exc_info=True)

    # ------------------------------------------------------------------ reporting

    def usage_dict(self) -> dict[str, Any]:
        """What the worker reports at settlement (`AgentResult.usage`)."""
        d = self.total.as_dict()
        d["calls"] = self.calls
        d["errors"] = self.errors
        return d
