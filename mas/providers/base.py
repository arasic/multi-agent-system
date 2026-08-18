"""ModelProvider interface (docs/architecture.md §11).

Provider-neutral message and tool shapes — every concrete provider translates to and from these:

    {"role": "system",    "content": str}                                   # hoisted by providers that take it separately
    {"role": "user",      "content": str}
    {"role": "assistant", "content": str, "tool_calls": [{"id", "name", "input"}],   # tool_calls optional
                          "native": {"provider": str, "content": [...]}}             # native optional, see below
    {"role": "tool",      "tool_call_id": str, "content": str, "is_error": bool}    # one per tool result

    tools = [{"name": str, "description": str, "input_schema": {<JSON schema>}}]

`native` is the assistant turn exactly as the producing provider returned it (its own content blocks — e.g. signed
thinking blocks that the vendor requires back, unchanged, with the tool results). It is opaque to everything outside
`mas/providers/`: `Completion.as_message()` carries it, the provider named in it replays it verbatim instead of
rebuilding the turn from text + tool_calls, every other provider ignores it, and the gateway forwards it across its
wire so a worker behind the gateway continues a tool round exactly like a direct client.

Nothing outside `mas/providers/` and config may name a model or a vendor (CLAUDE.md). The rest of the system sees
`ModelProvider.complete(...) -> Completion` and `Usage`.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any, Protocol

STOP_REASONS = ("end_turn", "tool_use", "max_tokens", "refusal", "other")
MIN_CALL_S = 2.0  # below this much remaining time a request (or a retry) is not worth starting


@dataclass(frozen=True)
class Usage:
    """Token accounting for one call (or a sum of calls). `priced=False` means no price is configured for the model:
    `cost_usd` is then 0 and *understates* — surfaced, never hidden (see Pricing / `mas status`)."""

    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    priced: bool = True

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def as_dict(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cost_usd": self.cost_usd,
            "cache_read_tokens": self.cache_read_tokens,
            "cache_write_tokens": self.cache_write_tokens,
            "priced": self.priced,
        }

    def __add__(self, other: Usage) -> Usage:
        if not isinstance(other, Usage):
            return NotImplemented
        model = self.model if self.model == other.model or not other.model else ("mixed" if self.model else other.model)
        return Usage(
            model=model,
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            cost_usd=self.cost_usd + other.cost_usd,
            cache_read_tokens=self.cache_read_tokens + other.cache_read_tokens,
            cache_write_tokens=self.cache_write_tokens + other.cache_write_tokens,
            priced=self.priced and other.priced,
        )

    def with_cost(self, cost_usd: float | None) -> Usage:
        """Return a copy priced by a Pricing table (None = unknown price → priced=False, cost unchanged)."""
        if cost_usd is None:
            return replace(self, priced=False)
        return replace(self, cost_usd=cost_usd, priced=True)


ZERO_USAGE = Usage(model="")


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    input: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {"id": self.id, "name": self.name, "input": dict(self.input)}


@dataclass
class Completion:
    text: str
    usage: Usage
    raw: Any = None
    tool_calls: list[ToolCall] = field(default_factory=list)
    stop_reason: str = "end_turn"  # one of STOP_REASONS
    request_id: str | None = None
    native: dict[str, Any] | None = None  # {"provider": name, "content": [provider blocks]} — the turn to replay verbatim

    @property
    def refused(self) -> bool:
        return self.stop_reason == "refusal"

    def as_message(self) -> dict[str, Any]:
        """The assistant turn to append to a conversation before sending tool results back."""
        m: dict[str, Any] = {"role": "assistant", "content": self.text}
        if self.tool_calls:
            m["tool_calls"] = [t.as_dict() for t in self.tool_calls]
        if self.native:
            m["native"] = self.native
        return m


def native_content(message: dict[str, Any], provider: str) -> list[dict[str, Any]] | None:
    """The `native` blocks of an assistant message if they were produced by `provider` (else None)."""
    native = message.get("native")
    if not isinstance(native, dict) or native.get("provider") != provider:
        return None
    content = native.get("content")
    if not isinstance(content, list) or not content:
        return None
    return [dict(b) for b in content if isinstance(b, dict)]


def native_summary(message: dict[str, Any]) -> dict[str, Any] | None:
    """A vendor-shape-free description of an assistant turn's `native` blocks, for diagnostics.

    `native` itself stays opaque outside this package (see the module docstring), but *whether* a turn carries signed
    reasoning that a continuation must replay is exactly what a live probe needs to report. Block *types* and counts
    are safe to hand out; the signed payloads never leave here."""
    native = message.get("native")
    if not isinstance(native, dict):
        return None
    blocks = [b for b in (native.get("content") or []) if isinstance(b, dict)]
    types: dict[str, int] = {}
    for block in blocks:
        key = str(block.get("type") or "unknown")
        types[key] = types.get(key, 0) + 1
    return {
        "provider": native.get("provider"),
        "blocks": len(blocks),
        "block_types": dict(sorted(types.items())),
        "signed_reasoning": types.get("thinking", 0) + types.get("redacted_thinking", 0),
    }


class ModelProvider(Protocol):
    name: str  # vendor/family label used in telemetry ("anthropic", "openai", "fake", ...)
    model: str  # model id as configured

    def complete(
        self,
        messages: list[dict[str, Any]],
        *,
        max_tokens: int = 4096,
        tools: list[dict[str, Any]] | None = None,
        temperature: float | None = None,
        timeout_s: float | None = None,  # total wall-clock budget for this call INCLUDING the provider's own retries
    ) -> Completion: ...


# ----------------------------------------------------------------------------- errors


class ProviderError(RuntimeError):
    """Base class. `retryable` tells the caller (worker/planner) whether a bounded retry makes sense."""

    retryable: bool = False

    def __init__(self, message: str, *, status: int | None = None, retryable: bool | None = None):
        super().__init__(message)
        self.status = status
        if retryable is not None:
            self.retryable = retryable


class ProviderRateLimited(ProviderError):
    retryable = True

    def __init__(self, message: str, *, status: int | None = 429, retry_after_s: float | None = None):
        super().__init__(message, status=status)
        self.retry_after_s = retry_after_s


class ProviderUnavailable(ProviderError):
    """5xx, overloaded, connection or timeout problems."""

    retryable = True


class ProviderRequestError(ProviderError):
    """4xx other than 429: bad request, auth, unknown model. Retrying the same request will not help."""

    retryable = False


# ----------------------------------------------------------------------------- bounded retries


def backoff_s(attempt: int) -> float:
    return min(20.0, 0.5 * (2 ** (attempt - 1))) + random.uniform(0, 0.25)


def call_with_retries(
    attempt_fn: Callable[[float | None], Any],
    *,
    budget_s: float | None,
    max_retries: int,
    sleep: Callable[[float], None] = time.sleep,
    min_call_s: float = MIN_CALL_S,
) -> Any:
    """Run `attempt_fn(remaining_s)` with bounded retries that can never outlive `budget_s`.

    - `remaining_s` (None = unbounded) is recomputed immediately before every request;
    - a retryable ProviderError is retried at most `max_retries` times, sleeping `retry_after_s` (rate limits) or an
      exponential backoff — but only if the sleep AND a minimum viable call window still fit in the budget; otherwise
      the error is raised right away (an excessive Retry-After therefore fails fast, it never sleeps past the deadline);
    - a non-retryable error is raised immediately.
    Providers use this so that hidden SDK/HTTP retries never silently exceed an attempt's deadline.
    """
    t0 = time.monotonic()
    tries = 0
    while True:
        remaining = None if budget_s is None else budget_s - (time.monotonic() - t0)
        if remaining is not None and remaining < min_call_s and tries > 0:
            raise ProviderUnavailable(f"call budget exhausted after {tries} attempt(s) ({budget_s:.1f}s)", retryable=False)
        try:
            return attempt_fn(remaining)
        except ProviderError as e:
            tries += 1
            if not e.retryable or tries > max_retries:
                raise
            delay = getattr(e, "retry_after_s", None)
            if delay is None:
                delay = backoff_s(tries)
            if remaining is not None:
                left = budget_s - (time.monotonic() - t0)  # type: ignore[operator]
                if delay > max(0.0, left - min_call_s):
                    e.retryable = False  # a retry cannot fit before the deadline; the caller must not loop on this
                    raise
            sleep(delay)
