"""ModelProvider interface (docs/architecture.md §11).

Provider-neutral message and tool shapes — every concrete provider translates to and from these:

    {"role": "system",    "content": str}                                   # hoisted by providers that take it separately
    {"role": "user",      "content": str}
    {"role": "assistant", "content": str, "tool_calls": [{"id", "name", "input"}]}   # tool_calls optional
    {"role": "tool",      "tool_call_id": str, "content": str, "is_error": bool}    # one per tool result

    tools = [{"name": str, "description": str, "input_schema": {<JSON schema>}}]

Nothing outside `mas/providers/` and config may name a model or a vendor (CLAUDE.md). The rest of the system sees
`ModelProvider.complete(...) -> Completion` and `Usage`.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Protocol

STOP_REASONS = ("end_turn", "tool_use", "max_tokens", "refusal", "other")


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

    @property
    def refused(self) -> bool:
        return self.stop_reason == "refusal"

    def as_message(self) -> dict[str, Any]:
        """The assistant turn to append to a conversation before sending tool results back."""
        m: dict[str, Any] = {"role": "assistant", "content": self.text}
        if self.tool_calls:
            m["tool_calls"] = [t.as_dict() for t in self.tool_calls]
        return m


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
