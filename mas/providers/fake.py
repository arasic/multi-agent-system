"""FakeProvider: deterministic, offline `ModelProvider` for tests and key-less runs.

Scripted like `StubAgent`: each call consumes the next script item —
    str                     → text completion
    dict                    → {"text", "tool_calls": [{"id","name","input"}], "stop_reason"}
    Exception instance      → raised (simulate rate limits / outages; wrap in ProviderError subclasses)
    callable(messages, tools) → any of the above, decided from the request
When the script is exhausted the last item repeats (or "OK" if there was none). Usage is synthetic but stable
(≈ 1 token per 4 characters) and priced from `cost_per_mtok`, so budgets and telemetry can be tested end to end.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Sequence
from typing import Any

from mas.providers.base import Completion, ToolCall, Usage

ScriptItem = str | dict[str, Any] | BaseException | Callable[..., Any] | Completion


class FakeProvider:
    name = "fake"

    def __init__(
        self,
        script: Sequence[ScriptItem] | Callable[..., Any] | None = None,
        *,
        model: str = "fake-1",
        chars_per_token: float = 4.0,
        cost_per_mtok: tuple[float, float] = (1.0, 5.0),
        input_tokens: int | None = None,  # fixed synthetic usage instead of length-based (tests)
        output_tokens: int | None = None,
    ):
        self.model = model
        self._script: list[ScriptItem] = list(script) if isinstance(script, Sequence) and not isinstance(script, str) else []
        self._fn: Callable[..., Any] | None = script if callable(script) else None
        if isinstance(script, str):
            self._script = [script]
        self.chars_per_token = chars_per_token
        self.cost_per_mtok = cost_per_mtok
        self._fixed_in = input_tokens
        self._fixed_out = output_tokens
        self.calls: list[dict[str, Any]] = []
        self._pos = 0

    # ------------------------------------------------------------------ helpers

    def _next(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None) -> Any:
        if self._fn is not None:
            return self._fn(messages, tools)
        if not self._script:
            return "OK"
        item = self._script[min(self._pos, len(self._script) - 1)]
        self._pos += 1
        if callable(item) and not isinstance(item, BaseException):
            return item(messages, tools)
        return item

    def _usage(self, messages: list[dict[str, Any]], text: str, tool_calls: list[ToolCall]) -> Usage:
        if self._fixed_in is not None and self._fixed_out is not None:
            n_in, n_out = self._fixed_in, self._fixed_out
        else:
            n_in = max(1, int(len(json.dumps(messages, default=str)) / self.chars_per_token))
            n_out = max(1, int((len(text) + sum(len(json.dumps(t.input)) for t in tool_calls)) / self.chars_per_token))
        cost = (n_in * self.cost_per_mtok[0] + n_out * self.cost_per_mtok[1]) / 1_000_000
        return Usage(model=self.model, input_tokens=n_in, output_tokens=n_out, cost_usd=round(cost, 8), priced=True)

    # ------------------------------------------------------------------ ModelProvider

    def complete(
        self,
        messages: list[dict[str, Any]],
        *,
        max_tokens: int = 4096,
        tools: list[dict[str, Any]] | None = None,
        temperature: float | None = None,
    ) -> Completion:
        self.calls.append({"messages": [dict(m) for m in messages], "tools": list(tools or []), "max_tokens": max_tokens})
        item = self._next(messages, tools)
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, Completion):
            return item
        if isinstance(item, str):
            text, tcs, stop = item, [], "end_turn"
        elif isinstance(item, dict):
            text = str(item.get("text", ""))
            tcs = [
                ToolCall(
                    id=str(t.get("id") or f"call_{uuid.uuid4().hex[:8]}"), name=str(t["name"]), input=dict(t.get("input", {}))
                )
                for t in item.get("tool_calls", [])
            ]
            stop = str(item.get("stop_reason") or ("tool_use" if tcs else "end_turn"))
        else:
            raise TypeError(f"FakeProvider script item of unsupported type {type(item).__name__}")
        # honour max_tokens roughly, like a real model would (truncation is visible via stop_reason)
        cap_chars = int(max_tokens * self.chars_per_token)
        if len(text) > cap_chars:
            text, stop = text[:cap_chars], "max_tokens"
        return Completion(text=text, usage=self._usage(messages, text, tcs), raw=item, tool_calls=tcs, stop_reason=stop)
