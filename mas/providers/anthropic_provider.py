"""AnthropicProvider — `ModelProvider` over the official `anthropic` SDK (Messages API).

Install with `pip install -e ".[llm]"`. Credentials resolve the SDK way (`ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`,
or an `ant auth login` profile) — nothing here reads a key from MAS config. Only this module names the vendor.

Request shape (current models):
- adaptive thinking on by default (`thinking={"type": "adaptive"}`), depth via `output_config.effort` when configured;
- `temperature` is *not* forwarded: current Claude models reject sampling parameters (400), so the protocol's
  `temperature` hint is ignored here (logged once);
- streaming (`messages.stream(...).get_final_message()`) when `max_tokens` is large, plain `create` otherwise;
- optional server-side refusal fallbacks (beta) when `fallbacks` is set — opt-in, off by default, because it is a
  Claude-API-only beta we cannot exercise offline;
- SDK retries are OFF (`max_retries=0`); retries run in `call_with_retries` so that a call's `timeout_s` budget bounds
  every request and every backoff/Retry-After sleep — nothing can silently outlive the attempt's deadline;
- `stop_reason == "refusal"` is surfaced as `Completion.stop_reason="refusal"` (empty text), never as an exception;
- thinking continuation: with adaptive thinking the API returns signed `thinking` / `redacted_thinking` blocks ahead of
  every `tool_use` and requires them back **unchanged** in that assistant turn when the tool results arrive (else the
  next call is a 400). `message_to_completion` therefore keeps the turn's blocks as `Completion.native` and
  `to_anthropic_messages` replays them verbatim; only turns without such blocks are rebuilt from text + tool_calls.

Usage returned is *unpriced* (`priced=False`); the MeteredProvider prices it from `MAS_MODEL_PRICES`.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from mas.providers.base import (
    Completion,
    ProviderError,
    ProviderRateLimited,
    ProviderRequestError,
    ProviderUnavailable,
    ToolCall,
    Usage,
    call_with_retries,
    native_content,
)

log = logging.getLogger(__name__)

STREAM_THRESHOLD_TOKENS = 16_000  # above this the SDK needs streaming to avoid HTTP timeouts
DEFAULT_MODEL = "claude-opus-5"
_FALLBACK_BETA = "server-side-fallback-2026-07-01"
NATIVE_PROVIDER = "anthropic"  # tag on Completion.native / message["native"]: only this provider replays those blocks
# response block types replayed verbatim, and the fields the Messages API accepts back for each of them
_REPLAY_FIELDS = {
    "thinking": ("thinking", "signature"),
    "redacted_thinking": ("data",),
    "text": ("text",),
    "tool_use": ("id", "name", "input"),
}


class AnthropicProvider:
    name = "anthropic"

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        *,
        client: Any | None = None,
        thinking: bool = True,
        effort: str | None = None,
        fallbacks: str | None = None,
        timeout_s: float = 600.0,
        max_retries: int = 2,
        api_key: str | None = None,
        base_url: str | None = None,
        sleep: Any = None,
    ):
        self.model = model
        self.thinking = thinking
        self.effort = effort or None
        self.fallbacks = fallbacks or None
        self.timeout_s = float(timeout_s)
        self.max_retries = max(0, int(max_retries))
        self._sleep = sleep
        self._warned_temperature = False
        if client is None:
            try:
                import anthropic
            except ImportError as e:  # pragma: no cover - environment
                raise ProviderRequestError("the 'anthropic' package is not installed: pip install -e '.[llm]'") from e
            # SDK retries are disabled: retries happen in call_with_retries, where they are bounded by the call budget
            kwargs: dict[str, Any] = {"timeout": timeout_s, "max_retries": 0}
            if api_key:
                kwargs["api_key"] = api_key
            if base_url:
                kwargs["base_url"] = base_url
            client = anthropic.Anthropic(**kwargs)
        self.client = client

    # ------------------------------------------------------------------ ModelProvider

    def complete(
        self,
        messages: list[dict[str, Any]],
        *,
        max_tokens: int = 4096,
        tools: list[dict[str, Any]] | None = None,
        temperature: float | None = None,
        timeout_s: float | None = None,
    ) -> Completion:
        if temperature is not None and not self._warned_temperature:
            log.info("anthropic provider: ignoring temperature=%s (rejected by current models)", temperature)
            self._warned_temperature = True
        params = self.build_params(messages, max_tokens=max_tokens, tools=tools)
        if self.fallbacks:
            params["extra_body"] = {"fallbacks": self.fallbacks}
            params["extra_headers"] = {"anthropic-beta": _FALLBACK_BETA}

        def attempt(remaining: float | None) -> Any:
            per_request = self.timeout_s if remaining is None else max(0.1, min(self.timeout_s, remaining))
            client = self._client_for(per_request)
            api = client.beta.messages if self.fallbacks else client.messages
            try:
                if max_tokens > STREAM_THRESHOLD_TOKENS:
                    with api.stream(**params) as stream:
                        return stream.get_final_message()
                return api.create(**params)
            except ProviderError:
                raise
            except Exception as e:  # translate SDK errors to provider-neutral ones (most specific first)
                raise _translate(e) from e

        # timeout_s is the whole call's budget: per-request HTTP timeout AND retry sleeps must fit inside it
        msg = call_with_retries(attempt, budget_s=timeout_s, max_retries=self.max_retries, sleep=self._sleep or time.sleep)
        return message_to_completion(msg)

    def _client_for(self, per_request_timeout: float) -> Any:
        """A view of the client with this request's timeout and NO SDK retries (fake test clients may lack with_options)."""
        with_options = getattr(self.client, "with_options", None)
        if with_options is None:
            return self.client
        return with_options(timeout=per_request_timeout, max_retries=0)

    # ------------------------------------------------------------------ translation (pure; unit-tested offline)

    def build_params(
        self, messages: list[dict[str, Any]], *, max_tokens: int, tools: list[dict[str, Any]] | None
    ) -> dict[str, Any]:
        system, msgs = to_anthropic_messages(messages)
        params: dict[str, Any] = {"model": self.model, "max_tokens": max_tokens, "messages": msgs}
        if system:
            params["system"] = system
        if tools:
            params["tools"] = [to_anthropic_tool(t) for t in tools]
        if self.thinking:
            params["thinking"] = {"type": "adaptive"}
        if self.effort:
            params["output_config"] = {"effort": self.effort}
        return params


def to_anthropic_tool(tool: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": str(tool["name"]),
        "description": str(tool.get("description", "")),
        "input_schema": dict(tool.get("input_schema") or {"type": "object", "properties": {}}),
    }


def to_anthropic_messages(messages: list[dict[str, Any]]) -> tuple[str | None, list[dict[str, Any]]]:
    """Provider-neutral messages → (system, Anthropic messages). Consecutive tool results are folded into ONE user
    message (all results of a parallel tool round must travel together). An assistant turn this provider produced
    (`native`) is replayed block for block — thinking blocks included, unchanged — so a tool round continues; a turn
    without usable native blocks is rebuilt from text + tool_calls, and an empty one is dropped (the API rejects empty
    text; consecutive same-role turns are merged server-side)."""
    system_parts: list[str] = []
    out: list[dict[str, Any]] = []
    for m in messages:
        role = m.get("role")
        if role == "system":
            if m.get("content"):
                system_parts.append(str(m["content"]))
        elif role == "user":
            out.append({"role": "user", "content": str(m.get("content", ""))})
        elif role == "assistant":
            replay = _replayable(native_content(m, NATIVE_PROVIDER))
            if replay:
                out.append({"role": "assistant", "content": replay})
                continue
            calls = m.get("tool_calls") or []
            text = str(m.get("content", "") or "")
            if not calls:
                if text.strip():
                    out.append({"role": "assistant", "content": text})
            else:
                blocks: list[dict[str, Any]] = []
                if text.strip():
                    blocks.append({"type": "text", "text": text})
                for c in calls:
                    blocks.append(
                        {"type": "tool_use", "id": str(c["id"]), "name": str(c["name"]), "input": dict(c.get("input") or {})}
                    )
                out.append({"role": "assistant", "content": blocks})
        elif role == "tool":
            block = {
                "type": "tool_result",
                "tool_use_id": str(m["tool_call_id"]),
                "content": str(m.get("content", "")),
            }
            if m.get("is_error"):
                block["is_error"] = True
            if out and out[-1]["role"] == "user" and isinstance(out[-1]["content"], list):
                out[-1]["content"].append(block)
            else:
                out.append({"role": "user", "content": [block]})
        else:
            raise ValueError(f"unknown message role {role!r}")
    system = "\n\n".join(system_parts) if system_parts else None
    return system, out


def _replay_block(block: Any) -> dict[str, Any] | None:
    """A response content block → the input block the API accepts back unchanged (None: not a replayable type)."""
    btype = getattr(block, "type", None)
    fields = _REPLAY_FIELDS.get(btype)
    if fields is None:
        return None
    out: dict[str, Any] = {"type": btype}
    for name in fields:
        value = getattr(block, name, None)
        if value is None:
            continue
        out[name] = dict(value) if name == "input" else value
    if btype == "text" and not str(out.get("text", "")).strip():
        return None  # the API rejects empty text blocks
    if btype == "tool_use" and not out.get("id"):
        return None
    return out


def _replayable(blocks: list[dict[str, Any]] | None) -> list[dict[str, Any]] | None:
    """Native blocks worth replaying: at least one text or tool_use block (a thinking-only turn — e.g. cut off by
    max_tokens before any output — is rebuilt/dropped instead; nothing downstream depends on it)."""
    if not blocks:
        return None
    if not any(b.get("type") in ("text", "tool_use") for b in blocks):
        return None
    return blocks


def message_to_completion(msg: Any) -> Completion:
    text_parts: list[str] = []
    calls: list[ToolCall] = []
    replay: list[dict[str, Any]] = []
    for block in getattr(msg, "content", []) or []:
        btype = getattr(block, "type", None)
        if btype == "text":
            text_parts.append(getattr(block, "text", "") or "")
        elif btype == "tool_use":
            calls.append(ToolCall(id=str(block.id), name=str(block.name), input=dict(block.input or {})))
        rb = _replay_block(block)
        if rb is not None:
            replay.append(rb)
    native = {"provider": NATIVE_PROVIDER, "content": replay} if replay else None
    u = getattr(msg, "usage", None)
    usage = Usage(
        model=str(getattr(msg, "model", "") or ""),
        input_tokens=int(getattr(u, "input_tokens", 0) or 0),
        output_tokens=int(getattr(u, "output_tokens", 0) or 0),
        cache_read_tokens=int(getattr(u, "cache_read_input_tokens", 0) or 0),
        cache_write_tokens=int(getattr(u, "cache_creation_input_tokens", 0) or 0),
        priced=False,
    )
    stop = _map_stop(getattr(msg, "stop_reason", None))
    return Completion(
        text="".join(text_parts),
        usage=usage,
        raw=msg,
        tool_calls=calls,
        stop_reason=stop,
        request_id=getattr(msg, "_request_id", None),
        native=native,
    )


def _map_stop(reason: str | None) -> str:
    if reason in ("end_turn", "stop_sequence"):
        return "end_turn"
    if reason == "tool_use":
        return "tool_use"
    if reason == "max_tokens":
        return "max_tokens"
    if reason == "refusal":
        return "refusal"
    return "other"


def _translate(e: Exception) -> ProviderError:
    try:
        import anthropic
    except ImportError:  # pragma: no cover
        return ProviderUnavailable(str(e))
    if isinstance(e, anthropic.RateLimitError):
        retry_after = None
        try:
            retry_after = float(e.response.headers.get("retry-after", ""))
        except Exception:
            pass
        return ProviderRateLimited(f"rate limited: {e.message}", retry_after_s=retry_after)
    if isinstance(e, anthropic.APIStatusError):
        if e.status_code >= 500 or getattr(e, "type", "") == "overloaded_error":
            return ProviderUnavailable(f"api error {e.status_code}: {e.message}", status=e.status_code)
        return ProviderRequestError(f"request error {e.status_code}: {e.message}", status=e.status_code)
    if isinstance(e, anthropic.APIConnectionError):  # includes APITimeoutError
        return ProviderUnavailable(f"connection error: {e}")
    if isinstance(e, anthropic.AnthropicError | ValueError | TypeError):
        # client-side: credential resolution/refresh failures, invalid parameters — retrying the same call will not help
        return ProviderRequestError(f"{type(e).__name__}: {e}")
    return ProviderUnavailable(f"{type(e).__name__}: {e}")
