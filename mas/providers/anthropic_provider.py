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
- `stop_reason == "refusal"` is surfaced as `Completion.stop_reason="refusal"` (empty text), never as an exception.

Usage returned is *unpriced* (`priced=False`); the MeteredProvider prices it from `MAS_MODEL_PRICES`.
"""

from __future__ import annotations

import logging
from typing import Any

from mas.providers.base import (
    Completion,
    ProviderError,
    ProviderRateLimited,
    ProviderRequestError,
    ProviderUnavailable,
    ToolCall,
    Usage,
)

log = logging.getLogger(__name__)

STREAM_THRESHOLD_TOKENS = 16_000  # above this the SDK needs streaming to avoid HTTP timeouts
DEFAULT_MODEL = "claude-opus-5"
_FALLBACK_BETA = "server-side-fallback-2026-07-01"


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
    ):
        self.model = model
        self.thinking = thinking
        self.effort = effort or None
        self.fallbacks = fallbacks or None
        self._warned_temperature = False
        if client is None:
            try:
                import anthropic
            except ImportError as e:  # pragma: no cover - environment
                raise ProviderRequestError("the 'anthropic' package is not installed: pip install -e '.[llm]'") from e
            kwargs: dict[str, Any] = {"timeout": timeout_s, "max_retries": max_retries}
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
    ) -> Completion:
        if temperature is not None and not self._warned_temperature:
            log.info("anthropic provider: ignoring temperature=%s (rejected by current models)", temperature)
            self._warned_temperature = True
        params = self.build_params(messages, max_tokens=max_tokens, tools=tools)
        try:
            if self.fallbacks:
                params["extra_body"] = {"fallbacks": self.fallbacks}
                params["extra_headers"] = {"anthropic-beta": _FALLBACK_BETA}
                api = self.client.beta.messages
            else:
                api = self.client.messages
            if max_tokens > STREAM_THRESHOLD_TOKENS:
                with api.stream(**params) as stream:
                    msg = stream.get_final_message()
            else:
                msg = api.create(**params)
        except ProviderError:
            raise
        except Exception as e:  # translate SDK errors to provider-neutral ones (most specific first)
            raise _translate(e) from e
        return message_to_completion(msg)

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
    message (all results of a parallel tool round must travel together)."""
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
            calls = m.get("tool_calls") or []
            text = str(m.get("content", "") or "")
            if not calls:
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


def message_to_completion(msg: Any) -> Completion:
    text_parts: list[str] = []
    calls: list[ToolCall] = []
    for block in getattr(msg, "content", []) or []:
        btype = getattr(block, "type", None)
        if btype == "text":
            text_parts.append(getattr(block, "text", "") or "")
        elif btype == "tool_use":
            calls.append(ToolCall(id=str(block.id), name=str(block.name), input=dict(block.input or {})))
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
