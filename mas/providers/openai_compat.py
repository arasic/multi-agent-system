"""OpenAICompatibleProvider — `ModelProvider` over the OpenAI *Chat Completions* wire format, stdlib HTTP only.

Speaks to api.openai.com and to any server exposing the same shape (OpenAI-compatible endpoints of other vendors,
local inference servers, or a narrow in-cluster gateway — the hardened workers have no egress, so a gateway on the
backend network is the intended production path). No SDK dependency; the transport is injectable for tests.

Config: `MAS_OPENAI_BASE_URL` (default https://api.openai.com/v1), `OPENAI_API_KEY` / `MAS_OPENAI_API_KEY`,
`MAS_OPENAI_MAX_TOKENS_FIELD` (`max_completion_tokens` default; older servers want `max_tokens`).
Usage returned is unpriced; the MeteredProvider prices it from `MAS_MODEL_PRICES`. Only this module names the vendor.

One extension on the wire, both directions: an assistant message may carry `mas_native` — the neutral shape's `native`
(the upstream provider's own content blocks, e.g. signed thinking blocks the vendor needs back with tool results). The
mas gateway emits it in responses and restores it from requests, so a worker behind the gateway continues a tool round
exactly like a direct client. A foreign server never emits it, so a client of a foreign server never sends it.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from collections.abc import Callable
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
)

log = logging.getLogger(__name__)

NATIVE_FIELD = "mas_native"  # wire name of the neutral shape's `native` on assistant messages (see module docstring)

# transport(url, body_bytes, headers, timeout_s) -> (status, headers, body_bytes)
Transport = Callable[[str, bytes, dict[str, str], float], tuple[int, dict[str, str], bytes]]


def _urllib_transport(url: str, body: bytes, headers: dict[str, str], timeout_s: float) -> tuple[int, dict[str, str], bytes]:
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:  # noqa: S310 - https URL from config
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in (e.headers or {}).items()}, e.read() or b""


class OpenAICompatibleProvider:
    name = "openai"

    def __init__(
        self,
        model: str,
        *,
        base_url: str = "https://api.openai.com/v1",
        api_key: str | None = None,
        timeout_s: float = 600.0,
        max_retries: int = 2,
        max_tokens_field: str = "max_completion_tokens",
        extra_headers: dict[str, str] | None = None,
        transport: Transport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_s = timeout_s
        self.max_retries = max(0, int(max_retries))
        self.max_tokens_field = max_tokens_field
        self.extra_headers = dict(extra_headers or {})
        self._transport = transport or _urllib_transport
        self._sleep = sleep

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
        body = self.build_body(messages, max_tokens=max_tokens, tools=tools, temperature=temperature)
        status, headers, raw = self._post_with_retries(json.dumps(body).encode("utf-8"), budget_s=timeout_s)
        try:
            data = json.loads(raw.decode("utf-8") or "{}")
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ProviderUnavailable(f"invalid JSON from provider (HTTP {status}): {e}", status=status) from e
        return response_to_completion(data, request_id=headers.get("x-request-id"), model=self.model)

    # ------------------------------------------------------------------ HTTP

    def _headers(self) -> dict[str, str]:
        h = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        h.update(self.extra_headers)
        return h

    def _post_with_retries(self, body: bytes, *, budget_s: float | None = None) -> tuple[int, dict[str, str], bytes]:
        """One call = bounded retries inside `budget_s`: the HTTP timeout of every request is clamped to the remaining
        budget, and a backoff / Retry-After sleep happens only if it (plus a viable request) still fits."""
        url = f"{self.base_url}/chat/completions"

        def attempt(remaining: float | None) -> tuple[int, dict[str, str], bytes]:
            http_timeout = self.timeout_s if remaining is None else max(0.1, min(self.timeout_s, remaining))
            try:
                status, headers, raw = self._transport(url, body, self._headers(), http_timeout)
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                raise ProviderUnavailable(f"connection error: {e}") from e
            if 200 <= status < 300:
                return status, headers, raw
            message = _error_message(raw, status)
            if status == 429:
                raise ProviderRateLimited(f"rate limited: {message}", retry_after_s=_retry_after(headers))
            if status >= 500:
                raise ProviderUnavailable(f"api error {status}: {message}", status=status)
            raise ProviderRequestError(f"request error {status}: {message}", status=status)

        return call_with_retries(attempt, budget_s=budget_s, max_retries=self.max_retries, sleep=self._sleep)

    # ------------------------------------------------------------------ translation (pure; unit-tested offline)

    def build_body(
        self,
        messages: list[dict[str, Any]],
        *,
        max_tokens: int,
        tools: list[dict[str, Any]] | None,
        temperature: float | None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"model": self.model, "messages": to_openai_messages(messages), self.max_tokens_field: max_tokens}
        if tools:
            body["tools"] = [to_openai_tool(t) for t in tools]
        if temperature is not None:
            body["temperature"] = temperature
        return body


def to_openai_tool(tool: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": str(tool["name"]),
            "description": str(tool.get("description", "")),
            "parameters": dict(tool.get("input_schema") or {"type": "object", "properties": {}}),
        },
    }


def to_openai_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for m in messages:
        role = m.get("role")
        if role in ("system", "user"):
            out.append({"role": role, "content": str(m.get("content", ""))})
        elif role == "assistant":
            calls = m.get("tool_calls") or []
            text = str(m.get("content", "") or "")
            entry: dict[str, Any] = {"role": "assistant", "content": text if text or not calls else None}
            if calls:
                entry["tool_calls"] = [
                    {
                        "id": str(c["id"]),
                        "type": "function",
                        "function": {"name": str(c["name"]), "arguments": json.dumps(dict(c.get("input") or {}))},
                    }
                    for c in calls
                ]
            if isinstance(m.get("native"), dict):
                entry[NATIVE_FIELD] = m["native"]
            out.append(entry)
        elif role == "tool":
            content = str(m.get("content", ""))
            if m.get("is_error"):
                content = f"ERROR: {content}"  # the wire format has no error flag
            out.append({"role": "tool", "tool_call_id": str(m["tool_call_id"]), "content": content})
        else:
            raise ValueError(f"unknown message role {role!r}")
    return out


def response_to_completion(data: dict[str, Any], *, request_id: str | None = None, model: str = "") -> Completion:
    choices = data.get("choices") or []
    if not choices:
        raise ProviderUnavailable(f"provider returned no choices: {str(data)[:300]}")
    choice = choices[0]
    msg = choice.get("message") or {}
    text = msg.get("content") or ""
    if isinstance(text, list):  # some servers return content parts
        text = "".join(str(p.get("text", "")) for p in text if isinstance(p, dict))
    calls: list[ToolCall] = []
    for tc in msg.get("tool_calls") or []:
        fn = tc.get("function") or {}
        args_raw = fn.get("arguments") or "{}"
        try:
            args = json.loads(args_raw) if isinstance(args_raw, str) else dict(args_raw)
            if not isinstance(args, dict):
                args = {"_value": args}
        except json.JSONDecodeError:
            args = {"_raw_arguments": args_raw}
        calls.append(ToolCall(id=str(tc.get("id") or ""), name=str(fn.get("name") or ""), input=args))
    finish = choice.get("finish_reason")
    if finish == "stop":
        stop = "tool_use" if calls else "end_turn"
    elif finish == "tool_calls" or finish == "function_call":
        stop = "tool_use"
    elif finish == "length":
        stop = "max_tokens"
    elif finish == "content_filter":
        stop = "refusal"
    else:
        stop = "other" if finish else ("tool_use" if calls else "end_turn")
    u = data.get("usage") or {}
    details = u.get("prompt_tokens_details") or {}
    usage = Usage(
        model=str(data.get("model") or model),
        input_tokens=int(u.get("prompt_tokens") or 0),
        output_tokens=int(u.get("completion_tokens") or 0),
        cache_read_tokens=int(details.get("cached_tokens") or 0),
        priced=False,
    )
    native = msg.get(NATIVE_FIELD)
    return Completion(
        text=str(text),
        usage=usage,
        raw=data,
        tool_calls=calls,
        stop_reason=stop,
        request_id=request_id or data.get("id"),
        native=native if isinstance(native, dict) else None,
    )


def _error_message(raw: bytes, status: int) -> str:
    try:
        data = json.loads(raw.decode("utf-8"))
        err = data.get("error")
        if isinstance(err, dict):
            return str(err.get("message") or err)
        if err:
            return str(err)
        return str(data)[:300]
    except Exception:
        return (raw[:300].decode("utf-8", "replace") if raw else f"HTTP {status}").strip()


def _retry_after(headers: dict[str, str]) -> float | None:
    v = headers.get("retry-after")
    if not v:
        return None
    try:
        return max(0.0, min(60.0, float(v)))
    except ValueError:
        return None


__all__ = ["OpenAICompatibleProvider", "ProviderError", "to_openai_messages", "to_openai_tool", "response_to_completion"]


# ----------------------------------------------------------------------------- server side of the same wire (gateway)


def from_openai_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """OpenAI Chat Completions messages → provider-neutral messages (the inverse of to_openai_messages)."""
    out: list[dict[str, Any]] = []
    for m in messages:
        role = m.get("role")
        content = m.get("content")
        if isinstance(content, list):  # content parts → text
            content = "".join(str(p.get("text", "")) for p in content if isinstance(p, dict))
        text = "" if content is None else str(content)
        if role in ("system", "user", "developer"):
            out.append({"role": "system" if role == "developer" else role, "content": text})
        elif role == "assistant":
            entry: dict[str, Any] = {"role": "assistant", "content": text}
            calls = []
            for tc in m.get("tool_calls") or []:
                fn = tc.get("function") or {}
                args_raw = fn.get("arguments") or "{}"
                try:
                    args = json.loads(args_raw) if isinstance(args_raw, str) else dict(args_raw)
                    if not isinstance(args, dict):
                        args = {"_value": args}
                except json.JSONDecodeError:
                    args = {"_raw_arguments": args_raw}
                calls.append({"id": str(tc.get("id") or ""), "name": str(fn.get("name") or ""), "input": args})
            if calls:
                entry["tool_calls"] = calls
            if isinstance(m.get(NATIVE_FIELD), dict):
                entry["native"] = m[NATIVE_FIELD]
            out.append(entry)
        elif role == "tool":
            is_error = text.startswith("ERROR: ")
            out.append(
                {
                    "role": "tool",
                    "tool_call_id": str(m.get("tool_call_id") or ""),
                    "content": text[len("ERROR: ") :] if is_error else text,
                    "is_error": is_error,
                }
            )
        else:
            raise ValueError(f"unknown message role {role!r}")
    return out


def from_openai_tools(tools: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for t in tools or []:
        fn = t.get("function") if t.get("type") == "function" else t
        if not isinstance(fn, dict) or not fn.get("name"):
            continue
        out.append(
            {
                "name": str(fn["name"]),
                "description": str(fn.get("description", "")),
                "input_schema": dict(fn.get("parameters") or {"type": "object", "properties": {}}),
            }
        )
    return out


def to_openai_response(comp: Completion, *, model: str, response_id: str) -> dict[str, Any]:
    """Completion → OpenAI Chat Completions response JSON (what a client of the gateway receives)."""
    msg: dict[str, Any] = {"role": "assistant", "content": comp.text if (comp.text or not comp.tool_calls) else None}
    if comp.tool_calls:
        msg["tool_calls"] = [
            {"id": tc.id, "type": "function", "function": {"name": tc.name, "arguments": json.dumps(tc.input)}}
            for tc in comp.tool_calls
        ]
    if comp.native:
        msg[NATIVE_FIELD] = comp.native
    finish = {"end_turn": "stop", "tool_use": "tool_calls", "max_tokens": "length", "refusal": "content_filter"}.get(
        comp.stop_reason, "stop"
    )
    u = comp.usage
    return {
        "id": response_id,
        "object": "chat.completion",
        "model": u.model or model,
        "choices": [{"index": 0, "message": msg, "finish_reason": finish}],
        "usage": {
            "prompt_tokens": u.input_tokens,
            "completion_tokens": u.output_tokens,
            "total_tokens": u.input_tokens + u.output_tokens,
            "prompt_tokens_details": {"cached_tokens": u.cache_read_tokens},
        },
    }
