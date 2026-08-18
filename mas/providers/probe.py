"""Two-turn tool-continuation probe: the cheapest live proof that a tool round-trip actually works.

`mas models --ping` proves authentication and one completion. It does **not** exercise the path that actually breaks:

    model response (tool_use) -> tool result -> SECOND model call

That second call is where a provider rejects a malformed continuation — most sharply Anthropic, which requires the
assistant turn to come back *unchanged*, signed `thinking` / `redacted_thinking` blocks included, or answers 400. Our
replay of that turn (`native`, see base.py) is covered by regression tests against SDK-shaped doubles, never against a
live API. Discovering it inside the URL-shortener worker stage costs a whole run; discovering it here costs two small
calls.

The probe is deliberately minimal: one harmless echo tool, at most two calls, tiny output budgets, no repository, no
database, no filesystem. It reports structured telemetry and *what the provider called itself*, so the reported model
id can be compared against the price table before anything expensive starts.

Same-shape gateway check: point `--spec` at the gateway (`openai:<model>` with `MAS_OPENAI_BASE_URL`) and the identical
exchange runs across the wire, which is what workers do in distributed mode.
"""

from __future__ import annotations

import json
from typing import Any

from mas.config import Settings, settings
from mas.providers.base import ModelProvider, native_summary
from mas.providers.pricing import Pricing
from mas.providers.telemetry import CallBudget, MemorySink, MeteredProvider

PROBE_TOOL: dict[str, Any] = {
    "name": "mas_probe_echo",
    "description": "Echo the given text back unchanged. Harmless: it reads nothing and writes nothing.",
    "input_schema": {
        "type": "object",
        "properties": {"text": {"type": "string", "description": "the text to echo back"}},
        "required": ["text"],
    },
}
NONCE = "MAS-PROBE-7Q4"
PROMPT = (
    f"Call the tool `mas_probe_echo` exactly once with text set to {NONCE!r}. "
    "When you receive the tool result, reply with the echoed text and nothing else."
)


def probe_tools(
    provider: ModelProvider,
    *,
    pricing: Pricing | None = None,
    max_tokens: int = 256,
    cfg: Settings | None = None,
) -> dict[str, Any]:
    """Run the two-turn exchange and report what happened, as data. Never raises for a *provider* failure: a 400 on the
    continuation is the answer the probe exists to get, so it is recorded as a failed check with the error text."""
    cfg = cfg or settings()
    sink = MemorySink()
    meter = MeteredProvider(
        provider,
        role="ping",
        sink=sink,
        pricing=pricing,
        budget=CallBudget(max_calls=2),
    )
    result: dict[str, Any] = {
        "probe": "tool_continuation",
        "nonce": NONCE,
        "checks": {},
        "calls": [],
        "models": [],
        "native": None,
        "error": None,
    }

    def finish(ok: bool) -> dict[str, Any]:
        result["calls"] = [r.as_dict() for r in sink.records]
        result["models"] = sorted({r.model for r in sink.records if r.model})
        usage = meter.total
        result["usage"] = usage.as_dict()
        result["priced"] = usage.priced
        result["cost_usd"] = usage.cost_usd if usage.priced else None
        result["ok"] = ok and all(result["checks"].values())
        return result

    messages: list[dict[str, Any]] = [{"role": "user", "content": PROMPT}]
    try:
        first = meter.complete(messages, tools=[PROBE_TOOL], max_tokens=max_tokens)
    except Exception as exc:  # noqa: BLE001 - diagnostic boundary: the failure is the finding
        result["error"] = f"first call failed: {type(exc).__name__}: {exc}"
        return finish(False)
    calls = list(first.tool_calls)
    result["checks"]["asked_for_the_tool"] = first.stop_reason == "tool_use" and len(calls) == 1
    result["checks"]["called_the_offered_tool"] = bool(calls) and calls[0].name == PROBE_TOOL["name"]
    # the assistant turn exactly as the provider produced it — what a continuation must send back unchanged
    turn = first.as_message()
    result["native"] = native_summary(turn)
    if not calls:
        result["error"] = f"the model did not call the tool (stop_reason={first.stop_reason}, text={first.text[:200]!r})"
        return finish(False)
    echoed = str(calls[0].input.get("text", ""))
    messages = [
        *messages,
        turn,
        {"role": "tool", "tool_call_id": calls[0].id, "content": json.dumps({"text": echoed}), "is_error": False},
    ]
    try:
        second = meter.complete(messages, tools=[PROBE_TOOL], max_tokens=max_tokens)
    except Exception as exc:  # noqa: BLE001 - this is the 400 the probe is looking for
        result["error"] = f"continuation failed: {type(exc).__name__}: {exc}"
        result["checks"]["continuation_accepted"] = False
        return finish(False)
    result["checks"]["continuation_accepted"] = True
    result["checks"]["answered_after_the_tool_result"] = second.stop_reason in ("end_turn", "max_tokens")
    result["checks"]["tool_result_reached_the_model"] = NONCE in (second.text or "") or NONCE in echoed
    result["text"] = (second.text or "").strip()[:400]
    return finish(True)
