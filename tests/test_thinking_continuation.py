"""Thinking continuation across a tool round (Anthropic adaptive thinking), directly and through the gateway.

The Messages API returns signed `thinking` / `redacted_thinking` blocks ahead of every `tool_use` and requires that exact
assistant turn back — blocks unchanged, thinking first — together with the tool results; a turn rebuilt from text +
tool_calls alone is a 400 on the worker's SECOND model call. These tests pin the whole path offline: the provider keeps
the turn as `Completion.native`, `as_message()` carries it, the translator replays it verbatim, and the OpenAI-shaped
gateway wire forwards it (`mas_native`) in both directions so a compose worker continues exactly like a direct client.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from mas.providers.anthropic_provider import AnthropicProvider, message_to_completion, to_anthropic_messages
from mas.providers.base import Completion, ToolCall, Usage
from mas.providers.gateway import ModelGateway
from mas.providers.openai_compat import (
    NATIVE_FIELD,
    OpenAICompatibleProvider,
    _urllib_transport,
    from_openai_messages,
    response_to_completion,
    to_openai_messages,
    to_openai_response,
)
from mas.providers.telemetry import MemorySink, MeteredProvider
from mas.workers.base import TaskContext
from mas.workers.llm import LLMAgent

# ----------------------------------------------------------------------------- SDK-shaped doubles


def _thinking(signature: str, text: str = "") -> SimpleNamespace:
    # `display: omitted` (the API default) returns thinking blocks with EMPTY text but a signature; both come back as-is
    return SimpleNamespace(type="thinking", thinking=text, signature=signature)


def _redacted(data: str) -> SimpleNamespace:
    return SimpleNamespace(type="redacted_thinking", data=data)


def _text(text: str) -> SimpleNamespace:
    return SimpleNamespace(type="text", text=text, citations=None)  # citations=None like the SDK object; never replayed


def _tool_use(tid: str, name: str, **inp) -> SimpleNamespace:
    return SimpleNamespace(type="tool_use", id=tid, name=name, input=inp)


def _sdk_message(*blocks, stop_reason: str = "tool_use", model: str = "m-2026") -> SimpleNamespace:
    return SimpleNamespace(
        model=model,
        content=list(blocks),
        stop_reason=stop_reason,
        usage=SimpleNamespace(input_tokens=10, output_tokens=5, cache_read_input_tokens=0, cache_creation_input_tokens=0),
        _request_id="req_x",
    )


class _ScriptedMessages:
    """`client.messages` double: hands out scripted responses in order and records every request's params."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.requests: list[dict] = []

    def _next(self, params):
        self.requests.append(params)
        if not self.responses:
            raise AssertionError("script exhausted")
        return self.responses.pop(0)

    def create(self, **params):
        return self._next(params)

    def stream(self, **params):
        resp = self._next(params)

        class _Ctx:
            def __enter__(self_inner):
                return SimpleNamespace(get_final_message=lambda: resp)

            def __exit__(self_inner, *a):
                return False

        return _Ctx()


def _client(responses) -> SimpleNamespace:
    m = _ScriptedMessages(responses)
    return SimpleNamespace(messages=m, beta=SimpleNamespace(messages=m))


TURN1 = [_thinking("sig-1"), _tool_use("tu_1", "write_file", path="docs/design.md", content="# design\n")]
TURN1_NATIVE = [
    {"type": "thinking", "thinking": "", "signature": "sig-1"},
    {"type": "tool_use", "id": "tu_1", "name": "write_file", "input": {"path": "docs/design.md", "content": "# design\n"}},
]
TURN2 = [
    _thinking("sig-2", "verified the file"),
    _redacted("opaque-bytes"),
    _text("Done."),
    _tool_use(
        "tu_2",
        "finish",
        success=True,
        summary="wrote the design",
        artifacts=[{"type": "document", "path": "docs/design.md", "name": "design.md"}],
    ),
]


# ----------------------------------------------------------------------------- translation (pure)


def test_native_turn_is_captured_carried_and_replayed_verbatim():
    c = message_to_completion(
        _sdk_message(_thinking("sig-1"), _redacted("opaque"), _text("Let me look."), _tool_use("tu_1", "ls", path="/"))
    )
    assert c.tool_calls == [ToolCall("tu_1", "ls", {"path": "/"})] and c.text == "Let me look."
    expected_native = [
        {"type": "thinking", "thinking": "", "signature": "sig-1"},
        {"type": "redacted_thinking", "data": "opaque"},
        {"type": "text", "text": "Let me look."},  # citations=None is not echoed
        {"type": "tool_use", "id": "tu_1", "name": "ls", "input": {"path": "/"}},
    ]
    assert c.native == {"provider": "anthropic", "content": expected_native}
    turn = c.as_message()
    assert turn["native"] == c.native and turn["tool_calls"] == [{"id": "tu_1", "name": "ls", "input": {"path": "/"}}]

    _, msgs = to_anthropic_messages(
        [{"role": "user", "content": "go"}, turn, {"role": "tool", "tool_call_id": "tu_1", "content": "a.py"}]
    )
    # the assistant turn is exactly the blocks the API returned — thinking first, unchanged — then the tool results
    assert msgs[1] == {"role": "assistant", "content": expected_native}
    assert msgs[2] == {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "tu_1", "content": "a.py"}]}


def test_translation_falls_back_when_native_is_foreign_thinking_only_or_absent():
    calls = [{"id": "c1", "name": "ls", "input": {}}]
    rebuilt = [{"type": "text", "text": "hi"}, {"type": "tool_use", "id": "c1", "name": "ls", "input": {}}]
    # another provider's native blocks are not ours to replay: rebuild from text + tool_calls
    foreign_native = {"provider": "other", "content": [{"type": "x"}]}
    foreign = {"role": "assistant", "content": "hi", "tool_calls": calls, "native": foreign_native}
    assert to_anthropic_messages([foreign])[1] == [{"role": "assistant", "content": rebuilt}]
    # a thinking-only turn (cut off by max_tokens before any output) has nothing to replay; an empty turn is dropped —
    # the API rejects empty text and merges consecutive same-role turns
    only_thinking = {"provider": "anthropic", "content": [{"type": "thinking", "thinking": "", "signature": "s"}]}
    thinking_only = {"role": "assistant", "content": "", "native": only_thinking}
    around = [{"role": "user", "content": "u"}, thinking_only, {"role": "user", "content": "continue"}]
    assert to_anthropic_messages(around)[1] == [{"role": "user", "content": "u"}, {"role": "user", "content": "continue"}]
    assert to_anthropic_messages([{"role": "assistant", "content": ""}])[1] == []
    # no native at all (an older conversation, a fake provider): unchanged behaviour
    plain = {"role": "assistant", "content": "hi", "tool_calls": calls}
    assert to_anthropic_messages([plain])[1] == [{"role": "assistant", "content": rebuilt}]
    # a completion without native blocks does not grow the neutral message
    assert "native" not in Completion(text="t", usage=Usage(model="m")).as_message()


def test_openai_wire_carries_native_both_ways_only_when_present():
    native = {"provider": "anthropic", "content": TURN1_NATIVE}
    calls = [{"id": "tu_1", "name": "write_file", "input": {"path": "p", "content": "c"}}]
    neutral = [
        {"role": "user", "content": "u"},
        {"role": "assistant", "content": "", "tool_calls": calls, "native": native},
        {"role": "tool", "tool_call_id": "tu_1", "content": "ok", "is_error": False},
        {"role": "assistant", "content": "final"},
    ]
    wire = to_openai_messages(neutral)
    assert wire[1][NATIVE_FIELD] == native and NATIVE_FIELD not in wire[3]  # only turns that have it
    assert from_openai_messages(wire) == neutral  # inverse, native included
    comp = Completion(
        text="",
        usage=Usage(model="m", input_tokens=1, output_tokens=2),
        tool_calls=[ToolCall("tu_1", "write_file", {})],
        stop_reason="tool_use",
        native=native,
    )
    resp = to_openai_response(comp, model="front", response_id="gw-1")
    assert resp["choices"][0]["message"][NATIVE_FIELD] == native
    back = response_to_completion(json.loads(json.dumps(resp)))
    assert back.native == native and back.as_message()["native"] == native
    plain = to_openai_response(Completion(text="t", usage=Usage(model="m")), model="front", response_id="gw-2")
    assert NATIVE_FIELD not in plain["choices"][0]["message"] and response_to_completion(plain).native is None


# ----------------------------------------------------------------------------- the worker's second call, end to end


def _ctx(model, root: Path) -> TaskContext:
    task = SimpleNamespace(
        key="T1",
        capability="architecture",
        goal="design it",
        output_contract={"artifacts": ["document:design.md"]},
        context_spec={},
        meta={},
        tools=["filesystem", "model"],
    )
    return TaskContext(
        run=SimpleNamespace(id=uuid4(), goal="a small service"),
        task=task,  # type: ignore[arg-type]
        attempt=SimpleNamespace(id=uuid4(), attempt_number=1),  # type: ignore[arg-type]
        inputs=[],
        workspace=root,
        cancel=threading.Event(),
        tools=["filesystem", "model"],
        model=model,
    )


def _assert_second_call_replays_turn_one(requests: list[dict]) -> None:
    assert len(requests) == 2
    first, second = requests
    assert first["thinking"] == {"type": "adaptive"} and second["thinking"] == {"type": "adaptive"}
    # the second request replays turn 1 exactly as received: signed thinking block first, then the tool_use…
    assert second["messages"][-2] == {"role": "assistant", "content": TURN1_NATIVE}
    # …and the tool results follow in one user message, keyed by the same tool_use id
    results = second["messages"][-1]
    assert results["role"] == "user" and [b["type"] for b in results["content"]] == ["tool_result"]
    assert results["content"][0]["tool_use_id"] == "tu_1" and "is_error" not in results["content"][0]
    # the prefix is untouched (system hoisted, brief as the first user turn)
    assert second["messages"][:-2] == first["messages"] and second["system"] == first["system"]


def test_llm_loop_direct_replays_thinking_blocks_with_tool_results(tmp_path: Path):
    client = _client([_sdk_message(*TURN1), _sdk_message(*TURN2)])
    provider = AnthropicProvider("m", client=client)
    sink = MemorySink()
    root = tmp_path / "wt"
    root.mkdir()
    res = LLMAgent().execute(_ctx(MeteredProvider(provider, sink=sink), root))
    assert res.success, res
    assert (root / "docs" / "design.md").read_text(encoding="utf-8") == "# design\n"
    _assert_second_call_replays_turn_one(client.messages.requests)
    assert len(sink.records) == 2 and all(r.status == "ok" for r in sink.records)


def test_llm_loop_through_the_gateway_replays_thinking_blocks_with_tool_results(tmp_path: Path):
    """compose shape: worker → openai: provider → gateway → anthropic upstream; the wire must not lose the turn."""
    client = _client([_sdk_message(*TURN1), _sdk_message(*TURN2)])
    upstream = AnthropicProvider("m", client=client)
    g = ModelGateway(upstream, allowed_models=["front"], token="t")
    thread = g.start()
    bodies: list[dict] = []

    def transport(url, body, headers, timeout_s):
        bodies.append(json.loads(body))
        return _urllib_transport(url, body, headers, timeout_s)

    try:
        worker_side = OpenAICompatibleProvider("front", base_url=g.base_url, api_key="t", max_retries=0, transport=transport)
        sink = MemorySink()
        root = tmp_path / "wt"
        root.mkdir()
        res = LLMAgent().execute(_ctx(MeteredProvider(worker_side, sink=sink), root))
        assert res.success, res
        assert (root / "docs" / "design.md").exists()
        # what the vendor SDK double saw is identical to the direct case
        _assert_second_call_replays_turn_one(client.messages.requests)
        # and the wire carried the native turn: absent on the first request, present on the second, restored upstream
        assert len(bodies) == 2 and not any(NATIVE_FIELD in m for m in bodies[0]["messages"])
        replayed = [m for m in bodies[1]["messages"] if m["role"] == "assistant"]
        assert len(replayed) == 1 and replayed[0][NATIVE_FIELD] == {"provider": "anthropic", "content": TURN1_NATIVE}
        assert g.stats["ok"] == 2 and sink.records[0].model == "m-2026"
    finally:
        g.close()
        thread.join(5)
