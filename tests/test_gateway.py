"""Step 10 — model gateway: the one process holding a vendor key. Offline: a fake upstream behind a real HTTP loopback
server, the ordinary `openai:` provider as the client, and the whole LLM loop across the wire."""

from __future__ import annotations

import json
import threading
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from mas import providers
from mas.providers.base import ProviderRateLimited, ProviderRequestError, ProviderUnavailable, ToolCall
from mas.providers.fake import FakeProvider
from mas.providers.gateway import ModelGateway
from mas.providers.openai_compat import OpenAICompatibleProvider, from_openai_messages, from_openai_tools, to_openai_response
from mas.providers.telemetry import MemorySink, MeteredProvider
from mas.workers.base import TaskContext
from mas.workers.llm import LLMAgent


@pytest.fixture
def gw():
    """Gateway on 127.0.0.1:<random>, upstream = scripted fake with tool calls, bearer token required."""
    upstream = FakeProvider(
        [
            {"text": "", "tool_calls": [{"id": "c1", "name": "ls", "input": {"path": "."}}]},
            "all done",
            ProviderRateLimited("slow down", retry_after_s=7),
            ProviderRequestError("bad model"),
            ProviderUnavailable("down"),
            {"text": "x" * 4000, "stop_reason": "max_tokens"},
        ],
        model="fake-up",
        input_tokens=11,
        output_tokens=3,
    )
    g = ModelGateway(upstream, allowed_models=["front", "front-2"], token="secret", max_body_bytes=20_000)
    t = g.start()
    yield g, upstream
    g.close()
    t.join(5)


def _client(g: ModelGateway, *, key="secret", model="front", **kw) -> OpenAICompatibleProvider:
    return OpenAICompatibleProvider(model, base_url=g.base_url, api_key=key, max_retries=0, sleep=lambda s: None, **kw)


def test_round_trip_tool_calls_and_usage(gw):
    g, upstream = gw
    p = _client(g)
    tools = [
        {"name": "ls", "description": "list", "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}}}
    ]
    convo = [{"role": "system", "content": "be terse"}, {"role": "user", "content": "list"}]
    c = p.complete(convo, tools=tools, max_tokens=50)
    assert c.stop_reason == "tool_use" and c.tool_calls == [ToolCall("c1", "ls", {"path": "."})]
    assert c.usage.model == "fake-up" and c.usage.input_tokens == 11 and c.usage.output_tokens == 3 and not c.usage.priced
    assert c.request_id and c.request_id.startswith("gw-")
    # what the upstream saw is the neutral shape, faithfully: system + user, the tool schema, our max_tokens
    seen = upstream.calls[0]
    assert seen["messages"] == convo and seen["tools"] == tools and seen["max_tokens"] == 50
    # tool results and assistant tool_calls survive the wire in both directions
    convo2 = convo + [c.as_message(), {"role": "tool", "tool_call_id": "c1", "content": "a.py", "is_error": True}]
    c2 = p.complete(convo2)
    assert c2.text == "all done" and upstream.calls[1]["messages"][-1] == {
        "role": "tool",
        "tool_call_id": "c1",
        "content": "a.py",
        "is_error": True,
    }
    assert upstream.calls[1]["messages"][-2]["tool_calls"] == [{"id": "c1", "name": "ls", "input": {"path": "."}}]
    assert g.stats["ok"] == 2


def test_gateway_maps_upstream_errors_and_bounds_requests(gw):
    g, upstream = gw
    p = _client(g)
    for _ in range(2):  # consume the two successful script items
        upstream._pos += 1
    with pytest.raises(ProviderRateLimited) as ei:
        p.complete([{"role": "user", "content": "x"}])
    assert ei.value.retry_after_s == 7.0
    with pytest.raises(ProviderRequestError):
        p.complete([{"role": "user", "content": "x"}])
    with pytest.raises(ProviderUnavailable):
        p.complete([{"role": "user", "content": "x"}])
    c = p.complete([{"role": "user", "content": "x"}], max_tokens=10)  # max_tokens → length → max_tokens
    assert c.stop_reason == "max_tokens"
    # auth, allow-list, size, streaming
    with pytest.raises(ProviderRequestError, match="401"):
        _client(g, key="wrong").complete([{"role": "user", "content": "x"}])
    with pytest.raises(ProviderRequestError, match="not allowed"):
        _client(g, model="claude-secret").complete([{"role": "user", "content": "x"}])
    with pytest.raises(ProviderRequestError, match="413"):
        _client(g).complete([{"role": "user", "content": "y" * 30_000}])
    req = urllib.request.Request(
        g.base_url + "/chat/completions",
        data=json.dumps({"model": "front", "messages": [{"role": "user", "content": "x"}], "stream": True}).encode(),
        headers={"Authorization": "Bearer secret", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=5)
        raise AssertionError("streaming should be refused")
    except urllib.error.HTTPError as e:
        assert e.code == 400 and b"streaming" in e.read()
    health = json.loads(urllib.request.urlopen(g.base_url.replace("/v1", "/healthz"), timeout=5).read())
    assert health["ok"] and health["upstream"] == "fake" and health["models"] == ["front", "front-2"]
    assert g.stats["rejected"] >= 7 and g.stats["upstream_errors"] == 3


def test_converters_are_inverses():
    neutral = [
        {"role": "system", "content": "s"},
        {"role": "user", "content": "u"},
        {"role": "assistant", "content": "", "tool_calls": [{"id": "a", "name": "f", "input": {"k": 1}}]},
        {"role": "tool", "tool_call_id": "a", "content": "boom", "is_error": True},
        {"role": "tool", "tool_call_id": "a", "content": "fine", "is_error": False},
        {"role": "assistant", "content": "final"},
    ]
    from mas.providers.openai_compat import to_openai_messages, to_openai_tool

    assert from_openai_messages(to_openai_messages(neutral)) == neutral
    tools = [{"name": "f", "description": "d", "input_schema": {"type": "object", "properties": {"k": {"type": "integer"}}}}]
    assert from_openai_tools([to_openai_tool(t) for t in tools]) == tools
    from mas.providers.base import Completion, Usage

    comp = Completion(
        text="",
        usage=Usage(model="m", input_tokens=1, output_tokens=2),
        tool_calls=[ToolCall("i", "f", {"k": 1})],
        stop_reason="tool_use",
    )
    resp = to_openai_response(comp, model="front", response_id="gw-1")
    assert resp["choices"][0]["finish_reason"] == "tool_calls" and resp["choices"][0]["message"]["content"] is None
    assert json.loads(resp["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"]) == {"k": 1}


def test_llm_loop_through_the_gateway_with_the_builder_upstream(tmp_path: Path):
    """The compose shape for models: worker → openai: provider → gateway → fake:builder — the whole loop across the wire."""
    upstream = providers.from_spec("fake:builder")
    g = ModelGateway(upstream, allowed_models=["builder"], token="t")
    t = g.start()
    try:
        client = OpenAICompatibleProvider("builder", base_url=g.base_url, api_key="t", max_retries=0)
        sink = MemorySink()
        model = MeteredProvider(client, sink=sink)
        root = tmp_path / "wt"
        root.mkdir()
        task = SimpleNamespace(
            key="T1",
            capability="architecture",
            goal="design it",
            output_contract={"artifacts": ["document:design.md"]},
            context_spec={},
            meta={},
            tools=["filesystem", "model"],
        )
        ctx = TaskContext(
            run=SimpleNamespace(id=uuid4(), goal="a small service"),
            task=task,  # type: ignore[arg-type]
            attempt=SimpleNamespace(id=uuid4(), attempt_number=1),  # type: ignore[arg-type]
            inputs=[],
            workspace=root,
            cancel=threading.Event(),
            tools=["filesystem", "model"],
            model=model,
        )
        res = LLMAgent().execute(ctx)
        assert res.success, res
        assert (root / "docs" / "design.md").exists()
        assert [a.ref for a in res.artifacts if a.type == "document"] == ["path:docs/design.md"]
        assert len(sink.records) == 2 and all(r.status == "ok" for r in sink.records)  # write_file turn + finish turn
        assert sink.records[0].model == "builder" and g.stats["ok"] == 2
    finally:
        g.close()
        t.join(5)
