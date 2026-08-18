"""Step 9 — ModelProvider, pricing, telemetry meter, concrete providers (all offline: fake clients / transports)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from mas import providers
from mas.providers.anthropic_provider import (
    AnthropicProvider,
    message_to_completion,
    to_anthropic_messages,
    to_anthropic_tool,
)
from mas.providers.base import Completion, ProviderRateLimited, ProviderRequestError, ProviderUnavailable, ToolCall, Usage
from mas.providers.fake import FakeProvider
from mas.providers.openai_compat import OpenAICompatibleProvider, response_to_completion, to_openai_messages, to_openai_tool
from mas.providers.pricing import Price, Pricing
from mas.providers.telemetry import AttemptBudgetExceeded, CallBudget, MemorySink, MeteredProvider

CONVO = [
    {"role": "system", "content": "You are terse."},
    {"role": "user", "content": "list files"},
    {"role": "assistant", "content": "", "tool_calls": [{"id": "c1", "name": "ls", "input": {"path": "."}}]},
    {"role": "tool", "tool_call_id": "c1", "content": "a.py\nb.py"},
    {"role": "user", "content": "thanks"},
]
TOOLS = [
    {
        "name": "ls",
        "description": "list a directory",
        "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}},
    }
]


# ----------------------------------------------------------------------------- Usage / Pricing


def test_usage_addition_and_pricing_flags():
    a = Usage(model="m", input_tokens=10, output_tokens=5, cost_usd=0.01)
    b = Usage(model="m", input_tokens=1, output_tokens=1, cost_usd=0.001, cache_read_tokens=7)
    s = a + b
    assert (s.input_tokens, s.output_tokens, s.cache_read_tokens, s.total_tokens) == (11, 6, 7, 17)
    assert s.model == "m" and s.priced and abs(s.cost_usd - 0.011) < 1e-9
    assert (a + Usage(model="other")).model == "mixed"
    u = Usage(model="x", input_tokens=1, priced=False).with_cost(0.5)
    assert u.priced and u.cost_usd == 0.5
    assert not Usage(model="x", cost_usd=1.0).with_cost(None).priced


def test_pricing_from_json_exact_prefix_and_short_form():
    p = Pricing.from_json(
        json.dumps(
            {
                "_comment": "ignored",
                "vendor-big": {"input": 5, "output": 25, "cache_read": 0.5, "cache_write": 6.25},
                "vendor-small": [1.0, 5.0],
            }
        )
    )
    assert len(p) == 2 and p.known_models() == ["vendor-big", "vendor-small"]
    assert p.price("vendor-big") == Price(5, 25, 0.5, 6.25)
    assert p.price("vendor-big-20260101") == Price(5, 25, 0.5, 6.25)  # prefix match: dated id reported back
    assert p.price("vendor-small") == Price(1.0, 5.0)
    assert p.price("unknown") is None and p.cost("unknown", input_tokens=10) is None
    # 1M in @5 + 1M out @25 + 1M cache-read @0.5 = 30.5
    assert p.cost("vendor-big", input_tokens=1_000_000, output_tokens=1_000_000, cache_read_tokens=1_000_000) == 30.5
    # short form: cache tokens billed as input
    assert p.cost("vendor-small", cache_read_tokens=1_000_000) == 1.0
    assert len(Pricing.from_json("")) == 0 and len(Pricing.from_json(None)) == 0


def test_pricing_rejects_garbage():
    with pytest.raises(ValueError):
        Pricing.from_json("{not json")
    with pytest.raises(ValueError):
        Pricing.from_json('{"m": {"input": 1}}')
    with pytest.raises(ValueError):
        Pricing.from_json('["m"]')


# ----------------------------------------------------------------------------- FakeProvider


def test_fake_provider_scripts_and_usage():
    boom = ProviderRateLimited("slow down")
    fp = FakeProvider(
        [
            "hello",
            {"text": "", "tool_calls": [{"id": "t1", "name": "ls", "input": {"path": "."}}]},
            boom,
            lambda messages, tools: f"seen {len(messages)} messages",
        ]
    )
    c1 = fp.complete([{"role": "user", "content": "hi"}])
    assert c1.text == "hello" and c1.stop_reason == "end_turn" and c1.usage.priced and c1.usage.cost_usd > 0
    c2 = fp.complete([{"role": "user", "content": "hi"}], tools=TOOLS)
    assert c2.stop_reason == "tool_use" and c2.tool_calls == [ToolCall("t1", "ls", {"path": "."})]
    assert c2.as_message()["tool_calls"][0]["name"] == "ls"
    with pytest.raises(ProviderRateLimited):
        fp.complete([{"role": "user", "content": "hi"}])
    c4 = fp.complete(CONVO)
    assert c4.text == "seen 5 messages"
    assert fp.complete(CONVO).text == "seen 5 messages"  # exhausted → last item repeats
    assert len(fp.calls) == 5
    # max_tokens is honoured (truncation is visible, never silent)
    short = FakeProvider(["x" * 400], chars_per_token=4).complete([{"role": "user", "content": "go"}], max_tokens=10)
    assert short.stop_reason == "max_tokens" and len(short.text) == 40
    fixed = FakeProvider("ok", input_tokens=100, output_tokens=10, cost_per_mtok=(1.0, 5.0)).complete(CONVO)
    assert fixed.usage.input_tokens == 100 and abs(fixed.usage.cost_usd - 0.00015) < 1e-12


# ----------------------------------------------------------------------------- MeteredProvider


def test_meter_records_prices_sums_and_enforces_call_budget():
    sink = MemorySink()
    inner = FakeProvider(["a", "bb", "ccc"], input_tokens=100, output_tokens=10, model="vendor-x")
    inner.cost_per_mtok = (0.0, 0.0)  # unpriced by the provider...
    pricing = Pricing({"vendor-x": Price(10.0, 20.0)})  # ...priced by config
    rid, tid, aid = uuid4(), uuid4(), uuid4()
    m = MeteredProvider(
        inner, sink=sink, pricing=pricing, role="worker", run_id=rid, task_id=tid, attempt_id=aid, budget=CallBudget(max_calls=2)
    )
    c = m.complete([{"role": "user", "content": "x"}])
    assert c.usage.priced and abs(c.usage.cost_usd - (100 * 10 + 10 * 20) / 1e6) < 1e-12
    m.complete([{"role": "user", "content": "y"}], tools=TOOLS)
    with pytest.raises(AttemptBudgetExceeded):
        m.complete([{"role": "user", "content": "z"}])
    assert m.calls == 2 and m.remaining_calls == 0 and len(inner.calls) == 2  # the refused call never reached the model
    assert m.total.input_tokens == 200 and m.total.output_tokens == 20 and m.total.priced
    d = m.usage_dict()
    assert d["calls"] == 2 and d["input_tokens"] == 200 and d["model"] == "vendor-x" and abs(d["cost_usd"] - 0.0024) < 1e-9
    # two calls + the recorded refusal (status "budget", zero tokens, seq of the call that did not happen)
    assert [r.seq for r in sink.records] == [1, 2, 3] and sink.records[1].meta["tools"] == 1
    assert sink.records[2].status == "budget" and sink.records[2].input_tokens == 0 and "call budget" in sink.records[2].error
    r = sink.records[0]
    assert (r.run_id, r.task_id, r.attempt_id, r.role, r.provider, r.model, r.status) == (
        rid,
        tid,
        aid,
        "worker",
        "fake",
        "vendor-x",
        "ok",
    )
    assert r.priced and r.duration_ms >= 0 and r.as_dict()["run_id"] == str(rid)


def test_meter_token_budget_errors_and_statuses():
    sink = MemorySink()
    inner = FakeProvider(
        ["ok", ProviderUnavailable("down"), {"text": "", "stop_reason": "refusal"}, "x" * 4000],
        input_tokens=60,
        output_tokens=10,
    )
    m = MeteredProvider(inner, sink=sink, budget=CallBudget(max_tokens=200))
    m.complete([{"role": "user", "content": "1"}])  # 70 tokens
    with pytest.raises(ProviderUnavailable):
        m.complete([{"role": "user", "content": "2"}])  # error: recorded, re-raised, counts as a call
    assert m.errors == 1 and sink.records[1].status == "error" and "ProviderUnavailable" in sink.records[1].error
    r = m.complete([{"role": "user", "content": "3"}])  # refusal → status refusal (140 tokens)
    assert r.refused and sink.records[2].status == "refusal"
    t = m.complete([{"role": "user", "content": "4"}], max_tokens=5)  # 210 tokens → over budget for the *next* call
    assert t.stop_reason == "max_tokens" and sink.records[3].status == "max_tokens"
    assert m.remaining_tokens == 0
    with pytest.raises(AttemptBudgetExceeded):
        m.complete([{"role": "user", "content": "5"}])
    assert m.calls == 4 and len(sink.records) == 5  # the refused call is not a call — but it is on record
    assert sink.records[4].status == "budget" and "token budget" in sink.records[4].error
    # unpriced provider stays unpriced without a price table (fake prices itself, so priced=True here)
    assert m.total.priced


# ----------------------------------------------------------------------------- Anthropic translation (offline)


def test_anthropic_message_translation_hoists_system_and_folds_tool_results():
    system, msgs = to_anthropic_messages(
        CONVO
        + [
            {
                "role": "assistant",
                "content": "two calls",
                "tool_calls": [{"id": "a", "name": "ls", "input": {}}, {"id": "b", "name": "ls", "input": {}}],
            },
            {"role": "tool", "tool_call_id": "a", "content": "ok"},
            {"role": "tool", "tool_call_id": "b", "content": "nope", "is_error": True},
        ]
    )
    assert system == "You are terse."
    assert msgs[0] == {"role": "user", "content": "list files"}
    assert msgs[1] == {"role": "assistant", "content": [{"type": "tool_use", "id": "c1", "name": "ls", "input": {"path": "."}}]}
    assert msgs[2] == {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "c1", "content": "a.py\nb.py"}]}
    assert msgs[3] == {"role": "user", "content": "thanks"}
    assert msgs[4]["content"][0] == {"type": "text", "text": "two calls"} and len(msgs[4]["content"]) == 3
    # both results of the parallel round travel in ONE user message; the error flag is preserved
    assert [b["tool_use_id"] for b in msgs[5]["content"]] == ["a", "b"] and msgs[5]["content"][1]["is_error"] is True
    assert to_anthropic_tool(TOOLS[0]) == TOOLS[0]
    with pytest.raises(ValueError):
        to_anthropic_messages([{"role": "narrator", "content": "x"}])


def test_anthropic_build_params_and_response_mapping():
    p = AnthropicProvider("some-model", client=object(), effort="high")
    params = p.build_params(CONVO, max_tokens=1234, tools=TOOLS)
    assert params["model"] == "some-model" and params["max_tokens"] == 1234 and params["system"] == "You are terse."
    assert params["thinking"] == {"type": "adaptive"} and params["output_config"] == {"effort": "high"}
    assert params["tools"] == TOOLS and "temperature" not in params
    assert "thinking" not in AnthropicProvider("m", client=object(), thinking=False).build_params([], max_tokens=1, tools=None)

    msg = SimpleNamespace(
        model="some-model-20260101",
        content=[
            SimpleNamespace(type="thinking", thinking=""),
            SimpleNamespace(type="text", text="Hello "),
            SimpleNamespace(type="text", text="world"),
            SimpleNamespace(type="tool_use", id="tu_1", name="ls", input={"path": "/"}),
        ],
        stop_reason="tool_use",
        usage=SimpleNamespace(input_tokens=11, output_tokens=7, cache_read_input_tokens=3, cache_creation_input_tokens=None),
        _request_id="req_123",
    )
    c = message_to_completion(msg)
    assert c.text == "Hello world" and c.tool_calls == [ToolCall("tu_1", "ls", {"path": "/"})] and c.stop_reason == "tool_use"
    assert c.usage == Usage(model="some-model-20260101", input_tokens=11, output_tokens=7, cache_read_tokens=3, priced=False)
    assert c.request_id == "req_123"
    refusal = message_to_completion(SimpleNamespace(model="m", content=[], stop_reason="refusal", usage=None))
    assert refusal.refused and refusal.text == ""
    assert (
        message_to_completion(SimpleNamespace(model="m", content=[], stop_reason="pause_turn", usage=None)).stop_reason == "other"
    )


class _FakeAnthropicMessages:
    def __init__(self, response):
        self.response = response
        self.created: list[dict] = []
        self.streamed: list[dict] = []

    def create(self, **params):
        self.created.append(params)
        return self.response

    def stream(self, **params):
        self.streamed.append(params)
        resp = self.response

        class _Ctx:
            def __enter__(self_inner):
                return SimpleNamespace(get_final_message=lambda: resp)

            def __exit__(self_inner, *a):
                return False

        return _Ctx()


def test_anthropic_provider_uses_create_or_stream_and_translates_errors():
    resp = SimpleNamespace(
        model="m",
        content=[SimpleNamespace(type="text", text="ok")],
        stop_reason="end_turn",
        usage=SimpleNamespace(input_tokens=1, output_tokens=1),
    )
    fake = SimpleNamespace(messages=_FakeAnthropicMessages(resp), beta=SimpleNamespace(messages=_FakeAnthropicMessages(resp)))
    p = AnthropicProvider("m", client=fake)
    assert p.complete(CONVO, max_tokens=100, temperature=0.2).text == "ok"  # temperature ignored, not forwarded
    assert len(fake.messages.created) == 1 and "temperature" not in fake.messages.created[0]
    p.complete(CONVO, max_tokens=50_000)
    assert len(fake.messages.streamed) == 1  # large outputs stream
    # opt-in server-side fallbacks go through the beta surface with the beta header + body field
    pf = AnthropicProvider("m", client=fake, fallbacks="default")
    pf.complete(CONVO, max_tokens=10)
    sent = fake.beta.messages.created[0]
    assert sent["extra_body"] == {"fallbacks": "default"} and "server-side-fallback" in sent["extra_headers"]["anthropic-beta"]

    anthropic = pytest.importorskip("anthropic")
    httpx = pytest.importorskip("httpx")

    def _err(cls, status):
        req = httpx.Request("POST", "https://example.invalid/v1/messages")
        return cls("boom", response=httpx.Response(status, request=req, headers={"retry-after": "3"}), body=None)

    class _Raising:
        def __init__(self, exc):
            self.exc = exc

        def create(self, **params):
            raise self.exc

    for exc, expected in [
        (_err(anthropic.RateLimitError, 429), ProviderRateLimited),
        (_err(anthropic.InternalServerError, 503), ProviderUnavailable),
        (_err(anthropic.BadRequestError, 400), ProviderRequestError),
        (anthropic.APIConnectionError(request=httpx.Request("POST", "https://example.invalid")), ProviderUnavailable),
    ]:
        pr = AnthropicProvider("m", client=SimpleNamespace(messages=_Raising(exc)))
        with pytest.raises(expected) as ei:
            pr.complete(CONVO, max_tokens=10)
        assert ei.value.retryable is (expected is not ProviderRequestError)
        if expected is ProviderRateLimited:
            assert ei.value.retry_after_s == 3.0


# ----------------------------------------------------------------------------- OpenAI-compatible (offline transport)


def test_openai_translation_body_and_response():
    p = OpenAICompatibleProvider("gen-model", api_key="k", transport=lambda *a: (200, {}, b"{}"))
    body = p.build_body(CONVO, max_tokens=99, tools=TOOLS, temperature=0.3)
    assert body["model"] == "gen-model" and body["max_completion_tokens"] == 99 and body["temperature"] == 0.3
    assert body["tools"] == [to_openai_tool(TOOLS[0])] and body["tools"][0]["function"]["parameters"] == TOOLS[0]["input_schema"]
    msgs = body["messages"]
    assert msgs[0] == {"role": "system", "content": "You are terse."}
    assert msgs[2]["tool_calls"][0]["function"] == {"name": "ls", "arguments": '{"path": "."}'} and msgs[2]["content"] is None
    assert msgs[3] == {"role": "tool", "tool_call_id": "c1", "content": "a.py\nb.py"}
    assert (
        to_openai_messages([{"role": "tool", "tool_call_id": "z", "content": "bad", "is_error": True}])[0]["content"]
        == "ERROR: bad"
    )
    legacy = OpenAICompatibleProvider("m", max_tokens_field="max_tokens", transport=lambda *a: (200, {}, b"{}"))
    assert legacy.build_body([], max_tokens=5, tools=None, temperature=None) == {"model": "m", "messages": [], "max_tokens": 5}

    data = {
        "id": "chatcmpl-1",
        "model": "gen-model-2026",
        "choices": [
            {
                "finish_reason": "tool_calls",
                "message": {
                    "content": None,
                    "tool_calls": [
                        {"id": "call_1", "type": "function", "function": {"name": "ls", "arguments": '{"path": "/tmp"}'}},
                        {"id": "call_2", "type": "function", "function": {"name": "ls", "arguments": "{oops"}},
                    ],
                },
            }
        ],
        "usage": {"prompt_tokens": 20, "completion_tokens": 4, "prompt_tokens_details": {"cached_tokens": 8}},
    }
    c = response_to_completion(data, request_id="rid")
    assert c.stop_reason == "tool_use" and c.text == "" and c.request_id == "rid"
    assert c.tool_calls[0] == ToolCall("call_1", "ls", {"path": "/tmp"}) and c.tool_calls[1].input == {"_raw_arguments": "{oops"}
    assert c.usage == Usage(model="gen-model-2026", input_tokens=20, output_tokens=4, cache_read_tokens=8, priced=False)
    for finish, stop in [("stop", "end_turn"), ("length", "max_tokens"), ("content_filter", "refusal"), ("weird", "other")]:
        d = {"choices": [{"finish_reason": finish, "message": {"content": "t"}}], "usage": {}}
        assert response_to_completion(d, model="m").stop_reason == stop
    with pytest.raises(ProviderUnavailable):
        response_to_completion({"choices": []})


def test_openai_http_retries_and_error_mapping():
    calls: list[tuple[str, dict]] = []
    sleeps: list[float] = []
    responses: list[tuple[int, dict, bytes]] = []

    def transport(url, body, headers, timeout):
        calls.append((url, json.loads(body)))
        assert headers["Authorization"] == "Bearer k" and headers["X-Extra"] == "1"
        return responses.pop(0)

    ok = json.dumps(
        {
            "choices": [{"finish_reason": "stop", "message": {"content": "fine"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        }
    ).encode()
    p = OpenAICompatibleProvider(
        "m",
        base_url="https://gw.internal/v1/",
        api_key="k",
        extra_headers={"X-Extra": "1"},
        transport=transport,
        sleep=sleeps.append,
        max_retries=2,
    )
    responses[:] = [(429, {"retry-after": "2"}, b'{"error": {"message": "slow"}}'), (500, {}, b"oops"), (200, {}, ok)]
    c = p.complete([{"role": "user", "content": "hi"}], max_tokens=5)
    assert c.text == "fine" and c.usage.priced is False
    assert len(calls) == 3 and calls[0][0] == "https://gw.internal/v1/chat/completions" and sleeps[0] == 2.0
    # retries exhausted → typed, retryable errors
    responses[:] = [(429, {}, b"{}")] * 3
    with pytest.raises(ProviderRateLimited):
        p.complete([{"role": "user", "content": "hi"}])
    responses[:] = [(503, {}, b"{}")] * 3
    with pytest.raises(ProviderUnavailable):
        p.complete([{"role": "user", "content": "hi"}])
    # 4xx is final immediately
    responses[:] = [(401, {}, b'{"error": {"message": "bad key"}}')]
    with pytest.raises(ProviderRequestError) as ei:
        p.complete([{"role": "user", "content": "hi"}])
    assert "bad key" in str(ei.value) and not ei.value.retryable
    # invalid JSON body on 200
    responses[:] = [(200, {}, b"<html>")]
    with pytest.raises(ProviderUnavailable):
        p.complete([{"role": "user", "content": "hi"}])


# ----------------------------------------------------------------------------- registry / config


def test_from_spec_and_roles(monkeypatch):
    assert providers.parse_spec("fake") == ("fake", "") and providers.parse_spec("openai:gpt-x") == ("openai", "gpt-x")
    assert providers.parse_spec(" Anthropic : some-model ") == ("anthropic", "some-model")
    for bad in ["", "gpt-x", "vendor:model", "fake:a:b"[:0]]:
        with pytest.raises(ValueError):
            providers.parse_spec(bad)
    fp = providers.from_spec("fake:unit")
    assert isinstance(fp, FakeProvider) and fp.model == "unit"
    ap = providers.from_spec("anthropic:m", api_key="test-key", client=object())
    assert isinstance(ap, AnthropicProvider) and ap.model == "m"
    assert isinstance(providers.from_spec("anthropic", client=object()), AnthropicProvider)
    op = providers.from_spec("openai:m", cfg=None)
    assert isinstance(op, OpenAICompatibleProvider) and op.max_tokens_field == "max_completion_tokens"
    with pytest.raises(ValueError):
        providers.from_spec("openai")

    monkeypatch.setenv("MAS_MODEL_WORKER", "fake:w")
    monkeypatch.setenv("MAS_MODEL_PLANNER", "")
    monkeypatch.setenv("MAS_MODEL_PRICES", json.dumps({"w": [1, 2]}))
    monkeypatch.setenv("MAS_OPENAI_BASE_URL", "http://gw:8080/v1")
    monkeypatch.setenv("MAS_ATTEMPT_MAX_CALLS", "7")
    from mas.config import settings

    cfg = settings()
    assert providers.role_spec("worker", cfg) == "fake:w" and providers.provider_for_role("planner", cfg) is None
    w = providers.provider_for_role("worker", cfg)
    assert isinstance(w, FakeProvider) and w.model == "w"
    assert providers.pricing_from_settings(cfg).price("w") == Price(1, 2) and cfg.attempt_max_calls == 7
    assert providers.from_spec("openai:m", cfg=cfg).base_url == "http://gw:8080/v1"
    m = providers.meter(w, role="worker", cfg=cfg, budget=CallBudget(max_calls=cfg.attempt_max_calls))
    c = m.complete([{"role": "user", "content": "hi"}])
    assert c.usage.priced and isinstance(c, Completion) and m.remaining_calls == 6
    with pytest.raises(ValueError):
        providers.role_spec("judge", cfg)


# ----------------------------------------------------------------------------- CLI


def test_cli_models_and_ping(monkeypatch, capsys):
    from mas.cli import main

    monkeypatch.setenv("MAS_MODEL_WORKER", "fake:demo")
    monkeypatch.setenv("MAS_MODEL_PLANNER", "")
    monkeypatch.setenv("MAS_MODEL_PRICES", json.dumps({"demo": [1, 5]}))
    assert main(["models"]) == 0
    out = capsys.readouterr().out
    assert "worker    fake:demo" in out and "[unpriced]" not in out and "planner   (none)" in out
    assert main(["models", "--ping"]) == 0
    out = capsys.readouterr().out
    assert "[worker] fake:demo: end_turn 'OK'" in out and '"role": "ping"' in out and '"priced": true' in out
    assert main(["models", "--ping", "--spec", "fake:other"]) == 0
    assert "[adhoc] fake:other" in capsys.readouterr().out
    monkeypatch.setenv("MAS_MODEL_WORKER", "")
    assert main(["models", "--ping"]) == 2  # nothing configured to ping


def test_models_json_output_is_one_parseable_document(monkeypatch, capsys):
    """`--json` is for a caller, not a reader: the roles table, the ping line and the telemetry all go to stderr."""
    from mas.cli import main

    monkeypatch.setenv("MAS_MODEL_PRICES", json.dumps({"probe": [1, 5]}))
    assert main(["models", "--ping", "--probe-tools", "--spec", "fake:probe", "--json"]) == 0
    captured = capsys.readouterr()
    records = json.loads(captured.out)  # would raise if anything human were printed to stdout
    assert [r.get("probe") or "ping" for r in records] == ["ping", "tool_continuation"]
    assert records[0]["priced"] and records[1]["ok"] and records[1]["models"] == ["probe"]
    assert "pricing:" in captured.err and "tool continuation: OK" in captured.err


# ----------------------------------------------------------------------------- tool-continuation probe


def test_tool_continuation_probe_runs_the_two_turn_round_trip_and_reports_it_as_data():
    """The path `--ping` cannot reach: model -> tool call -> tool result -> SECOND call. Two calls, one echo tool."""
    from mas.providers.probe import PROBE_TOOL, probe_tools

    report = probe_tools(providers.from_spec("fake:probe"), pricing=Pricing.from_json(json.dumps({"probe": [1, 5]})))
    assert report["ok"] and all(report["checks"].values())
    # protocol facts decide `ok`; what the model did with the result is observed, not gated
    assert set(report["checks"]) == {
        "asked_for_the_tool",
        "called_the_offered_tool",
        "continuation_accepted",
        "answered_after_the_tool_result",
    }
    assert report["observations"] == {"echoed_nonce_in_the_answer": True, "nonce_sent_to_the_tool": True}
    assert len(report["calls"]) == 2 and report["calls"][0]["meta"]["tools"] == 1
    assert report["models"] == ["probe"] and report["priced"] and report["cost_usd"] > 0
    assert report["nonce"] in report["text"]
    assert PROBE_TOOL["name"] == "mas_probe_echo"  # one harmless tool: reads nothing, writes nothing


def test_the_answer_check_reads_only_the_second_turn_not_the_models_own_tool_call():
    """A provider that dropped the tool result entirely would still have the nonce in the *first* turn's tool call.
    Reading it there would pass a check about the continuation on evidence from before the continuation."""
    from mas.providers.probe import NONCE, probe_tools

    def drops_the_result(messages, tools):
        if messages[-1].get("role") == "tool":
            return "I have nothing to report."  # answered, but never used what came back
        return {
            "text": "",
            "tool_calls": [{"id": "c1", "name": "mas_probe_echo", "input": {"text": NONCE}}],
            "stop_reason": "tool_use",
        }

    report = probe_tools(FakeProvider(drops_the_result))
    assert report["ok"]  # the protocol worked: the continuation was accepted and answered
    assert report["observations"] == {"echoed_nonce_in_the_answer": False, "nonce_sent_to_the_tool": True}


def test_the_probe_reports_a_rejected_continuation_instead_of_raising():
    """A 400 on the second call is the finding the probe exists to produce — cheaply, before a whole worker run."""
    from mas.providers.probe import probe_tools

    def script(messages, tools):
        if messages[-1].get("role") == "tool":
            raise ProviderRequestError("400 messages.1: thinking blocks missing from the assistant turn")
        return {
            "text": "",
            "tool_calls": [{"id": "c1", "name": "mas_probe_echo", "input": {"text": "x"}}],
            "stop_reason": "tool_use",
        }

    report = probe_tools(FakeProvider(script))
    assert not report["ok"] and report["checks"]["continuation_accepted"] is False
    assert "thinking blocks missing" in report["error"] and len(report["calls"]) == 2

    silent = probe_tools(FakeProvider("I will not use tools."))  # never calls the tool: also a failed probe, not a crash
    assert not silent["ok"] and "did not call the tool" in silent["error"]
    assert silent["checks"] == {"asked_for_the_tool": False, "called_the_offered_tool": False}

    broken = probe_tools(FakeProvider([ProviderUnavailable("upstream down")]), pricing=Pricing({"fake-1": Price(1, 5)}))
    assert not broken["ok"] and "first call failed" in broken["error"]
    # a probe where nothing was billed must not announce "$0.00, priced": an empty meter is priced by construction
    assert broken["priced"] is False and broken["cost_usd"] is None


def test_native_summary_describes_a_signed_turn_without_leaking_it():
    """The probe must be able to say "this turn carried signed reasoning we replayed" without vendor blocks escaping
    `mas/providers/` (base.py's contract)."""
    from mas.providers.base import native_summary

    turn = {
        "role": "assistant",
        "content": "",
        "native": {
            "provider": "anthropic",
            "content": [
                {"type": "thinking", "thinking": "...", "signature": "SIGNATURE-PAYLOAD"},
                {"type": "redacted_thinking", "data": "OPAQUE-PAYLOAD"},
                {"type": "tool_use", "id": "t1", "name": "mas_probe_echo", "input": {}},
            ],
        },
    }
    summary = native_summary(turn)
    assert summary == {
        "provider": "anthropic",
        "blocks": 3,
        "block_types": {"redacted_thinking": 1, "thinking": 1, "tool_use": 1},
        "signed_reasoning": 2,
    }
    assert "PAYLOAD" not in json.dumps(summary)  # types and counts leave; signed payloads never do
    assert native_summary({"role": "assistant", "content": "plain"}) is None
