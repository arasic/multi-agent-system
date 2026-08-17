"""Step 10, part 2 (commit 1) — attempt deadlines and cancellation across model calls, all offline.

The meter refuses before reserving a call when the attempt is cancelled or has less than a minimum viable window left,
hands the provider a `timeout_s` clamped to the remaining runtime, and providers keep their own retries and sleeps
inside that budget (`call_with_retries`) — so no request, retry or Retry-After sleep outlives the attempt."""

from __future__ import annotations

import json
import threading
import time
from types import SimpleNamespace

import pytest

from mas.providers.anthropic_provider import AnthropicProvider
from mas.providers.base import ProviderRateLimited, ProviderRequestError, ProviderUnavailable, call_with_retries
from mas.providers.fake import FakeProvider
from mas.providers.openai_compat import OpenAICompatibleProvider
from mas.providers.telemetry import (
    AttemptCancelled,
    AttemptDeadlineExceeded,
    AttemptEnded,
    CallBudget,
    MemorySink,
    MeteredProvider,
)

MSG = [{"role": "user", "content": "hi"}]


def _meter(inner, **kw) -> tuple[MeteredProvider, MemorySink]:
    sink = MemorySink()
    return MeteredProvider(inner, sink=sink, min_call_s=1.0, **kw), sink


# ----------------------------------------------------------------------------- meter: refusals before the call


def test_deadline_already_exhausted_refuses_before_the_call():
    inner = FakeProvider("late")
    m, sink = _meter(inner, deadline=time.monotonic() - 1.0)
    with pytest.raises(AttemptDeadlineExceeded) as ei:
        m.complete(MSG)
    assert isinstance(ei.value, AttemptEnded) and not ei.value.retryable
    assert inner.calls == [] and m.calls == 0  # nothing reached the provider, nothing was reserved
    assert [r.status for r in sink.records] == ["deadline"] and sink.records[0].input_tokens == 0 and not sink.records[0].priced
    assert sink.records[0].meta["remaining_s"] == 0.0


def test_less_than_a_viable_window_is_also_refused():
    inner = FakeProvider("late")
    m, sink = _meter(inner, deadline=time.monotonic() + 0.5)  # 0.5 s left < min_call_s (1.0)
    with pytest.raises(AttemptDeadlineExceeded):
        m.complete(MSG)
    assert inner.calls == [] and sink.records[0].status == "deadline"


def test_cancellation_before_the_call_and_no_second_call_after_cancel():
    cancel = threading.Event()
    inner = FakeProvider(["first", "second"], input_tokens=10, output_tokens=1)
    m, sink = _meter(inner, cancel=cancel, deadline=time.monotonic() + 60)
    assert m.complete(MSG).text == "first"
    cancel.set()  # reaped / cancelled by the orchestrator between two agent turns
    with pytest.raises(AttemptCancelled):
        m.complete(MSG)
    with pytest.raises(AttemptCancelled):
        m.complete(MSG)
    assert len(inner.calls) == 1 and m.calls == 1  # never a second provider call
    assert [r.status for r in sink.records] == ["ok", "cancelled", "cancelled"]
    assert m.total.input_tokens == 10  # accounting is exactly what was actually called


def test_slow_call_is_bounded_by_the_deadline_and_recorded_as_unpriced_error():
    inner = FakeProvider([{"text": "slow", "delay_s": 30}])
    m, sink = _meter(inner, deadline=time.monotonic() + 1.6, call_timeout_s=600.0)
    t0 = time.monotonic()
    with pytest.raises(ProviderUnavailable, match="timed out"):
        m.complete(MSG, timeout_s=120)  # the caller asked for 120 s; the attempt has ~1.6 s → the meter clamps
    assert time.monotonic() - t0 < 5.0
    assert inner.calls[0]["timeout_s"] is not None and inner.calls[0]["timeout_s"] <= 1.6 + 1e-6  # clock granularity
    r = sink.records[0]
    assert r.status == "error" and not r.priced and r.input_tokens == 0 and "timed out" in r.error
    assert r.meta["timeout_s"] <= 1.6 + 1e-6 and m.errors == 1 and m.calls == 1
    # after the timeout the deadline is gone: the next call is refused, not attempted
    with pytest.raises(AttemptDeadlineExceeded):
        m.complete(MSG)
    assert len(inner.calls) == 1 and sink.records[1].status == "deadline"


def test_effective_timeout_is_the_minimum_of_caller_cap_and_remaining():
    inner = FakeProvider("ok")
    m, _ = _meter(inner, deadline=time.monotonic() + 100, call_timeout_s=30)
    m.complete(MSG, timeout_s=50)
    assert abs(inner.calls[0]["timeout_s"] - 30) < 0.5  # per-call cap wins
    m2, _ = _meter(FakeProvider("ok"), deadline=time.monotonic() + 10, call_timeout_s=30)
    m2.complete(MSG)
    assert m2.inner.calls[0]["timeout_s"] <= 10  # remaining runtime wins
    m3, _ = _meter(FakeProvider("ok"))  # no deadline, no cap: unbounded, as before
    m3.complete(MSG)
    assert m3.inner.calls[0]["timeout_s"] is None


def test_budget_refusal_still_takes_precedence_after_deadline_and_cancel_checks():
    inner = FakeProvider("ok")
    m, sink = _meter(inner, deadline=time.monotonic() + 60, budget=CallBudget(max_calls=1))
    m.complete(MSG)
    with pytest.raises(AttemptEnded):
        m.complete(MSG)
    assert [r.status for r in sink.records] == ["ok", "budget"] and len(inner.calls) == 1


# ----------------------------------------------------------------------------- call_with_retries


def test_retry_fits_then_insufficient_remaining_time():
    sleeps: list[float] = []
    calls = {"n": 0}

    def flaky(remaining):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ProviderUnavailable("blip")
        return remaining

    # budget 10 s, min window 2 s: two backoffs (~0.5, ~1.0 s) fit → third try succeeds with a smaller remaining
    out = call_with_retries(flaky, budget_s=10, max_retries=3, sleep=sleeps.append, min_call_s=2.0)
    assert calls["n"] == 3 and len(sleeps) == 2 and out is not None and out <= 10
    # budget 2.3 s: after the first failure a 0.5 s backoff would leave < 2 s → raise immediately, no sleep at all
    calls["n"] = 0
    sleeps.clear()
    with pytest.raises(ProviderUnavailable) as ei:
        call_with_retries(flaky, budget_s=2.3, max_retries=3, sleep=sleeps.append, min_call_s=2.0)
    assert calls["n"] == 1 and sleeps == [] and ei.value.retryable is False  # marked non-retryable: nobody loops on it


def test_excessive_retry_after_fails_fast_and_non_retryable_is_immediate():
    sleeps: list[float] = []

    def limited(remaining):
        raise ProviderRateLimited("slow down", retry_after_s=60)

    t0 = time.monotonic()
    with pytest.raises(ProviderRateLimited) as ei:
        call_with_retries(limited, budget_s=30, max_retries=5, sleep=sleeps.append, min_call_s=2.0)
    assert sleeps == [] and time.monotonic() - t0 < 1 and ei.value.retry_after_s == 60
    # a Retry-After that fits IS honoured
    n = {"c": 0}

    def limited_once(remaining):
        n["c"] += 1
        if n["c"] == 1:
            raise ProviderRateLimited("slow down", retry_after_s=3)
        return "ok"

    assert call_with_retries(limited_once, budget_s=30, max_retries=2, sleep=sleeps.append, min_call_s=2.0) == "ok"
    assert sleeps == [3]
    # non-retryable → immediate, no sleep, no second attempt
    n["c"] = 0

    def bad(remaining):
        n["c"] += 1
        raise ProviderRequestError("400")

    with pytest.raises(ProviderRequestError):
        call_with_retries(bad, budget_s=30, max_retries=5, sleep=sleeps.append)
    assert n["c"] == 1 and sleeps == [3]
    # unbounded (budget None): plain bounded retries, sleeps happen
    n["c"] = 0
    with pytest.raises(ProviderRequestError):
        call_with_retries(bad, budget_s=None, max_retries=0, sleep=sleeps.append)


# ----------------------------------------------------------------------------- providers honour the budget


def test_openai_compat_clamps_http_timeout_and_backoff_to_the_budget():
    seen: list[float] = []
    sleeps: list[float] = []
    responses: list[tuple[int, dict, bytes]] = []
    ok = json.dumps({"choices": [{"finish_reason": "stop", "message": {"content": "fine"}}], "usage": {}}).encode()

    def transport(url, body, headers, timeout):
        seen.append(timeout)
        return responses.pop(0)

    p = OpenAICompatibleProvider("m", api_key="k", timeout_s=600, transport=transport, sleep=sleeps.append, max_retries=3)
    responses[:] = [(503, {}, b"{}"), (200, {}, ok)]
    assert p.complete(MSG, timeout_s=8.0).text == "fine"
    assert seen[0] <= 8.0 and seen[1] <= 8.0 and len(sleeps) == 1  # HTTP timeouts clamped, one backoff that fit
    responses[:] = [(429, {"retry-after": "30"}, b"{}")]
    seen.clear()
    sleeps.clear()
    with pytest.raises(ProviderRateLimited):
        p.complete(MSG, timeout_s=8.0)  # 30 s Retry-After does not fit an 8 s budget → immediate, no sleep
    assert sleeps == [] and len(seen) == 1
    responses[:] = [(200, {}, ok)]
    seen.clear()
    p.complete(MSG)  # no budget → the provider's own timeout
    assert seen == [600]


class _RetryingClient:
    """Fake Anthropic client: fails N times with a rate limit, records with_options() calls, then succeeds.

    The SDK import and the error object are built in __init__, never inside `create`: these tests measure what fits
    inside a small provider-call budget, and importing anthropic/httpx on the first call once cost enough seconds
    under parallel test load to eat the budget the retry needed (flake seen at `-n 4`)."""

    def __init__(self, fail_times: int, retry_after: str | None = None):
        anthropic = pytest.importorskip("anthropic")
        httpx = pytest.importorskip("httpx")
        headers = {"retry-after": retry_after} if retry_after else {}
        request = httpx.Request("POST", "https://example.invalid/v1/messages")
        response = httpx.Response(429, request=request, headers=headers)
        self.error = anthropic.RateLimitError("busy", response=response, body=None)
        self.fail_times = fail_times
        self.retry_after = retry_after
        self.options: list[dict] = []
        self.creates = 0
        self.messages = self
        self.beta = SimpleNamespace(messages=self)

    def with_options(self, **kw):
        self.options.append(kw)
        return self

    def create(self, **params):
        self.creates += 1
        if self.creates <= self.fail_times:
            raise self.error
        return SimpleNamespace(
            model="m",
            content=[SimpleNamespace(type="text", text="ok")],
            stop_reason="end_turn",
            usage=SimpleNamespace(input_tokens=1, output_tokens=1),
        )


def test_anthropic_provider_disables_sdk_retries_and_bounds_its_own():
    sleeps: list[float] = []
    client = _RetryingClient(fail_times=1, retry_after="2")
    p = AnthropicProvider("m", client=client, timeout_s=600, max_retries=2, sleep=sleeps.append)
    assert p.complete(MSG, timeout_s=20).text == "ok"
    assert client.creates == 2 and sleeps == [2.0]
    # every request went through with_options(timeout=<clamped>, max_retries=0): the SDK never retries on its own
    assert all(o["max_retries"] == 0 for o in client.options) and all(o["timeout"] <= 20 for o in client.options)
    # the Retry-After does not fit the budget → fail fast, no sleep, single request
    client = _RetryingClient(fail_times=1, retry_after="30")
    p = AnthropicProvider("m", client=client, timeout_s=600, max_retries=2, sleep=sleeps.append)
    sleeps.clear()
    with pytest.raises(ProviderRateLimited):
        p.complete(MSG, timeout_s=8)
    assert client.creates == 1 and sleeps == []
    # no budget: retries still bounded by max_retries, per-request timeout is the provider's own
    client = _RetryingClient(fail_times=5)
    p = AnthropicProvider("m", client=client, timeout_s=33, max_retries=1, sleep=sleeps.append)
    with pytest.raises(ProviderRateLimited):
        p.complete(MSG)
    assert client.creates == 2 and client.options[-1] == {"timeout": 33.0, "max_retries": 0}
