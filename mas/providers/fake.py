"""FakeProvider: deterministic, offline `ModelProvider` for tests and key-less runs.

Scripted like `StubAgent`: each call consumes the next script item —
    str                     → text completion
    dict                    → {"text", "tool_calls": [{"id","name","input"}], "stop_reason", "delay_s"}
                              (delay_s simulates latency; longer than the call's timeout_s → ProviderUnavailable, like a real
                              request timing out — the sleep itself is capped at timeout_s)
    Exception instance      → raised (simulate rate limits / outages; wrap in ProviderError subclasses)
    callable(messages, tools) → any of the above, decided from the request
When the script is exhausted the last item repeats (or "OK" if there was none). Usage is synthetic but stable
(≈ 1 token per 4 characters) and priced from `cost_per_mtok`, so budgets and telemetry can be tested end to end.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable, Sequence
from typing import Any

from mas.providers.base import Completion, ProviderUnavailable, ToolCall, Usage

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
        timeout_s: float | None = None,
    ) -> Completion:
        self.calls.append(
            {
                "messages": [dict(m) for m in messages],
                "tools": list(tools or []),
                "max_tokens": max_tokens,
                "timeout_s": timeout_s,
            }
        )
        item = self._next(messages, tools)
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, Completion):
            return item
        if isinstance(item, dict) and item.get("delay_s"):
            delay = float(item["delay_s"])
            if timeout_s is not None and delay > timeout_s:
                time.sleep(max(0.0, timeout_s))
                raise ProviderUnavailable(f"timed out after {timeout_s:.1f}s (fake latency {delay:.1f}s)")
            time.sleep(delay)
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


# ----------------------------------------------------------------------------- fake:builder (offline demo double)


def _asset(name: str) -> str:
    from importlib import resources

    return resources.files("mas.providers").joinpath("fake_assets", "url_shortener", name).read_text(encoding="utf-8")


def builder_script(messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None) -> dict[str, Any]:
    """Deterministic scripted 'model' for the LLM worker loop (`fake:builder`): reads the brief and does the task —
    documents for `document:<name>` contracts; for implementation/testing tasks the canned URL-shortener app + tests
    when the run goal names it (else a placeholder file); runs pytest when it can; integration → finish. Proves the
    plumbing end to end without a key, never the intelligence."""
    brief = messages[1]["content"] if len(messages) > 1 else ""
    first_line = brief.split("\n", 1)[0]
    key = first_line.split("# Task ")[1].split(" ")[0] if "# Task " in first_line else "T"
    tool_names = {t["name"] for t in (tools or [])}
    done = [m for m in messages if m["role"] == "tool"]
    url_shortener = "url shortener" in brief.lower() or "url-shortener" in brief.lower()

    def call(name: str, **args: Any) -> dict[str, Any]:
        return {"tool_calls": [{"id": f"{name}-{len(done)}", "name": name, "input": args}]}

    def finish(**extra: Any) -> dict[str, Any]:
        return {"tool_calls": [{"id": "finish", "name": "finish", "input": {"success": True, "summary": f"{key} done", **extra}}]}

    # the contract line decides: "document:<name>" entries → write those documents; git_commit → build files
    contract_line = ""
    for line in brief.split("\n"):
        if line.startswith("Output contract"):
            contract_line = line.split(":", 1)[1] if ":" in line else ""
            break
    names = [tok.split(":", 1)[1].strip("`.,;") for tok in contract_line.replace(",", " ").split() if tok.startswith("document:")]
    if names:
        if len(done) < len(names):
            n = names[len(done)]
            return call("write_file", path=f"docs/{n}", content=f"# {n}\n\n{key}: {first_line}\n")
        return finish(artifacts=[{"type": "document", "path": f"docs/{n}", "name": n} for n in names])
    if "capability: integration" in first_line or "git_commit" not in contract_line:
        return finish()
    testing = "capability: testing" in first_line
    # implementation / testing with a git_commit contract
    steps: list[tuple[str, dict[str, Any]]] = []
    if url_shortener:
        if testing:
            steps.append(("write_file", {"path": "test_app.py", "content": _asset("test_app.py")}))
        else:
            steps.append(("write_file", {"path": "app.py", "content": _asset("app.py")}))
    else:
        steps.append(("write_file", {"path": f"src/{key}.txt", "content": f"{key}\n"}))
    if "run_python" in tool_names:
        steps.append(("run_python", {"code": f"print('checked {key}')"}))
    if len(done) < len(steps):
        name, args = steps[len(done)]
        return call(name, **args)
    return finish()


FakeProvider.SCRIPTS = {"builder": builder_script}  # type: ignore[attr-defined]


# ----------------------------------------------------------------------------- fake:planner (offline demo double)


def planner_script(messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None) -> str:
    """Deterministic scripted 'planner model' (`fake:planner`): proposes the canned URL-shortener acceptance contract when
    the brief says the goal has no contract yet, else returns the canned URL-shortener DAG (with advisory shape metadata).
    Proves the whole gate — goal → contract → approval → DAG → workers → verifier — without a key; never intelligence."""
    brief = messages[-1]["content"] if messages else ""
    if "# Repair (amendment)" in brief:
        # bounded repair (13-lite): a canned amendment — one repair task on top of the last integration, plus a new
        # integration sink. The cycle number keeps the goal distinct so the amendment hash does not repeat by accident.
        cycle = 1
        for line in brief.splitlines():
            if line.startswith("Repair cycle "):
                try:
                    cycle = int(line.split()[2].split("/")[0])
                except (IndexError, ValueError):
                    cycle = 1
        last_integ = "T6"
        for line in brief.splitlines():
            if '"capability": "integration"' in line and '"key": "' in line:
                last_integ = line.split('"key": "', 1)[1].split('"', 1)[0]
        return json.dumps(
            {
                "kind": "dag",
                "assumptions": [f"repair cycle {cycle}: the failing checks point at the integrated app, not the design"],
                "tasks": [
                    {
                        "id": f"R{cycle}",
                        "capability": "implementation",
                        "goal": f"Repair cycle {cycle}: fix the failing acceptance checks in the integrated app",
                        "depends_on": [last_integ],
                        "output_contract": {"artifacts": ["git_commit"]},
                        "context_spec": {"artifacts_from": [last_integ]},
                    },
                    {
                        "id": f"R{cycle}_integrate",
                        "capability": "integration",
                        "goal": f"Repair cycle {cycle}: integrate the fix",
                        "depends_on": [f"R{cycle}"],
                        "output_contract": {"artifacts": ["git_commit"]},
                    },
                ],
            }
        )
    if "REJECTED" in brief and "produce the DAG" in brief:
        pass  # a contract was already frozen; fall through to the DAG
    elif "NO acceptance contract yet" in brief:
        contract = json.loads(_asset("contract.json"))
        return json.dumps(
            {
                "kind": "contract",
                "requirements": [
                    "POST /shorten with a URL returns 201 and a short code",
                    "GET /<code> redirects (302) to the original URL",
                    "GET /stats reports the number of stored URLs",
                    "stored URLs survive a service restart",
                    "the service has its own tests",
                ],
                "assumptions": ["single process, sqlite file storage under {state_dir}", "no authentication"],
                "exclusions": ["custom short codes", "analytics dashboards"],
                "quality": {"tests_required": True},
                "contract": contract,
            }
        )
    dag = json.loads(_asset("dag.json"))
    return json.dumps(
        {
            "kind": "dag",
            "assumptions": ["Python 3.12 standard library only", "sqlite for persistence"],
            "shape": {
                "estimated_width": 3,
                "dependency_density": 0.4,
                "critical_path_ratio": 0.5,
                "overlapping_outputs": [],
                "coupling_risk": "low",
                "integration_risk": "medium",
                "suggested_mode": "parallel_centralized_mas",
                "rationale": "API, storage and tests are independent after the contract; integration merges them.",
            },
            "tasks": dag["tasks"],
        }
    )


FakeProvider.SCRIPTS["planner"] = planner_script  # type: ignore[attr-defined]


# ----------------------------------------------------------------------------- fake:probe (tool-continuation double)


def probe_script(messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None) -> Any:
    """`fake:probe`: a model that plays the two-turn round trip of `mas models --probe-tools` — call the one offered
    tool, then answer from its result. Keeps the probe's own plumbing testable without a key (CLAUDE.md)."""
    last = messages[-1] if messages else {}
    if last.get("role") == "tool":
        try:
            return str(json.loads(last.get("content") or "{}").get("text", "OK"))
        except (json.JSONDecodeError, AttributeError):
            return "OK"
    name = (tools or [{}])[0].get("name", "mas_probe_echo")
    prompt = str(last.get("content") or "")
    quoted = [part for part in prompt.split("'") if part.strip()]
    text = quoted[1] if len(quoted) > 1 else "OK"
    return {
        "text": "",
        "tool_calls": [{"id": "probe-call-1", "name": name, "input": {"text": text}}],
        "stop_reason": "tool_use",
    }


FakeProvider.SCRIPTS["probe"] = probe_script  # type: ignore[attr-defined]
