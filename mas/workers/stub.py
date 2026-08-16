"""StubAgent — deterministic, scripted agent so the whole pipeline is testable with no LLM and no API key.

Script lives in task.meta["stub"] (hand-written DAG) or is passed per-agent:

    "meta": {"stub": {
        "sleep_s": 0.5,            # simulated work; checks the cancel event every 50 ms
        "fail_attempts": 1,        # report failure on attempts 1..k
        "crash_attempts": 0,       # raise on attempts 1..k (worker turns it into FAILED)
        "die_attempts": 0,         # simulate worker death (no report) on attempts 1..k
        "hang_attempts": 0,        # ignore sleep_s and run until cancelled (lease/timeout tests)
        "artifacts": [{"type": "document", "name": "design.md"}],   # default: derived from output_contract
        "new_work_required": null,
        "usage": {"input_tokens": 1000, "output_tokens": 200, "cost_usd": 0.001, "model": "stub"}
    }}
"""

from __future__ import annotations

import time
from typing import Any

from mas.orchestrator.contracts import required_artifacts
from mas.workers.base import AgentResult, ArtifactOut, TaskContext


class StubAgent:
    name = "stub"

    def __init__(self, default_script: dict[str, Any] | None = None):
        self.default_script = default_script or {}
        self.executions: list[tuple[str, int]] = []

    def _script(self, ctx: TaskContext) -> dict[str, Any]:
        s = dict(self.default_script)
        s.update(ctx.task.meta.get("stub", {}) or {})
        return s

    def execute(self, ctx: TaskContext) -> AgentResult:
        s = self._script(ctx)
        n = ctx.attempt.attempt_number
        self.executions.append((ctx.task.key, n))

        hang = n <= int(s.get("hang_attempts", 0))
        sleep_s = float(s.get("sleep_s", 0.0))
        t_end = None if hang else time.monotonic() + sleep_s
        while (t_end is None) or (time.monotonic() < t_end):
            if ctx.cancel.is_set():
                return AgentResult(success=False, failure_reason="cancelled while working")
            time.sleep(0.05)

        if n <= int(s.get("die_attempts", 0)):
            return AgentResult(success=False, failure_reason="simulated death", simulate_death=True)
        if n <= int(s.get("crash_attempts", 0)):
            raise RuntimeError(f"stub crash on {ctx.task.key}#{n}")
        arts = s.get("artifacts")
        if arts is None:
            arts = [{"type": t, "name": name} for t, name in required_artifacts(ctx.task)]
        outs = [
            ArtifactOut(
                type=a["type"],
                ref=f"stub:{ctx.task.key}:{n}:{a.get('name') or a['type']}",
                meta={"name": a.get("name"), "inputs": [str(i.id) for i in ctx.inputs]},
            )
            for a in arts
        ]

        if n <= int(s.get("fail_attempts", 0)):
            # publish_on_fail: leave candidate artifacts behind on a failed attempt (they must stay hints, never outputs)
            return AgentResult(
                success=False,
                failure_reason=f"scripted failure on attempt {n}",
                artifacts=outs if s.get("publish_on_fail") else [],
                usage=dict(s.get("usage", {})),
            )
        return AgentResult(
            success=True,
            artifacts=outs,
            usage=dict(s.get("usage", {"model": "stub"})),
            new_work_required=s.get("new_work_required"),
        )
