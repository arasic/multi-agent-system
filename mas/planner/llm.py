"""LLMPlanner — the provider-backed planner (roadmap step 11). Proposes; never decides.

One call to `plan()` = one *typed* outcome — a DAG, a question batch, or an acceptance-contract proposal — parsed
deterministically from a single JSON object the model returns. Malformed output is fed back to the model as data for a
bounded number of parse retries; anything still malformed ends the round with an error (the driver turns it into a
run verdict). The driver (`runs.plan_run`) then validates, records, and either installs the DAG, parks the run for
answers / approval, or returns validation errors to the next `plan()` as data.

What the planner can NOT do, by construction: write acceptance tests or suites (it names criteria in the adapters'
schema; approval and freezing are human + deterministic code), change budgets, set run state, or pick the execution
mode (task-shape metadata is advisory, ADR-008). Every model call is metered (role=planner: telemetry, pricing, a
per-round call/token budget capped by the run's remaining tokens, the run's remaining wall-clock as the deadline).
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from mas.planner.dag import ContractProposal, DagSpec, Questions, parse_plan
from mas.planner.planner import PlanRequest
from mas.planner.validator import SHAPE_LEVELS, SHAPE_MODES
from mas.providers.base import ModelProvider, ProviderError
from mas.providers.pricing import Pricing
from mas.providers.telemetry import CallBudget, MeteredProvider, Sink

log = logging.getLogger(__name__)

PROMPT_VERSION = "llm-planner/v2"


@dataclass(frozen=True)
class PlannerLimits:
    max_tokens: int = 8_192  # per model call
    max_parse_retries: int = 2  # malformed JSON / unknown kind → the error goes back as data, this many times
    max_calls_per_round: int = 4  # model calls per plan() (1 + parse retries + slack)
    max_tokens_per_round: int = 120_000
    min_deadline_s: float = 20.0  # below this much remaining wall-clock the planner refuses to start a call


class PlannerOutputError(RuntimeError):
    """The model did not produce a usable typed outcome within the parse-retry budget."""


SYSTEM_PROMPT = """You are the planner of a multi-agent software build system. You propose; deterministic code decides.

You receive a goal, the registered worker capabilities and the tool families each may use, budgets, any earlier
questions and answers, the frozen acceptance contract if one exists, and — on a retry — the exact validation errors
that rejected your previous output. Reply with ONE JSON object and nothing else. It must have a top-level "kind":

1. {"kind": "questions", "questions": ["..."], "context": "why"} — only when the goal is genuinely ambiguous in a way
   that changes what gets built. Ask everything you need in one batch. Batches are budgeted.

2. {"kind": "contract", "requirements": ["plain-language behavioural statements"], "assumptions": ["what you assumed
   instead of asking"], "exclusions": ["explicitly out of scope"], "quality": {"tests_required": true},
   "contract": {"protocol_version": 1, "service": {...optional...}, "checks": [...]}} — ONLY when told the goal has no
   acceptance contract yet. The checks are executable by trusted adapters and nothing else. Allowed check types:
   - build_succeeds: {"id", "type": "build_succeeds", "command": [argv], "timeout_s"}
   - tests_required: {"id", "type": "tests_required", "runner": "pytest"|"unittest", "args": [], "min_tests": n, "timeout_s"}
   - http_status: {"id", "type": "http_status", "request": {"method", "path", "json"?}, "expect": {"status", "json_contains"?,
     "header_equals"?}, "timeout_s"} (needs "service")
   - restart_persists: {"id", "type": "restart_persists", "setup": [requests], "verify": request, "expect": {...}, "timeout_s"}
     (needs "service")
   "service": {"start": [argv with {port} and {state_dir}], "health": "/path", "port": n, "startup_timeout_s": n}.
   Check ids: [a-z][a-z0-9_]*, unique. Sum of per-check timeouts must stay under 240 s. Do NOT write test code; name
   criteria. A human approves this contract once; it then freezes and decides success — you cannot change it later.

3. {"kind": "dag", "assumptions": ["..."], "shape": {...}, "tasks": [ {"id": "T1", "capability": "...", "goal": "...",
   "depends_on": [], "output_contract": {"artifacts": ["document:<name>" | "git_commit"]}, "context_spec": {"artifacts_from":
   [ids], "paths": [globs]}?, "tools": [families]?, "max_attempts": n?, "estimate": {"tokens": n, "seconds": n}? } ]} — a
   task DAG that satisfies the frozen contract. Rules the validator enforces: ids [A-Za-z0-9][A-Za-z0-9_.-]*, unique;
   depends_on names existing tasks; no cycles; every task has an output_contract with artifacts; capabilities only from
   the registered list; tools only from the capability's families; exactly one task with capability "integration" that
   depends on the final producers (or omit it and one is appended); at most the remaining task budget; max_attempts
   within budget. Budget allocation: the run funds one attempt of max_attempt_tokens for EVERY task (integration
   included) — tasks x max_attempt_tokens must fit the remaining tokens, so do not over-decompose; a task's "estimate"
   is optional and can only make the check stricter (never looser); an estimate above max_attempt_tokens or
   max_attempt_runtime_s means the task cannot finish in one attempt and is rejected — split it. Tasks run in
   isolated worktrees and only see the artifacts named in context_spec.artifacts_from — say what each task needs.
   "shape" is advisory metadata (it never selects the execution mode): {"estimated_width": int, "dependency_density":
   0..1, "critical_path_ratio": 0..1, "overlapping_outputs": ["paths several tasks would touch"], "coupling_risk":
   "low|medium|high", "integration_risk": "low|medium|high", "suggested_mode": "single_agent|sequential_workflow|
   parallel_centralized_mas", "rationale": "..."}. Prefer independent tasks that do not touch the same files.

Be concrete and honest. Never claim capabilities or tools that are not listed. If you proceed without asking, record
what you assumed in "assumptions"."""


class LLMPlanner:
    name = "llm"

    def __init__(
        self,
        provider: ModelProvider,
        *,
        sink_factory: Callable[[Any], Sink] | None = None,  # run_id -> Sink (DbSink on the orchestrator's connection)
        pricing: Pricing | None = None,
        limits: PlannerLimits | None = None,
        prompt_version: str = PROMPT_VERSION,
    ):
        self.provider = provider
        self.sink_factory = sink_factory
        self.pricing = pricing
        self.limits = limits or PlannerLimits()
        self.prompt_version = prompt_version
        self.rounds: list[dict[str, Any]] = []  # per plan(): calls, kind, parse errors (in-memory, for tests/status)

    # ------------------------------------------------------------------ Planner

    def plan(self, req: PlanRequest) -> DagSpec | Questions | ContractProposal:
        lim = self.limits
        deadline = None
        if req.deadline_s is not None:
            if req.deadline_s < lim.min_deadline_s:
                raise PlannerOutputError(f"not enough run wall-clock left to plan ({req.deadline_s:.0f}s)")
            deadline = time.monotonic() + req.deadline_s
        remaining_tokens = req.remaining.get("tokens")
        budget = CallBudget(
            max_calls=lim.max_calls_per_round,
            max_tokens=min(lim.max_tokens_per_round, int(remaining_tokens))
            if remaining_tokens is not None
            else lim.max_tokens_per_round,
        )
        model = MeteredProvider(
            self.provider,
            sink=self.sink_factory(req.run_id) if self.sink_factory else None,
            pricing=self.pricing,
            role="planner",
            run_id=req.run_id,
            budget=budget,
            deadline=deadline,
        )
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": self.brief(req)},
        ]
        parse_errors: list[str] = []
        round_info: dict[str, Any] = {"plan_attempt": req.plan_attempt, "calls": 0, "parse_errors": parse_errors, "kind": None}
        self.rounds.append(round_info)
        for _ in range(lim.max_parse_retries + 1):
            try:
                comp = model.complete(messages, max_tokens=lim.max_tokens)
            except ProviderError:  # budget/deadline/provider: the round is over; the driver makes it a verdict
                raise
            round_info["calls"] += 1
            if comp.refused:
                raise PlannerOutputError("model refused to plan (stop_reason=refusal)")
            try:
                out = parse_output(comp.text)
            except (ValueError, TypeError, KeyError) as e:
                msg = f"{type(e).__name__}: {e}"
                parse_errors.append(msg)
                messages.append(comp.as_message())
                messages.append(
                    {
                        "role": "user",
                        "content": "Your reply was not a valid plan object: "
                        + msg
                        + ". Reply again with ONE JSON object only (kind = questions | contract | dag), "
                        "no prose, no code fences.",
                    }
                )
                continue
            round_info["kind"] = out.__class__.__name__
            return out
        raise PlannerOutputError(
            f"planner output stayed malformed after {lim.max_parse_retries + 1} attempts: {parse_errors[-1]}"
        )

    # ------------------------------------------------------------------ prompt

    def brief(self, req: PlanRequest) -> str:
        lines = ["# Goal", "", req.goal.strip(), ""]
        caps = ", ".join(sorted(req.capabilities)) or "(none registered)"
        lines += ["# Registered capabilities and their tool families", ""]
        for c in sorted(req.capabilities):
            fams = ", ".join(req.tool_registry.get(c, ())) or "(none)"
            lines.append(f"- {c}: {fams}")
        if not req.capabilities:
            lines.append(f"- {caps}")
        lines += ["", "# Budgets remaining", "", json.dumps(req.remaining, sort_keys=True), ""]
        if req.qa:
            lines += ["# Questions you asked and the answers", ""]
            for i, qa in enumerate(req.qa, 1):
                lines.append(f"{i}. Q: {' | '.join(qa.questions)}")
                lines.append(f"   A: {qa.answer}")
            lines.append("")
        if req.needs_contract:
            lines += [
                "# Acceptance contract",
                "",
                "This goal has NO acceptance contract yet. Before any DAG, propose one (kind=contract) — or ask questions"
                " (kind=questions) if you cannot define done yet. A DAG will be rejected until a contract is frozen.",
                "",
            ]
        elif req.contract:
            lines += [
                "# Frozen acceptance contract (definition of done — cannot change)",
                "",
                json.dumps(
                    {
                        k: req.contract.get(k)
                        for k in ("benchmark", "check_ids", "requirements", "exclusions")
                        if k in req.contract
                    },
                    sort_keys=True,
                ),
                "",
                "Produce a DAG (kind=dag) whose integrated result passes these checks.",
                "",
            ]
        elif req.benchmark:
            lines += [
                "# Acceptance",
                "",
                f"An external acceptance suite exists for benchmark {req.benchmark!r}; a DAG (kind=dag) is expected.",
                "",
            ]
        if req.validation_errors:
            lines += [
                f"# Your previous output was REJECTED (attempt {req.plan_attempt - 1}). Fix exactly these:",
                "",
                *[f"- {e}" for e in req.validation_errors],
                "",
            ]
        lines.append(f"Reply with ONE JSON object (kind = questions | contract | dag). Plan attempt {req.plan_attempt}.")
        return "\n".join(lines)


# ----------------------------------------------------------------------------- deterministic parsing

_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)


def parse_output(text: str) -> DagSpec | Questions | ContractProposal:
    """Exactly one JSON object → exactly one typed outcome. Fences are tolerated; prose is not."""
    raw = (text or "").strip()
    if not raw:
        raise ValueError("empty reply")
    raw = _FENCE.sub("", raw).strip()
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError:
        # tolerate leading/trailing prose once: take the outermost {...}
        start, end = raw.find("{"), raw.rfind("}")
        if start < 0 or end <= start:
            raise ValueError("no JSON object found") from None
        doc = json.loads(raw[start : end + 1])
    if not isinstance(doc, dict):
        raise ValueError("top-level JSON must be an object")
    out = parse_plan(doc)
    if isinstance(out, DagSpec) and not out.tasks:
        raise ValueError("dag has no tasks")
    if isinstance(out, Questions) and not [q for q in out.questions if q.strip()]:
        raise ValueError("questions batch is empty")
    if isinstance(out, DagSpec) and out.shape:
        mode = out.shape.get("suggested_mode")
        if mode is not None and mode not in SHAPE_MODES:
            raise ValueError(f"shape.suggested_mode must be one of {SHAPE_MODES}")
        for k in ("coupling_risk", "integration_risk"):
            v = out.shape.get(k)
            if v is not None and v not in SHAPE_LEVELS:
                raise ValueError(f"shape.{k} must be one of {SHAPE_LEVELS}")
    return out
