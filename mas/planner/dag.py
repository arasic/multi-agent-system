"""Typed DAG spec. This is the planner's output format and the hand-written DAG file format (docs/architecture.md §8)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class TaskSpec:
    id: str
    capability: str
    goal: str
    depends_on: list[str] = field(default_factory=list)
    output_contract: dict[str, Any] = field(default_factory=dict)
    input_contract: dict[str, Any] = field(default_factory=dict)
    context_spec: dict[str, Any] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)
    tools: list[str] | None = None  # requested tools; None → the capability's default allow-list (validator fills)
    max_attempts: int | None = None
    # planner's own cost estimate {"tokens": int, "seconds": number} — optional; validator rule 8 lets it only *tighten*
    # the budget check (an estimate above the per-attempt allocation is infeasible), never loosen it
    estimate: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TaskSpec:
        tools = d.get("tools")
        est = d.get("estimate")
        return cls(
            id=str(d["id"]),
            capability=str(d["capability"]),
            goal=str(d.get("goal", "")),
            depends_on=[str(x) for x in d.get("depends_on", []) or []],
            output_contract=dict(d.get("output_contract", {}) or {}),
            input_contract=dict(d.get("input_contract", {}) or {}),
            context_spec=dict(d.get("context_spec", {}) or {}),
            meta=dict(d.get("meta", {}) or {}),
            tools=[str(x) for x in tools] if tools is not None else None,
            max_attempts=d.get("max_attempts"),
            estimate=dict(est) if isinstance(est, dict) else ({"_invalid": est} if est is not None else {}),
        )

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "id": self.id,
            "capability": self.capability,
            "goal": self.goal,
            "depends_on": list(self.depends_on),
            "output_contract": self.output_contract,
        }
        if self.input_contract:
            d["input_contract"] = self.input_contract
        if self.context_spec:
            d["context_spec"] = self.context_spec
        if self.meta:
            d["meta"] = self.meta
        if self.tools is not None:
            d["tools"] = list(self.tools)
        if self.max_attempts is not None:
            d["max_attempts"] = self.max_attempts
        if self.estimate:
            d["estimate"] = dict(self.estimate)
        return d


@dataclass
class Questions:
    """Planner output when it needs more information before it can plan (ADR-006)."""

    questions: list[str]
    context: str | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Questions:
        return cls(questions=[str(q) for q in d.get("questions", []) or []], context=d.get("context"))

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"questions": list(self.questions)}
        if self.context:
            d["context"] = self.context
        return d


@dataclass(frozen=True)
class QA:
    """One answered question batch, in order; fed back to the planner on the next call."""

    questions: list[str]
    answer: str


@dataclass
class ContractProposal:
    """Planner output for an ad-hoc goal (ADR-007): the *proposed* definition of done. `checks`/`service` follow the
    trusted adapters' schema (mas/verifier/adapters/schema.py) — the planner names criteria, never writes tests. A human
    approves (possibly edits) it once; approval freezes it as an immutable `acceptance_contract` artifact and a
    suite directory the verifier loads like any other."""

    requirements: list[str]
    checks: list[dict[str, Any]]
    service: dict[str, Any] | None = None
    quality: dict[str, Any] = field(default_factory=dict)
    assumptions: list[str] = field(default_factory=list)
    exclusions: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ContractProposal:
        c = d.get("contract") if isinstance(d.get("contract"), dict) else d
        return cls(
            requirements=[str(x) for x in d.get("requirements", []) or []],
            checks=[dict(x) for x in (c.get("checks", []) or []) if isinstance(x, dict)],
            service=dict(c["service"]) if isinstance(c.get("service"), dict) else None,
            quality=dict(d.get("quality", {}) or {}),
            assumptions=[str(x) for x in d.get("assumptions", []) or []],
            exclusions=[str(x) for x in d.get("exclusions", []) or []],
        )

    def contract_dict(self) -> dict[str, Any]:
        """The executable half, in the adapters' schema (protocol 1)."""
        d: dict[str, Any] = {"protocol_version": 1, "checks": [dict(c) for c in self.checks]}
        if self.service is not None:
            d["service"] = dict(self.service)
        return d

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "contract",
            "requirements": list(self.requirements),
            "contract": self.contract_dict(),
            "quality": dict(self.quality),
            "assumptions": list(self.assumptions),
            "exclusions": list(self.exclusions),
        }


PLAN_KINDS = ("dag", "questions", "contract")


def parse_plan(d: dict[str, Any]) -> DagSpec | Questions | ContractProposal:
    """Planner output is a union of exactly one kind: a DAG, a question batch, or an acceptance-contract proposal.
    `kind` decides when present; otherwise the shape does. Anything else is rejected upstream."""
    if not isinstance(d, dict):
        raise ValueError("planner output must be a JSON object")
    kind = d.get("kind")
    if kind is not None and kind not in PLAN_KINDS:
        raise ValueError(f"unknown plan kind {kind!r}; expected one of {PLAN_KINDS}")
    if kind == "questions" or (kind is None and "questions" in d and "tasks" not in d and "contract" not in d):
        return Questions.from_dict(d)
    if kind == "contract" or (kind is None and ("contract" in d or "checks" in d) and "tasks" not in d):
        return ContractProposal.from_dict(d)
    if kind == "dag" or "tasks" in d:
        return DagSpec.from_dict(d)
    raise ValueError("planner output is neither a DAG (tasks), a question batch (questions) nor a contract proposal")


@dataclass
class DagSpec:
    tasks: list[TaskSpec] = field(default_factory=list)
    goal: str | None = None
    benchmark: str | None = None
    # ADR-006 policy: a planner that proceeds without asking states what it assumed. Recorded as an artifact.
    assumptions: list[str] = field(default_factory=list)
    # ADR-008: task-shape metadata — ADVISORY. Recorded with the plan, validated for shape, never selects the mode:
    # A/B/C/D configuration controls execution through M3.
    shape: dict[str, Any] = field(default_factory=dict)
    # step 13 amendments only: keys of existing PENDING/READY tasks made obsolete by this amendment → CANCELLED at install
    # (validator rule 9: never RUNNING/COMPLETED work; an initial plan may not cancel anything)
    cancel: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> DagSpec:
        return cls(
            tasks=[TaskSpec.from_dict(t) for t in d.get("tasks", [])],
            goal=d.get("goal"),
            benchmark=d.get("benchmark"),
            assumptions=[str(a) for a in d.get("assumptions", []) or []],
            shape=dict(d.get("shape", {}) or {}),
            cancel=[str(k) for k in d.get("cancel", []) or []],
        )

    @classmethod
    def from_json(cls, text: str) -> DagSpec:
        return cls.from_dict(json.loads(text))

    @classmethod
    def from_file(cls, path: str | Path) -> DagSpec:
        return cls.from_json(Path(path).read_text(encoding="utf-8"))

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"tasks": [t.to_dict() for t in self.tasks]}
        if self.goal:
            d["goal"] = self.goal
        if self.benchmark:
            d["benchmark"] = self.benchmark
        if self.assumptions:
            d["assumptions"] = list(self.assumptions)
        if self.shape:
            d["shape"] = dict(self.shape)
        if self.cancel:
            d["cancel"] = list(self.cancel)
        return d

    def by_id(self) -> dict[str, TaskSpec]:
        return {t.id: t for t in self.tasks}
