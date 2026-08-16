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
    max_attempts: int | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TaskSpec:
        return cls(
            id=str(d["id"]),
            capability=str(d["capability"]),
            goal=str(d.get("goal", "")),
            depends_on=[str(x) for x in d.get("depends_on", []) or []],
            output_contract=dict(d.get("output_contract", {}) or {}),
            input_contract=dict(d.get("input_contract", {}) or {}),
            context_spec=dict(d.get("context_spec", {}) or {}),
            meta=dict(d.get("meta", {}) or {}),
            max_attempts=d.get("max_attempts"),
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
        if self.max_attempts is not None:
            d["max_attempts"] = self.max_attempts
        return d


@dataclass
class DagSpec:
    tasks: list[TaskSpec] = field(default_factory=list)
    goal: str | None = None
    benchmark: str | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> DagSpec:
        return cls(
            tasks=[TaskSpec.from_dict(t) for t in d.get("tasks", [])],
            goal=d.get("goal"),
            benchmark=d.get("benchmark"),
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
        return d

    def by_id(self) -> dict[str, TaskSpec]:
        return {t.id: t for t in self.tasks}
