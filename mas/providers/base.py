"""ModelProvider interface (docs/architecture.md §11). Concrete providers arrive at roadmap step 9."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class Usage:
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cost_usd": self.cost_usd,
        }


@dataclass
class Completion:
    text: str
    usage: Usage
    raw: Any = None
    tool_calls: list[dict[str, Any]] = field(default_factory=list)


class ModelProvider(Protocol):
    name: str

    def complete(
        self,
        messages: list[dict[str, Any]],
        *,
        max_tokens: int = 4096,
        tools: list[dict[str, Any]] | None = None,
        temperature: float | None = None,
    ) -> Completion: ...
