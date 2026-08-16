"""Model prices live in config, never in code (docs/models.md).

`MAS_MODEL_PRICES` is a JSON object keyed by model id (exact id, or a prefix of the id the provider reports back),
USD per 1M tokens:

    {"some-model": {"input": 5, "output": 25, "cache_read": 0.5, "cache_write": 6.25},
     "other-model": [1.0, 5.0]}                                   # short form: [input, output]

An unpriced model is not an error: calls are recorded with `priced=false` and cost 0, and `mas status` says so.
Cost claims in the evaluation must never rest on unpriced usage.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Price:
    input: float  # USD per 1M input tokens (uncached)
    output: float  # USD per 1M output tokens
    cache_read: float | None = None  # per 1M cache-read tokens (None → billed as input)
    cache_write: float | None = None  # per 1M cache-write tokens (None → billed as input)


class Pricing:
    def __init__(self, table: Mapping[str, Price] | None = None):
        self._table: dict[str, Price] = dict(table or {})

    # ------------------------------------------------------------------ construction

    @classmethod
    def from_json(cls, text: str | None) -> Pricing:
        if not text or not text.strip():
            return cls()
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(f"MAS_MODEL_PRICES is not valid JSON: {e}") from e
        if not isinstance(data, dict):
            raise ValueError("MAS_MODEL_PRICES must be a JSON object keyed by model id")
        table: dict[str, Price] = {}
        for model, spec in data.items():
            if str(model).startswith("_"):  # "_comment", "_date", ... are allowed
                continue
            table[str(model)] = _parse_price(str(model), spec)
        return cls(table)

    def with_price(self, model: str, price: Price) -> Pricing:
        t = dict(self._table)
        t[model] = price
        return Pricing(t)

    # ------------------------------------------------------------------ lookup

    def price(self, model: str | None) -> Price | None:
        """Exact match first, then the longest configured key that is a prefix of `model` (providers may report a
        dated/variant id back)."""
        if not model:
            return None
        if model in self._table:
            return self._table[model]
        best: str | None = None
        for key in self._table:
            if model.startswith(key) and (best is None or len(key) > len(best)):
                best = key
        return self._table[best] if best is not None else None

    def cost(
        self,
        model: str | None,
        *,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cache_read_tokens: int = 0,
        cache_write_tokens: int = 0,
    ) -> float | None:
        p = self.price(model)
        if p is None:
            return None
        cr = p.cache_read if p.cache_read is not None else p.input
        cw = p.cache_write if p.cache_write is not None else p.input
        usd = (input_tokens * p.input + output_tokens * p.output + cache_read_tokens * cr + cache_write_tokens * cw) / 1_000_000
        return round(usd, 8)

    def known_models(self) -> list[str]:
        return sorted(self._table)

    def __len__(self) -> int:
        return len(self._table)


def _parse_price(model: str, spec: Any) -> Price:
    if isinstance(spec, list | tuple):
        if len(spec) < 2:
            raise ValueError(f"price for {model!r}: short form needs [input, output]")
        return Price(float(spec[0]), float(spec[1]))
    if isinstance(spec, dict):
        try:
            return Price(
                input=float(spec["input"]),
                output=float(spec["output"]),
                cache_read=float(spec["cache_read"]) if spec.get("cache_read") is not None else None,
                cache_write=float(spec["cache_write"]) if spec.get("cache_write") is not None else None,
            )
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError(f"price for {model!r}: needs numeric 'input' and 'output' ({e})") from e
    raise ValueError(f"price for {model!r}: expected an object or [input, output]")
