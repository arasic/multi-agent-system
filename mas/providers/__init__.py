"""Model providers (roadmap step 9). The only package that may name a vendor or a model.

from mas.providers import from_spec, provider_for_role, meter
p = from_spec("anthropic:claude-opus-5")          # or "openai:<model>", "fake[:<model>]"
m = meter(p, role="worker", run_id=..., attempt_id=..., sink=DbSink(conn), budget=CallBudget(40, 300_000))
m.complete([{"role": "user", "content": "..."}])
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from mas.config import Settings, settings
from mas.providers.base import (
    Completion,
    ModelProvider,
    ProviderError,
    ProviderRateLimited,
    ProviderRequestError,
    ProviderUnavailable,
    ToolCall,
    Usage,
)
from mas.providers.pricing import Price, Pricing
from mas.providers.telemetry import (
    AttemptBudgetExceeded,
    AttemptCancelled,
    AttemptDeadlineExceeded,
    AttemptEnded,
    CallBudget,
    CallRecord,
    DbSink,
    MemorySink,
    MeteredProvider,
    NullSink,
    Sink,
)

__all__ = [
    "AttemptBudgetExceeded",
    "AttemptCancelled",
    "AttemptDeadlineExceeded",
    "AttemptEnded",
    "CallBudget",
    "CallRecord",
    "Completion",
    "DbSink",
    "MemorySink",
    "MeteredProvider",
    "ModelProvider",
    "NullSink",
    "Price",
    "Pricing",
    "ProviderError",
    "ProviderRateLimited",
    "ProviderRequestError",
    "ProviderUnavailable",
    "Sink",
    "ToolCall",
    "Usage",
    "from_spec",
    "meter",
    "pricing_from_settings",
    "provider_for_role",
    "role_spec",
]

PROVIDERS = ("anthropic", "openai", "fake")
ROLES = ("planner", "worker", "reviewer")


def parse_spec(spec: str) -> tuple[str, str]:
    """'anthropic:claude-opus-5' → ('anthropic', 'claude-opus-5'); 'fake' → ('fake', '')."""
    s = (spec or "").strip()
    if not s:
        raise ValueError("empty model spec")
    if ":" not in s:
        if s in PROVIDERS:
            return s, ""
        raise ValueError(f"model spec {spec!r} must look like '<provider>:<model>' with provider in {PROVIDERS}")
    provider, model = s.split(":", 1)
    provider, model = provider.strip().lower(), model.strip()
    if provider not in PROVIDERS:
        raise ValueError(f"unknown provider {provider!r} in {spec!r}; known: {PROVIDERS}")
    return provider, model


def from_spec(spec: str, *, cfg: Settings | None = None, **overrides: Any) -> ModelProvider:
    """Build a concrete provider from a spec string and settings. Overrides go straight to the constructor."""
    cfg = cfg or settings()
    provider, model = parse_spec(spec)
    if provider == "fake":
        from mas.providers.fake import FakeProvider

        kw: dict[str, Any] = {"model": model or "fake-1"}
        kw.update(overrides)
        script = getattr(FakeProvider, "SCRIPTS", {}).get(model)  # fake:builder → the offline demo double
        if script is not None and "script" not in kw:
            return FakeProvider(script, **kw)
        return FakeProvider(**kw)
    if provider == "anthropic":
        from mas.providers.anthropic_provider import DEFAULT_MODEL, AnthropicProvider

        kw = {
            "thinking": cfg.anthropic_thinking,
            "effort": cfg.anthropic_effort or None,
            "fallbacks": cfg.anthropic_fallbacks or None,
            "timeout_s": cfg.provider_timeout_s,
            "max_retries": cfg.provider_max_retries,
        }
        kw.update(overrides)
        return AnthropicProvider(model or DEFAULT_MODEL, **kw)
    if provider == "openai":
        from mas.providers.openai_compat import OpenAICompatibleProvider

        if not model:
            raise ValueError("openai spec needs a model: 'openai:<model>'")
        kw = {
            "base_url": cfg.openai_base_url,
            "api_key": cfg.openai_api_key or None,
            "timeout_s": cfg.provider_timeout_s,
            "max_retries": cfg.provider_max_retries,
            "max_tokens_field": cfg.openai_max_tokens_field,
        }
        kw.update(overrides)
        return OpenAICompatibleProvider(model, **kw)
    raise ValueError(f"unknown provider {provider!r}")  # pragma: no cover - parse_spec already rejects


def role_spec(role: str, cfg: Settings | None = None) -> str:
    cfg = cfg or settings()
    if role not in ROLES:
        raise ValueError(f"unknown role {role!r}; known: {ROLES}")
    return {"planner": cfg.model_planner, "worker": cfg.model_worker, "reviewer": cfg.model_reviewer}[role]


def provider_for_role(role: str, cfg: Settings | None = None) -> ModelProvider | None:
    """The configured provider for a role, or None when the role has no model (M1 behaviour: stub agents)."""
    cfg = cfg or settings()
    spec = role_spec(role, cfg)
    return from_spec(spec, cfg=cfg) if spec else None


def pricing_from_settings(cfg: Settings | None = None) -> Pricing:
    return Pricing.from_json((cfg or settings()).model_prices)


def meter(
    provider: ModelProvider,
    *,
    role: str,
    sink: Sink | None = None,
    pricing: Pricing | None = None,
    run_id: UUID | None = None,
    task_id: UUID | None = None,
    attempt_id: UUID | None = None,
    budget: CallBudget | None = None,
    deadline: float | None = None,
    cancel: Any = None,
    call_timeout_s: float | None = None,
    cfg: Settings | None = None,
) -> MeteredProvider:
    """Wrap a provider for one unit of work (attempt / planning round) with telemetry, pricing, a call budget and the
    unit's deadline / cancellation."""
    cfg = cfg or settings()
    return MeteredProvider(
        provider,
        sink=sink,
        pricing=pricing if pricing is not None else pricing_from_settings(cfg),
        role=role,
        run_id=run_id,
        task_id=task_id,
        attempt_id=attempt_id,
        budget=budget,
        deadline=deadline,
        cancel=cancel,
        call_timeout_s=call_timeout_s if call_timeout_s is not None else cfg.provider_timeout_s,
    )
