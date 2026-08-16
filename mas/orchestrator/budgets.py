"""Hard-limit checks (invariant I-4). Returns a reason string when a budget is exceeded, else None."""

from __future__ import annotations

from datetime import UTC, datetime

from mas.models.types import Run


def violation(run: Run, *, now: datetime | None = None) -> str | None:
    now = now or datetime.now(UTC)
    b = run.budgets
    if run.tokens_used > b.max_tokens:
        return f"max_tokens exceeded ({run.tokens_used} > {b.max_tokens})"
    if run.cost_used_usd > b.max_cost_usd:
        return f"max_cost_usd exceeded ({run.cost_used_usd:.4f} > {b.max_cost_usd:.4f})"
    if run.tasks_created > b.max_tasks:
        return f"max_tasks exceeded ({run.tasks_created} > {b.max_tasks})"
    if run.replans_used > b.max_replans:
        return f"max_replans exceeded ({run.replans_used} > {b.max_replans})"
    if run.started_at is not None:
        elapsed = (now - run.started_at).total_seconds()
        if elapsed > b.max_wallclock_s:
            return f"max_wallclock_s exceeded ({elapsed:.1f}s > {b.max_wallclock_s}s)"
    if b.deadline_at is not None and now > b.deadline_at:
        return f"deadline passed ({b.deadline_at.isoformat()})"
    return None
