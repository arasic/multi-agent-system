"""Run metrics from attempts + events (docs/evaluation.md §4). Pure SQL/Python; used by `mas status` and tests."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any
from uuid import UUID

from mas.db.connection import Conn
from mas.orchestrator import state_machine as sm


@dataclass
class RunMetrics:
    run_id: str
    status: str
    verdict: str | None
    verdict_reason: str | None  # ADR-008 §6 reason code for non-passing terminal runs
    replans_used: int  # bounded repair cycles taken (13-lite)
    tokens_used: int  # the run's hard budget counter (attempt settlements + planner rounds)
    max_tokens: int
    cost_used_usd: float
    max_cost_usd: float
    wall_clock_s: float | None  # started_at → finished_at (execution phase)
    total_s: float | None  # created_at → finished_at (includes planning and any human wait)
    human_wait_s: float  # time spent AWAITING_INPUT (ADR-006) — reported separately so comparisons aren't confounded
    machine_s: float | None  # total_s − human_wait_s
    questions: int
    tasks: int
    tasks_by_status: dict[str, int]
    attempts: int
    attempts_by_status: dict[str, int]
    retries: int
    abandoned: int
    timeouts: int
    replans: int
    max_concurrent_attempts: int
    sum_attempt_s: float
    parallelism_efficiency: float | None  # sum_attempt_s / wall_clock_s
    input_tokens: int
    output_tokens: int
    cost_usd: float
    events: int
    per_task: dict[str, dict[str, Any]] = field(default_factory=dict)
    # step 9: per-call telemetry (model_calls) — evidence independent of settlement; unpriced calls are called out
    model_calls: int = 0  # rows in model_calls: provider calls + refusals (cancelled/deadline/budget, zero tokens)
    model_call_errors: int = 0
    model_call_refused: int = 0
    unpriced_calls: int = 0
    call_input_tokens: int = 0
    call_output_tokens: int = 0
    call_cache_read_tokens: int = 0
    call_cost_usd: float = 0.0
    call_seconds: float = 0.0
    per_model: dict[str, dict[str, Any]] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def compute(conn: Conn, run_id: UUID) -> RunMetrics:
    run = sm.get_run(conn, run_id)
    tasks = sm.tasks_for_run(conn, run_id)
    attempts = sm.attempts_for_run(conn, run_id)

    wall = None
    if run.started_at and run.finished_at:
        wall = (run.finished_at - run.started_at).total_seconds()

    tasks_by: dict[str, int] = {}
    for t in tasks:
        tasks_by[t.status.value] = tasks_by.get(t.status.value, 0) + 1
    att_by: dict[str, int] = {}
    for a in attempts:
        att_by[a.status.value] = att_by.get(a.status.value, 0) + 1

    tasks_with_attempts = {a.task_id for a in attempts}
    retries = len(attempts) - len(tasks_with_attempts)

    # sweep for max concurrency
    points: list[tuple[float, int]] = []
    sum_s = 0.0
    for a in attempts:
        if a.started_at is None:
            continue
        end = a.finished_at or run.finished_at
        if end is None:
            continue
        s, e = a.started_at.timestamp(), end.timestamp()
        sum_s += max(0.0, e - s)
        points.append((s, +1))
        points.append((e, -1))
    points.sort(key=lambda p: (p[0], p[1]))  # ends before starts at equal timestamps
    cur = peak = 0
    for _, d in points:
        cur += d
        peak = max(peak, cur)

    per_task: dict[str, dict[str, Any]] = {}
    by_task_id = {t.id: t for t in tasks}
    for a in attempts:
        t = by_task_id.get(a.task_id)
        if t is None:
            continue
        d = per_task.setdefault(t.key, {"status": t.status.value, "attempts": 0, "seconds": 0.0, "workers": []})
        d["attempts"] += 1
        if a.started_at and a.finished_at:
            d["seconds"] += (a.finished_at - a.started_at).total_seconds()
        if a.worker_id and a.worker_id not in d["workers"]:
            d["workers"].append(a.worker_id)

    n_events = conn.execute("SELECT count(*) AS n FROM events WHERE run_id = %s", (run_id,)).fetchone()["n"]  # type: ignore[index]

    # human wait: sum of intervals spent in AWAITING_INPUT (run.awaiting_input → next run.* transition)
    human_wait = 0.0
    trans = conn.execute("SELECT type, ts FROM events WHERE run_id = %s AND type LIKE 'run.%%' ORDER BY id", (run_id,)).fetchall()
    waiting_since = None
    for e in trans:
        if e["type"] == "run.awaiting_input":
            waiting_since = e["ts"]
        elif waiting_since is not None:
            human_wait += (e["ts"] - waiting_since).total_seconds()
            waiting_since = None
    if waiting_since is not None:  # still waiting right now
        now_row = conn.execute("SELECT now() AS n").fetchone()
        human_wait += (now_row["n"] - waiting_since).total_seconds()  # type: ignore[index]

    total = None
    if run.created_at and run.finished_at:
        total = (run.finished_at - run.created_at).total_seconds()

    calls = model_call_summary(conn, run_id)

    return RunMetrics(
        run_id=str(run.id),
        status=run.status.value,
        verdict=run.verdict,
        verdict_reason=run.verdict_reason,
        replans_used=run.replans_used,
        tokens_used=run.tokens_used,
        max_tokens=run.budgets.max_tokens,
        cost_used_usd=round(float(run.cost_used_usd), 6),
        max_cost_usd=float(run.budgets.max_cost_usd),
        wall_clock_s=round(wall, 3) if wall is not None else None,
        total_s=round(total, 3) if total is not None else None,
        human_wait_s=round(human_wait, 3),
        machine_s=round(total - human_wait, 3) if total is not None else None,
        questions=run.questions_asked,
        tasks=len(tasks),
        tasks_by_status=tasks_by,
        attempts=len(attempts),
        attempts_by_status=att_by,
        retries=retries,
        abandoned=att_by.get("ABANDONED", 0),
        timeouts=att_by.get("TIMEOUT", 0),
        replans=run.replans_used,
        max_concurrent_attempts=peak,
        sum_attempt_s=round(sum_s, 3),
        parallelism_efficiency=(round(sum_s / wall, 3) if wall else None),
        input_tokens=sum(a.input_tokens for a in attempts),
        output_tokens=sum(a.output_tokens for a in attempts),
        cost_usd=round(sum(a.cost_usd for a in attempts), 6),
        events=int(n_events),
        per_task=per_task,
        **calls,
    )


def model_call_summary(conn: Conn, run_id: UUID) -> dict[str, Any]:
    """Totals over model_calls for a run, plus a per-model breakdown (role, calls, tokens, cost, priced?)."""
    rows = conn.execute(
        """
        SELECT provider, model, role,
               count(*)                                   AS calls,
               count(*) FILTER (WHERE status = 'error')   AS errors,
               count(*) FILTER (WHERE status IN ('cancelled', 'deadline', 'budget')) AS refused,
               count(*) FILTER (WHERE NOT priced)         AS unpriced,
               coalesce(sum(input_tokens), 0)             AS input_tokens,
               coalesce(sum(output_tokens), 0)            AS output_tokens,
               coalesce(sum(cache_read_tokens), 0)        AS cache_read_tokens,
               coalesce(sum(cost_usd), 0)                 AS cost_usd,
               coalesce(sum(duration_ms), 0)              AS duration_ms
        FROM model_calls WHERE run_id = %s
        GROUP BY provider, model, role ORDER BY provider, model, role
        """,
        (run_id,),
    ).fetchall()
    per_model: dict[str, dict[str, Any]] = {}
    tot: dict[str, Any] = {
        "model_calls": 0,
        "model_call_errors": 0,
        "model_call_refused": 0,
        "unpriced_calls": 0,
        "call_input_tokens": 0,
        "call_output_tokens": 0,
        "call_cache_read_tokens": 0,
        "call_cost_usd": 0.0,
        "call_seconds": 0.0,
    }
    for r in rows:
        key = f"{r['provider']}:{r['model']}/{r['role']}"
        per_model[key] = {
            "calls": int(r["calls"]),
            "errors": int(r["errors"]),
            "refused": int(r["refused"]),
            "unpriced": int(r["unpriced"]),
            "input_tokens": int(r["input_tokens"]),
            "output_tokens": int(r["output_tokens"]),
            "cache_read_tokens": int(r["cache_read_tokens"]),
            "cost_usd": round(float(r["cost_usd"]), 6),
            "seconds": round(int(r["duration_ms"]) / 1000.0, 3),
        }
        tot["model_calls"] += int(r["calls"])
        tot["model_call_errors"] += int(r["errors"])
        tot["model_call_refused"] += int(r["refused"])
        tot["unpriced_calls"] += int(r["unpriced"])
        tot["call_input_tokens"] += int(r["input_tokens"])
        tot["call_output_tokens"] += int(r["output_tokens"])
        tot["call_cache_read_tokens"] += int(r["cache_read_tokens"])
        tot["call_cost_usd"] += float(r["cost_usd"])
        tot["call_seconds"] += int(r["duration_ms"]) / 1000.0
    tot["call_cost_usd"] = round(tot["call_cost_usd"], 6)
    tot["call_seconds"] = round(tot["call_seconds"], 3)
    tot["per_model"] = per_model
    return tot
