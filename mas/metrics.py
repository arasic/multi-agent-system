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

    return RunMetrics(
        run_id=str(run.id),
        status=run.status.value,
        verdict=run.verdict,
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
    )
