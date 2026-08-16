"""Run creation and DAG installation. Deterministic; the DAG comes from a file (M1) or the planner (M2)."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any
from uuid import UUID

from mas.db.connection import Conn, Jsonb
from mas.db.events import emit
from mas.models.enums import RunStatus
from mas.models.types import Budgets, Run
from mas.orchestrator import state_machine as sm
from mas.planner.dag import DagSpec
from mas.planner.validator import ValidationResult, validate


class InvalidDag(Exception):
    def __init__(self, result: ValidationResult):
        self.result = result
        super().__init__("; ".join(str(e) for e in result.errors))


def create_run(
    conn: Conn,
    *,
    goal: str,
    budgets: Budgets | None = None,
    benchmark: str | None = None,
    config: str | None = None,
    base_ref: str | None = None,
    pool: str = "default",
) -> Run:
    b = budgets or Budgets()
    with conn.transaction():
        row = conn.execute(
            """
            INSERT INTO runs (goal, benchmark, config, base_ref, pool,
                max_concurrency, max_tasks, max_attempts_per_task, max_replans, max_plan_attempts,
                max_tokens, max_cost_usd, max_wallclock_s, max_attempt_runtime_s, lease_s, deadline_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                goal,
                benchmark,
                config,
                base_ref,
                pool,
                b.max_concurrency,
                b.max_tasks,
                b.max_attempts_per_task,
                b.max_replans,
                b.max_plan_attempts,
                b.max_tokens,
                b.max_cost_usd,
                b.max_wallclock_s,
                b.max_attempt_runtime_s,
                b.lease_s,
                b.deadline_at,
            ),
        ).fetchone()
        assert row is not None
        emit(
            conn,
            row["id"],
            "run.created",
            payload={
                "goal": goal,
                "benchmark": benchmark,
                "config": config,
                "pool": pool,
                "budgets": {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in asdict(b).items()},
            },
        )
    return Run.from_row(row)


def install_dag(
    conn: Conn,
    run_id: UUID,
    dag: DagSpec,
    *,
    created_by: str = "planner",
    capabilities: set[str] | None = None,
    start: bool = True,
) -> ValidationResult:
    """Validate and insert the DAG's tasks; move the run CREATED → PLANNING → RUNNING (if `start`).

    Raises InvalidDag if validation fails (run is moved to FAILED so it always has a verdict).
    """
    with conn.transaction():
        run = sm.get_run(conn, run_id)
        if run.status is RunStatus.CREATED:
            sm.transition_run(conn, run_id, RunStatus.PLANNING)
        existing = conn.execute("SELECT count(*) AS n FROM tasks WHERE run_id = %s", (run_id,)).fetchone()["n"]  # type: ignore[index]
        result = validate(dag, budgets=run.budgets, capabilities=capabilities, existing_task_count=existing)
        emit(
            conn,
            run_id,
            "plan.validated" if result.ok else "plan.rejected",
            payload={
                "errors": [str(e) for e in result.errors],
                "auto_added": result.auto_added,
                "task_count": len(result.dag.tasks),
                "created_by": created_by,
            },
        )
        if not result.ok:
            sm.fail_run(conn, run_id, f"invalid plan: {'; '.join(str(e) for e in result.errors)}")
            invalid = result
        else:
            invalid = None
            _insert_tasks(conn, run_id, run, result, created_by)
            if start:
                sm.transition_run(conn, run_id, RunStatus.RUNNING)
    if invalid is not None:  # raised outside the transaction so the FAILED verdict is committed
        raise InvalidDag(invalid)
    return result


def _insert_tasks(conn: Conn, run_id: UUID, run: Run, result: ValidationResult, created_by: str) -> None:
    """Insert validated tasks + dependencies (caller holds the transaction)."""
    ids: dict[str, UUID] = {}
    for t in result.dag.tasks:
        row = conn.execute(
            """
            INSERT INTO tasks (run_id, key, goal, capability, input_contract, output_contract,
                               context_spec, meta, max_attempts, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """,
            (
                run_id,
                t.id,
                t.goal,
                t.capability,
                Jsonb(t.input_contract),
                Jsonb(t.output_contract),
                Jsonb(t.context_spec),
                Jsonb(t.meta),
                t.max_attempts if t.max_attempts is not None else run.budgets.max_attempts_per_task,
                "system" if t.id in result.auto_added else created_by,
            ),
        ).fetchone()
        assert row is not None
        ids[t.id] = row["id"]
        emit(
            conn,
            run_id,
            "task.created",
            task_id=row["id"],
            payload={"key": t.id, "capability": t.capability, "depends_on": t.depends_on},
        )
    for t in result.dag.tasks:
        for d in t.depends_on:
            conn.execute(
                "INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES (%s, %s)",
                (ids[t.id], ids[d]),
            )
    conn.execute("UPDATE runs SET tasks_created = tasks_created + %s WHERE id = %s", (len(result.dag.tasks), run_id))


def create_run_from_dag(
    conn: Conn,
    dag: DagSpec,
    *,
    goal: str | None = None,
    budgets: Budgets | None = None,
    benchmark: str | None = None,
    config: str | None = None,
    capabilities: set[str] | None = None,
    created_by: str = "file",
    pool: str = "default",
) -> Run:
    run = create_run(
        conn,
        goal=goal or dag.goal or "(no goal)",
        budgets=budgets,
        benchmark=benchmark or dag.benchmark,
        config=config,
        pool=pool,
    )
    install_dag(conn, run.id, dag, created_by=created_by, capabilities=capabilities)
    return sm.get_run(conn, run.id)


def summary(conn: Conn, run_id: UUID) -> dict[str, Any]:
    run = sm.get_run(conn, run_id)
    tasks = sm.tasks_for_run(conn, run_id)
    return {
        "run_id": str(run.id),
        "status": run.status.value,
        "verdict": run.verdict,
        "tasks": {t.key: t.status.value for t in tasks},
    }
