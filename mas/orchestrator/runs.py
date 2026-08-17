"""Run creation, DAG installation, and the planning driver (questions ↔ answers, ADR-006).

Deterministic. The DAG comes from a file (M1) or a Planner (StubPlanner now, LLM at step 11).
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any
from uuid import UUID

from mas.artifacts import store
from mas.db.connection import Conn, Jsonb
from mas.db.events import emit
from mas.models.enums import RunStatus
from mas.models.types import Budgets, Run
from mas.orchestrator import state_machine as sm
from mas.planner import contracts as contract_mod
from mas.planner.capabilities import DEFAULT_CAPABILITY_TOOLS
from mas.planner.dag import QA, ContractProposal, DagSpec, Questions
from mas.planner.planner import Planner, PlanRequest
from mas.planner.validator import Remaining, ValidationResult, validate


class InvalidDag(Exception):
    def __init__(self, result: ValidationResult):
        self.result = result
        super().__init__("; ".join(str(e) for e in result.errors))


class NotAwaitingInput(Exception):
    """answer() called on a run that is not AWAITING_INPUT."""


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
                max_tokens, max_cost_usd, max_wallclock_s, max_attempt_runtime_s, max_attempt_tokens, lease_s,
                max_questions, deadline_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                b.max_attempt_tokens,
                b.lease_s,
                b.max_questions,
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


def remaining_budget(conn: Conn, run: Run) -> Remaining:
    """What the run has left right now, measured by the driver for validator rule 8 (budget allocation): tokens, cost,
    wall-clock (one clock from creation, I-4), the existing tasks that still need funding, and this run's observed mean
    attempt duration (the only per-attempt time figure the system owns; None until an attempt has settled)."""
    row = conn.execute(
        """
        SELECT extract(epoch from (now() - %s)) AS elapsed,
               (SELECT count(*) FROM tasks t WHERE t.run_id = %s
                  AND t.status NOT IN ('COMPLETED', 'FAILED', 'BLOCKED', 'CANCELLED')) AS open_tasks,
               (SELECT avg(extract(epoch from (a.finished_at - a.started_at)))
                  FROM attempts a JOIN tasks t ON t.id = a.task_id
                 WHERE t.run_id = %s AND a.status <> 'RUNNING' AND a.finished_at IS NOT NULL) AS observed_s
        """,
        (run.created_at, run.id, run.id),
    ).fetchone()
    assert row is not None
    b = run.budgets
    wall = float(b.max_wallclock_s) - float(row["elapsed"] or 0.0) if run.created_at is not None else float(b.max_wallclock_s)
    if b.deadline_at is not None:
        left = conn.execute("SELECT extract(epoch from (%s - now())) AS s", (b.deadline_at,)).fetchone()
        wall = min(wall, float(left["s"]))  # type: ignore[index]
    return Remaining(
        tokens=b.max_tokens - run.tokens_used,
        wallclock_s=wall,
        cost_usd=float(b.max_cost_usd) - float(run.cost_used_usd),
        open_tasks=int(row["open_tasks"] or 0),
        observed_attempt_s=float(row["observed_s"]) if row["observed_s"] is not None else None,
    )


def install_dag(
    conn: Conn,
    run_id: UUID,
    dag: DagSpec,
    *,
    created_by: str = "planner",
    capabilities: set[str] | None = None,
    start: bool = True,
    plan_attempt: int = 1,
) -> ValidationResult:
    """Validate and insert the DAG's tasks; move the run CREATED → PLANNING → RUNNING (if `start`).

    Raises InvalidDag if validation fails (run is moved to FAILED so it always has a verdict).
    """
    with conn.transaction():
        run = sm.lock_run(conn, run_id)  # lock order: run before task inserts
        if run.status is RunStatus.CREATED:
            sm.transition_run(conn, run_id, RunStatus.PLANNING)
        existing = conn.execute("SELECT count(*) AS n FROM tasks WHERE run_id = %s", (run_id,)).fetchone()["n"]  # type: ignore[index]
        result = validate(
            dag,
            budgets=run.budgets,
            capabilities=capabilities,
            existing_task_count=existing,
            remaining=remaining_budget(conn, run),
        )
        emit(
            conn,
            run_id,
            "plan.validated" if result.ok else "plan.rejected",
            payload={
                "errors": [str(e) for e in result.errors],
                "auto_added": result.auto_added,
                "task_count": len(result.dag.tasks),
                "created_by": created_by,
                "plan_attempt": plan_attempt,
                "shape": result.dag.shape or None,
            },
        )
        if not result.ok:
            sm.fail_run(conn, run_id, f"invalid plan: {'; '.join(str(e) for e in result.errors)}")
            invalid = result
        else:
            invalid = None
            if result.dag.assumptions:
                # ADR-006 policy: proceeded without asking → what was assumed is on the record and vetoable
                store.publish(
                    conn,
                    run_id=run_id,
                    type="assumptions",
                    ref=f"assumptions:{run_id}:{run.questions_asked + 1}",
                    meta={"assumptions": list(result.dag.assumptions), "planner": created_by},
                )
                emit(conn, run_id, "plan.assumptions", payload={"assumptions": list(result.dag.assumptions)})
            _insert_tasks(conn, run_id, run, result, created_by)
            # the plan itself is evidence: the validated DAG + advisory task-shape metadata (ADR-008), on record
            store.publish(
                conn,
                run_id=run_id,
                type="plan",
                ref=f"plan:{run_id}:{plan_attempt}",
                meta={
                    "dag": result.dag.to_dict(),
                    "shape": result.dag.shape,
                    "planner": created_by,
                    "plan_attempt": plan_attempt,
                },
            )
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
                               context_spec, meta, tools, max_attempts, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
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
                Jsonb(list(t.tools or [])),
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


# ----------------------------------------------------------------------------- clarifying questions (ADR-006)


def ask_questions(conn: Conn, run_id: UUID, questions: Questions, *, planner: str = "planner") -> Run:
    """Record a planner question batch and move the run to AWAITING_INPUT.

    Enforces max_questions: asking beyond the budget fails the run with a verdict (never a stall).
    """
    qs = [q.strip() for q in questions.questions if q and q.strip()]
    with conn.transaction():
        run = sm.lock_run(conn, run_id)  # lock order: run before artifact/event inserts
        if run.status not in {RunStatus.PLANNING, RunStatus.REPLANNING}:
            raise sm.IllegalTransition("run", run_id, run.status, RunStatus.AWAITING_INPUT)
        if not qs:
            return sm.fail_run(conn, run_id, "planner returned an empty question batch")
        if run.questions_asked + 1 > run.budgets.max_questions:
            return sm.fail_run(conn, run_id, f"planner exceeded max_questions ({run.budgets.max_questions})")
        conn.execute("UPDATE runs SET questions_asked = questions_asked + 1 WHERE id = %s", (run_id,))
        n = run.questions_asked + 1
        store.publish(
            conn,
            run_id=run_id,
            type="question",
            ref=f"question:{run_id}:{n}",
            meta={"batch": n, "questions": qs, "context": questions.context, "planner": planner},
        )
        emit(conn, run_id, "plan.questions", payload={"batch": n, "questions": qs, "context": questions.context})
        return sm.transition_run(conn, run_id, RunStatus.AWAITING_INPUT, payload={"batch": n, "questions": qs})


def answer(conn: Conn, run_id: UUID, text: str, *, by: str = "human") -> Run:
    """Record the human's answer to the pending batch and send the run back to (re)planning."""
    with conn.transaction():
        run = sm.lock_run(conn, run_id)
        if run.status is not RunStatus.AWAITING_INPUT:
            raise NotAwaitingInput(f"run {run_id} is {run.status.value}, not AWAITING_INPUT")
        n = run.questions_asked
        store.publish(
            conn,
            run_id=run_id,
            type="answer",
            ref=f"answer:{run_id}:{n}",
            meta={"batch": n, "answer": text, "by": by},
        )
        emit(conn, run_id, "plan.answered", payload={"batch": n, "answer": text, "by": by})
        back_to = RunStatus.REPLANNING if run.tasks_created > 0 else RunStatus.PLANNING
        return sm.transition_run(conn, run_id, back_to, payload={"batch": n})


def qa_history(conn: Conn, run_id: UUID) -> list[QA]:
    """Ordered (questions, answer) pairs; the current unanswered batch, if any, is excluded."""
    rows = conn.execute(
        "SELECT type, meta FROM artifacts WHERE run_id = %s AND type IN ('question','answer') ORDER BY created_at, id",
        (run_id,),
    ).fetchall()
    out: list[QA] = []
    pending: list[str] | None = None
    for r in rows:
        if r["type"] == "question":
            pending = list(r["meta"].get("questions", []))
        elif pending is not None:
            out.append(QA(questions=pending, answer=str(r["meta"].get("answer", ""))))
            pending = None
    return out


def pending_questions(conn: Conn, run_id: UUID) -> list[str]:
    run = sm.get_run(conn, run_id)
    if run.status is not RunStatus.AWAITING_INPUT:
        return []
    row = conn.execute(
        "SELECT meta FROM artifacts WHERE run_id = %s AND type = 'question' ORDER BY created_at DESC, id DESC LIMIT 1",
        (run_id,),
    ).fetchone()
    return list(row["meta"].get("questions", [])) if row else []


def _plan_request(conn: Conn, run: Run, capabilities: set[str], *, plan_attempt: int, errors: tuple[str, ...]) -> PlanRequest:
    contract = contract_mod.approved(conn, run.id)
    remaining_s = None
    if run.created_at is not None:
        row = conn.execute("SELECT extract(epoch from (now() - %s)) AS e", (run.created_at,)).fetchone()
        remaining_s = max(0.0, float(run.budgets.max_wallclock_s) - float(row["e"]))  # type: ignore[index]
    return PlanRequest(
        goal=run.goal,
        capabilities=frozenset(capabilities),
        qa=tuple(qa_history(conn, run.id)),
        remaining={
            "questions": run.budgets.max_questions - run.questions_asked,
            "tasks": run.budgets.max_tasks - run.tasks_created,
            "tokens": run.budgets.max_tokens - run.tokens_used,
            "plan_attempts": run.budgets.max_plan_attempts - plan_attempt + 1,
            "max_concurrency": run.budgets.max_concurrency,
            "max_attempts_per_task": run.budgets.max_attempts_per_task,
            "max_attempt_tokens": run.budgets.max_attempt_tokens,
            "max_attempt_runtime_s": run.budgets.max_attempt_runtime_s,
            "wallclock_s": int(remaining_s) if remaining_s is not None else None,
        },
        plan_attempt=plan_attempt,
        validation_errors=errors,
        run_id=run.id,
        benchmark=run.benchmark,
        needs_contract=run.benchmark is None and contract is None,
        contract=contract,
        deadline_s=remaining_s,
        tool_registry={c: tuple(sorted(DEFAULT_CAPABILITY_TOOLS.get(c, ()))) for c in sorted(capabilities)},
    )


def plan_run(conn: Conn, run_id: UUID, planner: Planner, *, capabilities: set[str]) -> Run:
    """One planning round: ask the planner up to `max_plan_attempts` times; every rejection goes back to it as data.

    Deterministic driver (ADR-001). Exactly one of the planner's typed outcomes is acted on per call:
      questions  → `ask_questions` (AWAITING_INPUT), the human answers with `mas answer`
      contract   → only for an ad-hoc goal without a frozen contract: validated, recorded, AWAITING_INPUT for approval
      dag        → validated (rules 1–8, 10 + shape) and installed → RUNNING; a DAG before the contract is frozen is rejected
    Anything invalid is returned to the planner with the errors; when the plan-attempt budget is gone the run FAILS
    with a verdict. Call when the run is CREATED/PLANNING; idempotent on other states.
    """
    run = sm.get_run(conn, run_id)
    if run.status is RunStatus.CREATED:
        with conn.transaction():
            run = sm.transition_run(conn, run_id, RunStatus.PLANNING)
    if run.status is not RunStatus.PLANNING:
        return run
    who = getattr(planner, "name", "planner")
    errors: tuple[str, ...] = ()
    last: str = ""
    for attempt in range(1, run.budgets.max_plan_attempts + 1):
        req = _plan_request(conn, run, capabilities, plan_attempt=attempt, errors=errors)
        try:
            out = planner.plan(req)
        except Exception as e:  # noqa: BLE001 - a planner error (provider outage, budget, malformed output) is a run verdict
            with conn.transaction():
                emit(conn, run_id, "plan.error", payload={"attempt": attempt, "error": f"{type(e).__name__}: {e}"[:500]})
                return sm.fail_run(conn, run_id, f"planner failed: {type(e).__name__}: {e}"[:400])
        if isinstance(out, Questions):
            return ask_questions(conn, run_id, out, planner=who)
        if isinstance(out, ContractProposal):
            if not req.needs_contract:
                errors = ("a contract already exists for this run (benchmark suite or frozen contract): produce the DAG",)
                last = errors[0]
                with conn.transaction():
                    emit(conn, run_id, "plan.rejected", payload={"attempt": attempt, "kind": "contract", "errors": list(errors)})
                continue
            errs = contract_mod.validate_proposal(out)
            if errs:
                errors = tuple(errs)
                last = "; ".join(errs)
                with conn.transaction():
                    emit(conn, run_id, "plan.rejected", payload={"attempt": attempt, "kind": "contract", "errors": errs})
                continue
            return contract_mod.propose(conn, run_id, out, planner=who)
        # a DAG
        if req.needs_contract:
            errors = ("this goal has no acceptance contract yet: propose the contract first (kind=contract), not a DAG",)
            last = errors[0]
            with conn.transaction():
                emit(conn, run_id, "plan.rejected", payload={"attempt": attempt, "kind": "dag", "errors": list(errors)})
            continue
        with conn.transaction():
            existing = conn.execute("SELECT count(*) AS n FROM tasks WHERE run_id = %s", (run_id,)).fetchone()["n"]  # type: ignore[index]
            remaining = remaining_budget(conn, run)
        result = validate(out, budgets=run.budgets, capabilities=capabilities, existing_task_count=existing, remaining=remaining)
        if not result.ok:
            errors = tuple(str(e) for e in result.errors)
            last = "; ".join(errors)
            with conn.transaction():
                emit(conn, run_id, "plan.rejected", payload={"attempt": attempt, "kind": "dag", "errors": list(errors)})
            continue
        install_dag(conn, run_id, out, created_by=who, capabilities=capabilities, plan_attempt=attempt)
        return sm.get_run(conn, run_id)
    with conn.transaction():
        return sm.fail_run(
            conn, run_id, f"planner exhausted max_plan_attempts ({run.budgets.max_plan_attempts}); last: {last}"[:400]
        )


def summary(conn: Conn, run_id: UUID) -> dict[str, Any]:
    run = sm.get_run(conn, run_id)
    tasks = sm.tasks_for_run(conn, run_id)
    return {
        "run_id": str(run.id),
        "status": run.status.value,
        "verdict": run.verdict,
        "tasks": {t.key: t.status.value for t in tasks},
    }
