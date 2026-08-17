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
from mas.models.enums import RunStatus, TaskStatus, VerdictReason
from mas.models.types import Budgets, Run
from mas.orchestrator import budgets as budget_rules
from mas.orchestrator import leases, progress
from mas.orchestrator import state_machine as sm
from mas.planner import contracts as contract_mod
from mas.planner.capabilities import DEFAULT_CAPABILITY_TOOLS
from mas.planner.dag import QA, ContractProposal, DagSpec, Questions
from mas.planner.planner import Planner, PlanRequest
from mas.planner.validator import ExistingTask, Remaining, ValidationError, ValidationResult, validate


class InvalidDag(Exception):
    def __init__(self, result: ValidationResult):
        self.result = result
        super().__init__("; ".join(str(e) for e in result.errors))


def reason_for(errors: list[ValidationError] | list[str]) -> VerdictReason:
    """Verdict reason code for a plan that was rejected (ADR-008 §6): only policy denials → POLICY_DENIED; a repeated
    amendment (rule 9, no progress) → NO_PROGRESS; unmappable acceptance criteria → UNSUPPORTED; else INVALID_PLAN."""
    texts = [str(e) for e in errors]
    if texts and all("prohibited by policy" in t for t in texts):
        return VerdictReason.POLICY_DENIED
    if texts and all("repeats" in t and "amendment" in t for t in texts):
        return VerdictReason.NO_PROGRESS
    if texts and all("unmappable" in t for t in texts):
        return VerdictReason.UNSUPPORTED
    return VerdictReason.INVALID_PLAN


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
    """What the run has left right now, measured by the driver for validator rule 8 (budget allocation): tokens (net of
    what RUNNING attempts have reserved), cost, wall-clock (one clock from creation, I-4), the existing tasks that still
    need funding, and this run's observed *minimum* attempt duration (a lower bound; None until an attempt settled)."""
    row = conn.execute(
        """
        SELECT extract(epoch from (now() - %s)) AS elapsed,
               (SELECT count(*) FROM tasks t WHERE t.run_id = %s
                  AND t.status NOT IN ('COMPLETED', 'FAILED', 'BLOCKED', 'CANCELLED')) AS open_tasks,
               (SELECT min(extract(epoch from (a.finished_at - a.started_at)))
                  FROM attempts a JOIN tasks t ON t.id = a.task_id
                 WHERE t.run_id = %s AND a.status = 'SUCCESS' AND a.finished_at IS NOT NULL) AS observed_ok_s,
               (SELECT min(extract(epoch from (a.finished_at - a.started_at)))
                  FROM attempts a JOIN tasks t ON t.id = a.task_id
                 WHERE t.run_id = %s AND a.status <> 'RUNNING' AND a.finished_at IS NOT NULL) AS observed_any_s
        """,
        (run.created_at, run.id, run.id, run.id),
    ).fetchone()
    assert row is not None
    b = run.budgets
    wall = float(b.max_wallclock_s) - float(row["elapsed"] or 0.0) if run.created_at is not None else float(b.max_wallclock_s)
    if b.deadline_at is not None:
        left = conn.execute("SELECT extract(epoch from (%s - now())) AS s", (b.deadline_at,)).fetchone()
        wall = min(wall, float(left["s"]))  # type: ignore[index]
    return Remaining(
        tokens=b.max_tokens - run.tokens_used - leases.reserved_tokens(conn, run.id),
        wallclock_s=wall,
        cost_usd=float(b.max_cost_usd) - float(run.cost_used_usd),
        open_tasks=int(row["open_tasks"] or 0),
        observed_attempt_s=_observed(row),
    )


def _shortest_attempt_s(conn: Conn, run: Run) -> float | None:
    if run.status is not RunStatus.REPLANNING:
        return None
    return remaining_budget(conn, run).observed_attempt_s


def _observed(row: dict[str, Any]) -> float | None:
    """A *lower bound* on how long a task takes here: the shortest successful attempt so far (a slow history — one
    long timeout — must not reject a plan that can complete faster); only if nothing succeeded yet, the shortest
    settled attempt of any kind."""
    v = row.get("observed_ok_s")
    if v is None:
        v = row.get("observed_any_s")
    return float(v) if v is not None else None


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
    """Validate and insert the DAG's tasks; move the run CREATED → PLANNING → RUNNING (if `start`). On a REPLANNING run
    the DAG is an *amendment* (new tasks only, rule 9) and the run goes REPLANNING → RUNNING.

    Raises InvalidDag if validation fails (run is moved to FAILED so it always has a verdict).
    """
    with conn.transaction():
        run = sm.lock_run(conn, run_id)  # lock order: run before task inserts
        if run.status is RunStatus.CREATED:
            sm.transition_run(conn, run_id, RunStatus.PLANNING)
        amendment = run.status is RunStatus.REPLANNING
        prior = existing_tasks(conn, run_id) if amendment else []
        existing = conn.execute("SELECT count(*) AS n FROM tasks WHERE run_id = %s", (run_id,)).fetchone()["n"]  # type: ignore[index]
        result = validate(
            dag,
            budgets=run.budgets,
            capabilities=capabilities,
            existing_task_count=existing,
            remaining=remaining_budget(conn, run),
            existing=prior if amendment else None,
        )
        a_hash = progress.amendment_hash([t.to_dict() for t in result.dag.tasks], result.dag.cancel) if amendment else None
        if amendment and result.ok and a_hash in previous_amendments(conn, run_id):
            result.errors.append(ValidationError("9", "amendment repeats an earlier amendment (no progress)"))
        emit(
            conn,
            run_id,
            "plan.validated" if result.ok else "plan.rejected",
            payload={
                "errors": [str(e) for e in result.errors],
                "warnings": [str(w) for w in result.warnings],
                "auto_added": result.auto_added,
                "task_count": len(result.dag.tasks),
                "created_by": created_by,
                "plan_attempt": plan_attempt,
                "shape": result.dag.shape or None,
                "amendment": amendment,
                "replan": run.replans_used if amendment else 0,
                "amendment_hash": a_hash,
            },
        )
        if not result.ok:
            sm.fail_run(
                conn,
                run_id,
                f"invalid {'amendment' if amendment else 'plan'}: {'; '.join(str(e) for e in result.errors)}",
                code=reason_for(result.errors),
            )
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
            _insert_tasks(conn, run_id, run, result, created_by, existing_keys={e.key for e in prior})
            if amendment:  # every FAILED task on record was shown to the planner: this amendment is its answer
                conn.execute(
                    "UPDATE tasks SET meta = meta || %s "
                    "WHERE run_id = %s AND status = 'FAILED' AND NOT (meta ? 'repair_handled')",
                    (Jsonb({"repair_handled": run.replans_used}), run_id),
                )
            for key in result.dag.cancel:  # obsolete pending work named by the amendment (validated: PENDING/READY only)
                row = conn.execute(
                    "SELECT id FROM tasks WHERE run_id = %s AND key = %s FOR NO KEY UPDATE", (run_id, key)
                ).fetchone()
                if row is not None:
                    sm.transition_task(
                        conn, row["id"], TaskStatus.CANCELLED, payload={"reason": "amendment", "replan": run.replans_used}
                    )
            # the plan itself is evidence: the validated DAG + advisory task-shape metadata (ADR-008), on record;
            # an amendment is recorded with its hash (repeating one is no progress)
            ref = f"plan:{run_id}:r{run.replans_used}" if amendment else f"plan:{run_id}:{plan_attempt}"
            store.publish(
                conn,
                run_id=run_id,
                type="plan",
                ref=ref,
                meta={
                    "dag": result.dag.to_dict(),
                    "shape": result.dag.shape,
                    "planner": created_by,
                    "plan_attempt": plan_attempt,
                    "amendment": amendment,
                    "replan": run.replans_used if amendment else 0,
                    "amendment_hash": a_hash,
                },
            )
            if start:
                sm.transition_run(conn, run_id, RunStatus.RUNNING)
    if invalid is not None:  # raised outside the transaction so the FAILED verdict is committed
        raise InvalidDag(invalid)
    return result


def existing_tasks(conn: Conn, run_id: UUID) -> list[ExistingTask]:
    """The run's recorded tasks with their dependency keys — what an amendment may build on (rule 9)."""
    rows = conn.execute(
        """
        SELECT t.key, t.status,
               coalesce(array_agg(d.key ORDER BY d.key) FILTER (WHERE d.key IS NOT NULL), '{}') AS deps
          FROM tasks t
          LEFT JOIN task_dependencies td ON td.task_id = t.id
          LEFT JOIN tasks d ON d.id = td.depends_on_task_id
         WHERE t.run_id = %s GROUP BY t.id, t.key, t.status, t.created_at ORDER BY t.created_at, t.key
        """,
        (run_id,),
    ).fetchall()
    return [ExistingTask(key=r["key"], status=r["status"], depends_on=tuple(r["deps"] or ())) for r in rows]


def planned_dag(conn: Conn, run_id: UUID) -> dict[str, Any] | None:
    """The run's initial validated DAG as recorded by `install_dag` (the `plan` artifact), or None if it never planned.

    This is the plan *after* validation — auto-added tasks included — which is what `mas plan` exports so another run
    can execute exactly what was validated here (ADR-009)."""
    row = conn.execute(
        "SELECT meta FROM artifacts WHERE run_id = %s AND type = 'plan' AND NOT COALESCE((meta->>'amendment')::boolean, false) "
        "ORDER BY created_at, id LIMIT 1",
        (run_id,),
    ).fetchone()
    dag = (row["meta"] or {}).get("dag") if row is not None else None
    return dag if isinstance(dag, dict) else None


def previous_amendments(conn: Conn, run_id: UUID) -> list[str]:
    rows = conn.execute(
        "SELECT meta->>'amendment_hash' AS h FROM artifacts WHERE run_id = %s AND type = 'plan' "
        "AND (meta->>'amendment')::boolean ORDER BY created_at",
        (run_id,),
    ).fetchall()
    return [r["h"] for r in rows if r["h"]]


def _insert_tasks(
    conn: Conn, run_id: UUID, run: Run, result: ValidationResult, created_by: str, *, existing_keys: set[str] | None = None
) -> None:
    """Insert validated tasks + dependencies (caller holds the transaction). Amendment tasks may depend on existing keys."""
    ids: dict[str, UUID] = {}
    if existing_keys:
        for r in conn.execute("SELECT id, key FROM tasks WHERE run_id = %s AND key = ANY(%s)", (run_id, list(existing_keys))):
            ids[r["key"]] = r["id"]
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
            return sm.fail_run(conn, run_id, "planner returned an empty question batch", code=VerdictReason.INVALID_PLAN)
        if run.questions_asked + 1 > run.budgets.max_questions:
            return sm.fail_run(
                conn,
                run_id,
                f"planner exceeded max_questions ({run.budgets.max_questions})",
                code=VerdictReason.BUDGET_EXHAUSTED,
            )
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


def _amendment_context(conn: Conn, run: Run) -> dict[str, Any]:
    """What the planner needs to propose a repair: the recorded tasks (with outputs), the failure it must fix, and the
    amendments already tried. Bounded; read-only; the planner still has no authority over any of it."""
    tasks = conn.execute(
        "SELECT id, key, capability, goal, status FROM tasks WHERE run_id = %s ORDER BY created_at, key", (run.id,)
    ).fetchall()
    deps = {e.key: list(e.depends_on) for e in existing_tasks(conn, run.id)}
    existing = []
    for t in tasks:
        outs = [f"{a.type}:{a.ref}" for a in store.outputs_of_task(conn, t["id"])][:20]
        existing.append(
            {
                "key": t["key"],
                "capability": t["capability"],
                "goal": t["goal"][:300],
                "status": t["status"],
                "depends_on": deps.get(t["key"], []),
                "outputs": outs,
            }
        )
    trig = conn.execute(
        "SELECT payload FROM events WHERE run_id = %s AND type = 'run.replanning' ORDER BY id DESC LIMIT 1", (run.id,)
    ).fetchone()
    trigger = dict(trig["payload"]) if trig is not None else {}
    kind = trigger.get("trigger", "verification_failed")
    report: dict[str, Any] | None = {"trigger": kind}
    if kind == "task_failed":
        key = trigger.get("task")
        failed = conn.execute(  # every FAILED task no amendment has addressed yet, the triggering one first
            "SELECT id, key FROM tasks WHERE run_id = %s AND status = 'FAILED' AND NOT (meta ? 'repair_handled') "
            "ORDER BY (key = %s) DESC, updated_at",
            (run.id, key),
        ).fetchall()
        failed_tasks = []
        for f in failed:
            atts = conn.execute(
                "SELECT attempt_number, status, failure_reason FROM attempts WHERE task_id = %s ORDER BY attempt_number",
                (f["id"],),
            ).fetchall()
            failed_tasks.append(
                {
                    "key": f["key"],
                    "attempts": [
                        {"n": a["attempt_number"], "status": a["status"], "failure_reason": str(a["failure_reason"] or "")[:500]}
                        for a in atts
                    ],
                }
            )
        report.update(
            {
                "task": key,
                "attempts": failed_tasks[0]["attempts"] if failed_tasks else [],
                "failed_tasks": failed_tasks,
                "blocked": list(trigger.get("blocked", [])),
            }
        )
    elif kind == "new_work_required":
        report.update({"task": trigger.get("task"), "detail": str(trigger.get("detail") or "")[:1000]})
    last = conn.execute(
        "SELECT meta FROM artifacts WHERE run_id = %s AND type = 'verification' ORDER BY created_at DESC, id DESC LIMIT 1",
        (run.id,),
    ).fetchone()
    if last is not None and kind == "verification_failed":
        rep = dict(last["meta"].get("report") or {})
        checks = [
            {"id": c.get("id"), "status": c.get("status"), "detail": str(c.get("detail") or "")[:500]}
            for c in (rep.get("checks") or [])
            if isinstance(c, dict)
        ]
        report.update(
            {
                "status": rep.get("status"),
                "reason": str(rep.get("reason") or "")[:500],
                "checks": checks,
                "failing": [c["id"] for c in checks if c["status"] != "PASS"],
            }
        )
        for k in ("stdout", "stderr"):
            if isinstance(rep.get(k), str) and rep[k]:
                report[k] = rep[k][-2000:]
    return {
        "amendment": True,
        "replan": run.replans_used,
        "existing_tasks": tuple(existing),
        "failure_report": report,
        "previous_amendments": tuple(previous_amendments(conn, run.id)),
    }


def _plan_request(conn: Conn, run: Run, capabilities: set[str], *, plan_attempt: int, errors: tuple[str, ...]) -> PlanRequest:
    contract = contract_mod.approved(conn, run.id)
    remaining_s = None
    if run.created_at is not None:
        row = conn.execute("SELECT extract(epoch from (now() - %s)) AS e", (run.created_at,)).fetchone()
        remaining_s = max(0.0, float(run.budgets.max_wallclock_s) - float(row["e"]))  # type: ignore[index]
    extra = _amendment_context(conn, run) if run.status is RunStatus.REPLANNING else {}
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
            "replans": run.budgets.max_replans - run.replans_used,
            "shortest_attempt_s": _shortest_attempt_s(conn, run),  # advisory history for the planner's own estimates
        },
        plan_attempt=plan_attempt,
        validation_errors=errors,
        run_id=run.id,
        benchmark=run.benchmark,
        # the contract gate applies to the initial plan of an ad-hoc goal; an amendment repairs a run that already has
        # its definition of done (a suite, a frozen contract, or a hand-written DAG run without one)
        needs_contract=run.status is RunStatus.PLANNING and run.benchmark is None and contract is None,
        contract=contract,
        deadline_s=remaining_s,
        tool_registry={c: tuple(sorted(DEFAULT_CAPABILITY_TOOLS.get(c, ()))) for c in sorted(capabilities)},
        **extra,
    )


def plan_run(conn: Conn, run_id: UUID, planner: Planner, *, capabilities: set[str]) -> Run:
    """One planning round: ask the planner up to `max_plan_attempts` times; every rejection goes back to it as data.

    Deterministic driver (ADR-001). Exactly one of the planner's typed outcomes is acted on per call:
      questions  → `ask_questions` (AWAITING_INPUT), the human answers with `mas answer`
      contract   → only for an ad-hoc goal without a frozen contract: validated, recorded, AWAITING_INPUT for approval
      dag        → validated (rules 1–8, 10 + shape) and installed → RUNNING; a DAG before the contract is frozen is rejected
      amendment  → on a REPLANNING run (bounded repair, 13-lite) the DAG holds new tasks only: rule 9 protects recorded
                   work, a repeated amendment is rejected as no progress, then it is installed → RUNNING
    Anything invalid is returned to the planner with the errors; when the plan-attempt budget is gone the run FAILS
    with a verdict and a reason code. Call when the run is CREATED/PLANNING/REPLANNING; idempotent on other states.
    """
    run = sm.get_run(conn, run_id)
    if run.status is RunStatus.CREATED:
        with conn.transaction():
            run = sm.transition_run(conn, run_id, RunStatus.PLANNING)
    if run.status not in {RunStatus.PLANNING, RunStatus.REPLANNING}:
        return run
    amendment = run.status is RunStatus.REPLANNING  # bounded repair (13-lite): the planner proposes an amendment
    who = getattr(planner, "name", "planner")
    errors: tuple[str, ...] = ()
    last: str = ""
    last_errors: list[str] = []
    for attempt in range(1, run.budgets.max_plan_attempts + 1):
        req = _plan_request(conn, run, capabilities, plan_attempt=attempt, errors=errors)
        try:
            out = planner.plan(req)
        except Exception as e:  # noqa: BLE001 - a planner error (provider outage, budget, malformed output) is a run verdict
            settle_planner_usage(conn, run_id)  # what it spent before failing is charged all the same
            with conn.transaction():
                emit(conn, run_id, "plan.error", payload={"attempt": attempt, "error": f"{type(e).__name__}: {e}"[:500]})
                if sm.get_run(conn, run_id).status.terminal:
                    return sm.get_run(conn, run_id)  # the settlement already ended it (budget)
                return sm.fail_run(conn, run_id, f"planner failed: {type(e).__name__}: {e}"[:400], code=_planner_error_reason(e))
        settle_planner_usage(conn, run_id)  # planner tokens/cost count against the run's hard budget (I-4)
        run = sm.get_run(conn, run_id)
        if run.status.terminal:
            return run  # the planning round itself exhausted a budget: verdict, nothing installed
        if isinstance(out, Questions):
            return ask_questions(conn, run_id, out, planner=who)
        if isinstance(out, ContractProposal):
            if not req.needs_contract:
                errors = ("a contract already exists for this run (benchmark suite or frozen contract): produce the DAG",)
                last, last_errors = errors[0], list(errors)
                with conn.transaction():
                    emit(conn, run_id, "plan.rejected", payload={"attempt": attempt, "kind": "contract", "errors": list(errors)})
                continue
            errs = contract_mod.validate_proposal(out)
            if errs:
                errors = tuple(errs)
                last, last_errors = "; ".join(errs), list(errs)
                with conn.transaction():
                    emit(conn, run_id, "plan.rejected", payload={"attempt": attempt, "kind": "contract", "errors": errs})
                continue
            return contract_mod.propose(conn, run_id, out, planner=who)
        # a DAG
        if req.needs_contract:
            errors = ("this goal has no acceptance contract yet: propose the contract first (kind=contract), not a DAG",)
            last, last_errors = errors[0], list(errors)
            with conn.transaction():
                emit(conn, run_id, "plan.rejected", payload={"attempt": attempt, "kind": "dag", "errors": list(errors)})
            continue
        with conn.transaction():
            existing = conn.execute("SELECT count(*) AS n FROM tasks WHERE run_id = %s", (run_id,)).fetchone()["n"]  # type: ignore[index]
            remaining = remaining_budget(conn, run)
            prior = existing_tasks(conn, run_id) if amendment else None
            tried = previous_amendments(conn, run_id) if amendment else []
        result = validate(
            out,
            budgets=run.budgets,
            capabilities=capabilities,
            existing_task_count=existing,
            remaining=remaining,
            existing=prior,
        )
        if (
            amendment
            and result.ok
            and progress.amendment_hash([t.to_dict() for t in result.dag.tasks], result.dag.cancel) in tried
        ):
            result.errors.append(
                ValidationError("9", "amendment repeats an earlier amendment (no progress): propose a different repair")
            )
        if not result.ok:
            errors = tuple(str(e) for e in result.errors)
            last, last_errors = "; ".join(errors), list(errors)
            with conn.transaction():
                emit(
                    conn,
                    run_id,
                    "plan.rejected",
                    payload={"attempt": attempt, "kind": "amendment" if amendment else "dag", "errors": list(errors)},
                )
            continue
        install_dag(conn, run_id, out, created_by=who, capabilities=capabilities, plan_attempt=attempt)
        return sm.get_run(conn, run_id)
    with conn.transaction():
        return sm.fail_run(
            conn,
            run_id,
            f"planner exhausted max_plan_attempts ({run.budgets.max_plan_attempts}); last: {last}"[:400],
            code=reason_for(last_errors),
        )


def settle_planner_usage(conn: Conn, run_id: UUID) -> dict[str, Any] | None:
    """Charge the planner's model calls to the run (hard total budget, I-4). Settled from telemetry — the meter's
    `model_calls` rows written as each call finished — never from anything the planner reports. Idempotent: rows are
    marked settled. Returns what was charged (or None), and aborts the run with a verdict if that crossed a budget."""
    with conn.transaction():
        run = sm.lock_run(conn, run_id)
        rows = conn.execute(
            """
            UPDATE model_calls SET settled = true
             WHERE run_id = %s AND attempt_id IS NULL AND NOT settled
            RETURNING input_tokens, output_tokens, cost_usd, priced, role
            """,
            (run_id,),
        ).fetchall()
        if not rows:
            return None
        tokens = sum(int(r["input_tokens"]) + int(r["output_tokens"]) for r in rows)
        cost = float(sum(float(r["cost_usd"]) for r in rows))
        unpriced = sum(1 for r in rows if not r["priced"])
        conn.execute(
            "UPDATE runs SET tokens_used = tokens_used + %s, cost_used_usd = cost_used_usd + %s WHERE id = %s",
            (tokens, cost, run_id),
        )
        charged = {"calls": len(rows), "tokens": tokens, "cost_usd": round(cost, 6), "unpriced_calls": unpriced}
        emit(conn, run_id, "plan.usage", payload=charged)
        run = sm.get_run(conn, run_id)
        reason = budget_rules.violation(run)
        if reason and not run.status.terminal:
            sm.abort_run(conn, run_id, reason)
        return charged


def _planner_error_reason(e: Exception) -> VerdictReason:
    name = type(e).__name__
    if name in {"AttemptBudgetExceeded", "AttemptDeadlineExceeded", "AttemptCancelled"}:
        return VerdictReason.BUDGET_EXHAUSTED
    if name == "PlannerOutputError":
        return VerdictReason.INVALID_PLAN
    return VerdictReason.UNRECOVERABLE_FAILURE


def summary(conn: Conn, run_id: UUID) -> dict[str, Any]:
    run = sm.get_run(conn, run_id)
    tasks = sm.tasks_for_run(conn, run_id)
    return {
        "run_id": str(run.id),
        "status": run.status.value,
        "verdict": run.verdict,
        "tasks": {t.key: t.status.value for t in tasks},
    }
