# 006 — Clarifying questions: the planner may ask before it plans (`AWAITING_INPUT`)
Status: Accepted
Date: 2026-08-16

## Context
MAST failure mode 2.2, *fail to ask for clarification*: agents guess instead of asking, and all downstream work inherits the wrong premise. Our frozen design had no channel for a question — the planner had to emit a DAG. The user's requirement is explicit: "if it needs more information it asks". Adding this touches the run state machine (frozen in ADR-001/architecture §4), hence this ADR.

## Decision
1. **Planner output is a union:** `DagSpec` *or* `Questions` (`{"questions": ["…", "…"], "context": "…"}`). Typed JSON, validated like everything else; a question batch may not be empty.
2. **New run state `AWAITING_INPUT`.** Transitions: `PLANNING → AWAITING_INPUT`, `REPLANNING → AWAITING_INPUT` (planner asked); `AWAITING_INPUT → PLANNING | REPLANNING` (answer recorded; back to `REPLANNING` if the run already has tasks, else `PLANNING`); `AWAITING_INPUT → FAILED | ABORTED`.
3. **Questions and answers are artifacts** (`type=question`, `type=answer`, run-level, immutable) and events (`plan.questions`, `plan.answered`). The planner is re-invoked with the full Q&A history.
4. **Bounded:** `runs.max_questions` (default 3). Asking beyond it → run `FAILED:planner exceeded max_questions`. **The clock keeps running:** wall-clock is measured from run creation, so a run waiting on a human still hits `max_wallclock_s`/`deadline_at` and ends `ABORTED` with a verdict (I-4).
5. **Deterministic driver:** `plan_run()` calls the planner and either installs the DAG or records the questions; `mas answer <run_id> "…"` records the answer; the orchestrator tick re-plans when it owns a planner. No model in the orchestrator (I-1) — the planner is injected.
6. **Fairness:** the single-agent baseline (configs A/B) runs through the same driver, so it gets the same right to ask through the same channel. Worker-level questions mid-task are *not* in scope (a worker reports `new_work_required` → re-plan, step 13).
7. **Policy — ask rarely, assume visibly.** A question costs human latency, which is exactly what hands the race to a single agent that just assumes and moves. So the planner asks **only when different answers would materially change the DAG or the acceptance criteria**; otherwise it proceeds and **states its assumptions** (`DagSpec.assumptions` → `assumptions` artifact + `plan.assumptions` event), which are auditable and vetoable. This is the same rule a careful senior engineer follows. The LLM planner's prompt (step 11) must encode it; the evaluation measures both `questions` and `human_wait_s`.
8. **Measurement:** metrics report `total_s` (creation→finish), `human_wait_s` (time in `AWAITING_INPUT`) and `machine_s = total − human_wait` separately, so comparisons between configurations are not confounded by how long a human took to answer.

## Consequences
- The run state machine gains one state; every non-terminal state still reaches a terminal one and can be aborted (tested).
- Human-in-the-loop is explicit, budgeted and audited instead of emergent prompt behaviour.
- The stub planner can script questions, so the flow is tested end-to-end without an LLM (M1 discipline).

## Alternatives considered
- **Let the planner guess and re-plan on failure:** rejected — burns budget on a known-wrong premise; that is exactly MAST 2.2.
- **Questions as a special task in the DAG:** rejected — a task nobody can claim; termination semantics get muddy.
- **Unbounded waiting:** rejected — violates I-4.
