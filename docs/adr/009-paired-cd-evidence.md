# 009 — Paired C/D evidence: one plan per (N, repetition), replayed; plan-only runs
Status: Accepted
Date: 2026-08-17

## Context
[evaluation.md §3](../evaluation.md) says C isolates "decomposition without parallelism" and D "full MAS", and that
"C and D are the same code path with one knob". The M3 harness did not implement that: every C run and every D run
invoked the live planner separately, so each got its **own** DAG. Two independent LLM plans differ in task count,
split and dependencies, and the planner is additionally told the run's `max_concurrency` (`_plan_request.remaining`),
which is 1 for C and N for D — so the two configurations were being planned *for different machines*.

A D-versus-C difference therefore measured "different plan **plus** different concurrency", not concurrency. With
five repetitions per cell, plan variance is not averaged away either: it is confounded with the treatment.

A second, related defect: the matrix executed every repetition of A, then B, then C, then D, in fixed width order.
Provider load, rate limits, cache warmth and time of day drift over a matrix that takes hours, and that drift lined up
with configuration.

## Decision
1. **One plan per (N, repetition), shared by C and D.** Before a `(N, repetition)` block runs, the harness produces
   exactly one validated DAG for that block with the live planner, records it as a file plus its SHA-256, and both C
   and D execute *that* DAG (`mas run --dag`). Neither configuration plans its own initial DAG any more.
2. **The shared plan is produced under the parallel budget** (`max_concurrency = N`), i.e. the planner is asked once,
   for the problem, not once per execution mode. C then installs the same DAG under its own frozen budget
   (`max_concurrency = 1`, evaluation.md §3 unchanged). Validation is not relaxed for the replay: if rule 8 finds the
   decomposition infeasible sequentially within the equal wall-clock budget, C fails with `INVALID_PLAN` and that is
   recorded as an experimental result — "this decomposition does not fit the equal budget without parallelism" is a
   finding about C, not an error in the harness.
3. **Planning is measured, not hidden.** The plan-only run is a real run: its planner calls are metered and charged
   the ordinary way (`runs.settle_planner_usage`), and its cost, tokens, latency, plan attempts and rejections are
   recorded per block in `plans.jsonl` and reported in `analysis.md`. Because C and D share one plan, planning cost is
   reported once per block instead of being counted inside either configuration.
4. **`mas plan` produces such a run**: create run → `runs.plan_run` (the ordinary deterministic driver, validator
   included) → export the validated DAG recorded in the `plan` artifact → end the run. Questions or a contract
   proposal end it the same way, unexecuted, and are reported to the caller.
5. **A run ended because execution was never requested is `ABORTED` with the new reason code `CANCELLED`**
   (extending the six codes of ADR-008 §6). It is not `BUDGET_EXHAUSTED` (no budget was exhausted), not
   `INVALID_PLAN` (the plan validated), and not a failure of the system under test. `CANCELLED` means: an operator
   ended this run deliberately; it is not evidence about MAS. `scripts/benchmark.py` classifies it as
   `infrastructure`, never as an experimental outcome.
6. **The execution order is a deterministic randomized block schedule.** The harness draws the order of the
   `(N, repetition)` blocks and the order of A/B/C/D inside each block from a seeded PRNG, and freezes both the seed
   and the resulting schedule in `experiment.json`. A resumed experiment replays the recorded schedule; changing the
   seed is a different experiment.

## Consequences
- C versus D becomes a **paired** comparison: identical goal, identical validated DAG, identical suite, identical
  budgets, one difference (`max_concurrency`, hence how many tasks execute at once).
- A versus D and B versus D remain **unpaired, system-level** comparisons (single agent versus planner + workers) —
  which is what they are meant to be. They now exclude planning cost from D's measured run; the shared plan's cost is
  reported alongside, so the system-level total is still derivable and must be quoted as such.
- The planner is exercised live, once per block, exactly as in a real run — but plan *variance* no longer sits inside
  the C/D contrast. Variance across repetitions is still visible, because each repetition gets a fresh plan.
- Provider/time drift is spread across configurations instead of aligning with them. It is not eliminated; the
  schedule and seed are published so the exposure is auditable.
- One new verdict reason code exists. Nothing else may emit it: only an explicit operator cancellation.
- The M3 report gains a "Shared plans" section (plan hash, task count, planner cost/latency, rejections per block) and
  the MVP gate audits the pairing: every block must have a **recorded planning outcome**, and where a plan exists both
  the C and the D row must carry that plan's `plan_sha256`. A block the planner could *not* plan is not a gate failure
  — that is an experimental result, and demanding a successful plan per block would push the operator to re-roll the
  planner until it cooperated. It is instead required that C and D both recorded exactly that (`plan_failed`), so
  neither can quietly have executed something of its own.

## Alternatives considered
- **Keep independent planning and label the result.** The reviewer's fallback ("describe it explicitly as an unpaired
  system-level comparison"). Cheaper, but it throws away the one comparison that can isolate parallelism, which is the
  MVP's headline question.
- **Plan under `max_concurrency = 1` and replay in D.** Always installable (rule 8 only loosens), but the planner is
  then told it has no parallelism, so it would decompose for a sequential machine and D would be measured on a plan
  designed against it.
- **Plan inside D and replay in C.** No extra run, but asymmetric: D would carry planning cost and latency that C does
  not, biasing exactly the comparison being fixed.
- **Redefine C as "`max_concurrency = N`, one worker".** Would make even validation identical for C and D, but it
  changes the frozen meaning of C in evaluation.md §3 mid-experiment; the concurrency budget is the knob the table
  names, and rule 8 rejecting an infeasible sequential plan is information, not noise.
- **A new run state for plan-only runs.** Rejected: no new core nouns or states for a harness convenience; `ABORTED`
  plus a reason code is the existing, honest shape.
