# Evaluation

Status: frozen for MVP (2026-08-16).

The MVP answers two questions **separately**. Conflating them is how MAS projects fool themselves.

1. **Architecture** — does the runtime do what the design says, reliably?
2. **Value** — for a task of width *N*, when does parallel MAS beat a single agent — on time, cost, or success rate?

Architecture can pass while value is negative at small *N*. That is an acceptable, interesting result.

---

## 1. Architecture pass criteria

All ten must hold, each demonstrated by a repeatable test or scripted demo:

| # | Criterion | How demonstrated |
|---|---|---|
| A1 | Planner produces a valid typed DAG from an unfamiliar goal | LLM planner run on both benchmarks; validator accepts |
| A2 | Validator rejects invalid DAGs | Unit tests: cycle, missing dep, unknown capability, missing contract, no integration sink, over budget |
| A3 | ≥ 3 workers execute concurrently | Event timeline shows ≥ 3 overlapping `RUNNING` attempts |
| A4 | Workers are isolated | Separate processes/containers, per-attempt worktrees, scoped context (tokens-in per attempt recorded) |
| A5 | Worker death recovers automatically | Kill a worker mid-attempt → `ABANDONED` → task re-claimed → run completes |
| A6 | Artifacts stay consistent | Immutability enforced; retries don't inherit partial outputs; supersession chain intact |
| A7 | Conflicts are representable and resolved visibly | Forced-disagreement demo: two competing candidate artifacts → `decision` artifact → loser `superseded`. **Status (2026-08-17):** ✅ `benchmarks/forced_disagreement/dag.json` — ARCH_A/ARCH_B both produce `document:design.md`; IMPL must decide; winner accepted, loser `superseded_by` winner, decision artifact with rationale, `artifact.decided` event; silent choice fails the attempt; forged winners rejected (`tests/test_conflicts.py`, stub and git worktrees; LLM `finish.decisions` unit-tested) |
| A8 | Bounded re-plan works | Induced integration/verifier failure → amendment → validator → completes; `max_replans` respected. **Status (13-lite, offline):** stub verifier FAIL → amendment (rule 9) → PASS with `replans_used = 1`; repeated fingerprint → `NO_PROGRESS`; budget spent → `BUDGET_EXHAUSTED`/`NO_PROGRESS` (`tests/test_repair.py`) |
| A9 | External verifier alone controls PASS | Agents' own tests irrelevant to verdict; `acceptance/` read-only; verifier is not a task |
| A10 | Every run terminates inside budget, fully auditable | Budget-starvation tests reach `ABORTED`; `mas replay <run_id>` reconstructs the run from `events` |

Plus the LLM-free gate: A3–A6, A9, A10 must pass with **stub workers and no API key** before any LLM is wired in. A2 is also LLM-free (validator unit tests) and lands at roadmap step 12.

---

## 2. Benchmarks

Both are software-construction tasks because success is externally decidable.

### 2.1 Smoke test — URL shortener
> Build a working URL-shortener service with persistent storage, HTTP API, tests, Dockerfile and documentation. Decide how to divide the work. Stop only when the acceptance suite passes.

Purpose: prove the whole pipeline end-to-end. **Not** used to claim MAS superiority — it is small and tightly coupled, exactly the shape where multi-agent is expected to struggle.

### 2.2 Width benchmark — N adapters against a fixed interface
> Given a fixed core contract and specs for N adapters, implement all N adapters and integrate them. Stop only when the acceptance suite passes.

```
                    CORE CONTRACT
                         │
     ┌────────┬──────────┼──────────┬────────┐
     ▼        ▼          ▼          ▼        ▼
 Adapter 1 Adapter 2 Adapter 3 Adapter 4 … Adapter N
```

Run at **N = 1, 2, 4, 8, 16**. This is where the value question is answered: where is the crossover?

Fixtures for stub workers exist for both benchmarks so the pipeline is testable without an LLM. The width family is
generated deterministically by `mas.evaluation.width_dag(N)` and verified by immutable `acceptance/adapters_<N>` suites;
`tests/test_benchmark.py` permanently gates N=4 through real Git worktrees and the Docker verifier. The full experiment
is `python scripts/benchmark.py` (JSONL + CSV/JSON + SVG); real evidence requires at least five repetitions per cell.

---

## 3. Configurations

Same task. Same acceptance suite. Same tools. Same budgets. Same runtime code.

| Config | Shape | What it isolates |
|---|---|---|
| **A** | fast/cheap model, single agent | the cheap baseline |
| **B** | strong model, single agent | the strong baseline |
| **C** | strong planner → fast workers, **sequential** (`max_concurrency = 1`) | decomposition without parallelism |
| **D** | strong planner → fast workers, **parallel** (`max_concurrency = N`) | full MAS |

Comparisons: **A vs D** — is MAS worth more than one cheap agent? **B vs D** — can heterogeneous MAS beat fast frontier intelligence? **C vs D** — what does actual parallelism buy?

Through M3, the execution mode is **explicitly configured and frozen**. The planner may record task-shape metadata (estimated width, coupling, shared-output risk and critical path), but that metadata is observational and cannot silently change A/B/C/D. Automatic mode selection is a post-MVP hypothesis evaluated under [ADR-008](adr/008-adaptive-execution-modes.md), not a prerequisite for an honest MAS result.

**Enforcement (2026-08-17):** `mas.evaluation` and the CLI make this table executable. A/B are transformed into one
`solve` task plus system integration and forced to concurrency 1; their deterministic repair policy preserves that shape.
C is forced to concurrency 1; D alone honors N. Model specs stay explicit at the experiment boundary.

### Fairness rules (non-negotiable)

- A/B run **inside the same runtime**: one `solve` task + system integration task, same worktree/tool layer, same read-only acceptance, same verifier stage, failure report fed back on the next cycle.
- Equal **total** budgets: tokens, cost, wall-clock. Equal number of verifier-driven repair cycles (single-agent "re-plan" = next attempt with the failure report).
- C and D are the same code path with one knob. Never two implementations.
- Same acceptance suite version across all configs and all N.
- Each (config, N) cell run ≥ 5 times; report distributions, not single runs.
- Clarifying questions (ADR-006): every config may ask through the same channel; a fixed, pre-written answer key per benchmark is used so answers are identical across configs. Report `questions` and `human_wait_s` separately and compare on `machine_s` — a config is not "faster" because a human answered quickly, nor "slower" because it asked a good question.

---

## 4. Metrics (recorded per run from `attempts` + `events`)

Outcome: verdict · acceptance pass rate · human interventions other than answering questions (must be 0).
Time: total (creation→finish) · machine time (total − human wait) · wall-clock of the execution phase · human wait · critical-path duration · parallelism efficiency (sum of attempt durations / wall-clock).
Questions: batches asked · assumptions recorded (planner proceeded without asking).
Cost: input tokens · output/thinking tokens · cache-read tokens · total USD · tokens-in per attempt (context-scoping claim) · model calls and call latency per attempt. Source of truth: `model_calls` (per-call telemetry, written as each call finishes) reconciled with the settled `attempts.*` columns; a run with any **unpriced** call (`priced=false`, no price in `MAS_MODEL_PRICES`) must be reported as *cost unknown*, never as cheap.
Structure: tasks created · attempts · retries · re-plans · plan-attempts · agent calls · worker utilisation.
Task shape: estimated independent width · dependency density · estimated critical-path ratio · overlapping-output risk · coupling/risk flags · planner-suggested mode versus configured mode. These fields are descriptive through M3 and become selector inputs only after the fixed-mode evidence exists.
Failures: acceptance failures · integration failures · planning/validation failures · abandoned attempts.

Report: for each config, plot **N vs wall-clock**, **N vs cost**, **N vs success rate**; table of A/B/C/D at each N.

---

## 5. Failure-injection protocol (part of every demo)

1. Kill one worker while ≥ 3 attempts are `RUNNING` → assert recovery (A5).
2. Force disagreement: two `architecture` tasks answer the same design question → assert `decision` artifact (A7).
3. Corrupt the integration (or make an adapter violate the contract) → assert verifier FAIL → re-plan → PASS within `max_replans` (A8).
4. Starve each budget in turn → assert `ABORTED:<reason>` (A10).

---

## 6. Time-box

The MAS lab is where the "generic framework" trap can re-enter. Guard: **one task family, one baseline set, one domain, ~6 weeks of build-and-run** from roadmap step 2. If A1–A10 are not demonstrated by then, the finding is that the plan was wrong somewhere — write it up as such rather than extending scope.

---

## 7. Acceptable outcomes

- Architecture passes; MAS loses at N ≤ 2, ties at N ≈ 4, wins at N ≥ 8 → **excellent result**; proceed to the SOC application.
- Architecture passes; MAS never wins on any axis at any N → still a valid result; the runtime is a durable execution engine and the value question needs a different task shape. Do **not** move to the SOC on the strength of architecture alone without understanding this.
- Architecture fails → fix or stop. Do not add models to compensate for substrate failures (MAST).

---

## 8. Post-MVP adaptive-mode evaluation

This section does **not** change the frozen MVP experiment. After M3 has produced fixed-mode A/B/C/D traces, M4 may evaluate the deterministic selector described by ADR-008.

1. Replay the M3 corpus without executing work and ask the selector to choose `single_agent`, `sequential_workflow`, or `parallel_centralized_mas` from goal, acceptance contract, budgets and validated task-shape metadata.
2. Compare its choice with the best observed fixed mode for the declared objective (success first, then machine time and cost). Report selection accuracy, regret, abstention rate and unsupported-task rate — never only aggregate success.
3. Compare against simple policies such as “always strong single agent” and “parallel only when validated width ≥ 4.” A learned or LLM-written selector must beat these before it can control execution.
4. Roll out behind a configuration flag with the chosen mode and reason recorded before work begins. The validator, budgets, tool policy, verifier and human approval gates remain authoritative in every mode.

If the selector cannot demonstrate lower regret than the simple policies, keep mode selection explicit. Adaptive routing is an optimisation, not part of correctness.
