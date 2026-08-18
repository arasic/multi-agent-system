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
| A8 | Bounded re-plan works | Induced integration/verifier failure → amendment → validator → completes; `max_replans` respected. **Status:** verifier FAIL, task-FAILED and `new_work_required` triggers are implemented; offline FAIL → amendment (rule 9) → PASS with `replans_used = 1`; repeated fingerprint → `NO_PROGRESS`; budget spent → `BUDGET_EXHAUSTED`/`NO_PROGRESS` (`tests/test_repair.py`) |
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
is `python scripts/benchmark.py` (immutable experiment manifest + append-only JSONL + CSV/JSON + SVG + completion record +
generated report); real evidence requires at least five repetitions per cell. Every run is classified deterministically
(`benchmark.classify_run`): `experimental` failures — the model, the plan or the run's budgets decided the outcome — are
evidence and stay; `infrastructure` failures — verifier crash/timeout, unusable suite, provider outage, sandbox/workspace
failure, worker death, a client that lost the run — cannot answer the value question, stay in the log for audit, and
their cell is rerun
(the last row per cell counts, and only if every earlier row was infrastructure-invalid). `python scripts/mvp_gate.py`
audits the *raw* rows: it recomputes completion, regenerates the report and requires the summaries on disk to match, and
requires the evidence commit to be the clean, currently checked-out one. It also audits the experimental design
(ADR-009): a frozen randomized block schedule covering every cell, a frozen environment fingerprint, and one recorded
plan per block that C and D both executed — plus (ADR-010) that the schedule is exactly what its seed draws and the
rows show it was followed, that an aggregate spend ceiling was recorded and the recomputed spend stayed inside it, and
that reasoning effort was an explicit frozen choice. It rejects incomplete, mixed-revision, dirty,
infrastructure-invalid, unpriced or unpaired evidence and does not require MAS to win—the measured result is the point
of M3.

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

**Paired C/D (ADR-009, 2026-08-17):** C and D of one (N, repetition) **execute the same validated plan**. The harness
produces it once per block with `mas plan` (a real, metered planning round, under the parallel budget
`max_concurrency = N`), records the DAG and its SHA-256 in `plans.jsonl`, and replays it in both configurations
(`mas run --dag`). Before this, each run planned for itself and the planner was even told a different concurrency
budget, so a C-versus-D difference measured *a different plan plus different concurrency*. Consequences to quote with
the result: (1) planning cost/latency is measured once per block and sits in **neither** C nor D — a system-level
comparison against A/B must add it back; (2) C installs the shared plan under its own frozen budget, so a
decomposition that cannot fit sequentially is rejected by validator rule 8 and recorded as an experimental `C` failure
— that is a finding about C, not a harness error; (3) A-versus-D and B-versus-D remain unpaired system-level
comparisons, as intended.

**Execution order (ADR-009):** cells run on a deterministic randomized **block schedule** — the order of the
(N, repetition) blocks and of A/B/C/D inside each block is drawn from a seed frozen with the experiment, so provider
load, rate limits, cache warmth and time of day cannot line up with a configuration. The seed and the exact schedule
are recorded in `experiment.json` and replayed verbatim on resume; `analysis.md` quotes the seed.

### Fairness rules (non-negotiable)

- A/B run **inside the same runtime**: one `solve` task + system integration task, same worktree/tool layer, same read-only acceptance, same verifier stage, failure report fed back on the next cycle.
- Equal **total** budgets: tokens, cost, wall-clock. Equal number of verifier-driven repair cycles (single-agent "re-plan" = next attempt with the failure report).
  A and B plan inside their run, so their planning is already charged against that budget; C and D plan once per block,
  outside the run, and therefore execute under what the shared plan **left** of the same total (ADR-010). A block whose
  planning consumed the whole budget records `ABORTED`/`BUDGET_EXHAUSTED` for C and D — a finding about the
  decomposition's cost, never a reason to hand execution a fresh budget.
- C and D are the same code path with one knob, running **one and the same validated plan** per (N, repetition)
  (ADR-009). Never two implementations, and never two plans.
- Same acceptance suite version across all configs and all N.
- The experiment's identity is frozen in `experiment.json` and enforced on resume: models, prices, budgets, suite
  hashes, the schedule seed **and the environment** (Python/platform, provider timeout/retries and Anthropic request
  shape, per-attempt call budget, sandbox/verifier image ids and limits). A changed environment is a new experiment.
- Each (config, N) cell run ≥ 5 times; report distributions, not single runs.
- The reasoning effort of every model role is chosen **before** the manifest exists and frozen with it (ADR-010); a
  live matrix using an Anthropic model refuses to start with `MAS_ANTHROPIC_EFFORT` unset, because the provider default
  would otherwise decide cost and behaviour silently.
- **M3 is unattended and has no clarification answer key** (ADR-010, replacing the earlier answer-key rule). The width
  benchmarks are frozen, fully specified and machine-checkable; a planner that asks a question instead of planning is
  recording a real property of the planner, so it is an **experimental** planning outcome and stays as evidence.
  `questions` and `human_wait_s` are still reported, and comparisons are still made on `machine_s`. Clarification
  behaviour itself (ADR-006) is exercised deliberately in `scripts/live_smoke.py`, not measured here — doing that
  fairly would need a resumable plan-only run and equal clarification rights for A/B, both post-MVP.
- **The matrix is financially bounded and stoppable** (ADR-010): `--max-total-cost-usd` is required for a live matrix
  and is enforced *before* each operation from spend recomputed out of the raw append-only logs (superseded and retried
  operations included); unknown cost stops it; `--pace-s`/`--cooldown-s` pace it; and
  `--max-consecutive-infrastructure` stops it during a provider incident. Stopping never discards evidence — the same
  command resumes.

---

## 4. Metrics (recorded per run from `attempts` + `events`)

Outcome: verdict · acceptance pass rate · human interventions other than answering questions (must be 0).
Time: total (creation→finish) · machine time (total − human wait) · wall-clock of the execution phase · human wait · critical-path duration · parallelism efficiency (sum of attempt durations / wall-clock).
Questions: batches asked · assumptions recorded (planner proceeded without asking).
Cost: input tokens · output/thinking tokens · cache-read tokens · total USD · tokens-in per attempt (context-scoping claim) · model calls and call latency per attempt. Two readings for C/D (ADR-010): the run's own `call_cost_usd`/`machine_s` are **execution-only** (the right C-versus-D contrast, since both replay the same plan) and `system_call_cost_usd`/`system_machine_s` add the block's shared planning back — the figures to quote against A/B, whose planning is inside their run. Source of truth: `model_calls` (per-call telemetry, written as each call finishes) reconciled with the settled `attempts.*` columns; a run with any **unpriced** call (`priced=false`, no price in `MAS_MODEL_PRICES`) must be reported as *cost unknown*, never as cheap.
Structure: tasks created · attempts · retries · re-plans · plan-attempts · agent calls · worker utilisation.
Task shape: estimated independent width · dependency density · estimated critical-path ratio · overlapping-output risk · coupling/risk flags · planner-suggested mode versus configured mode. These fields are descriptive through M3 and become selector inputs only after the fixed-mode evidence exists.
Failures: acceptance failures · integration failures · planning/validation failures · abandoned attempts.

Where they live: `mas.metrics.compute` records the per-run values (`critical_path_s` = the longest dependency chain of
task work, all attempts of a task included; `worker_utilisation`; `plan_rejections`; `assumptions`; `verifier_fails` /
`verifier_incomplete`; `attempt_failure_classes` from `classify_attempt_failure`; the planner's advisory `task_shape`).
`scripts/benchmark.py` reports each of them per (config, N) cell as a distribution — min / p25 / median / p75 / max /
mean over the cell's evidence runs — plus failure-class, verdict-reason and planner-suggested-mode histograms.

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
