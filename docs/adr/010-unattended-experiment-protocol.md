# 010 — Unattended M3 protocol: no answer key, equal totals across the plan boundary, a spend ceiling that stops
Status: Accepted
Date: 2026-08-18
Amends: [ADR-009](009-paired-cd-evidence.md) (§3 planning cost, §6 schedule), [evaluation.md](../evaluation.md) §3

## Context

ADR-009 made C and D execute one shared plan per block. Preparing the first paid run surfaced four consequences of
that change, plus two protocol gaps, that would each have corrupted the evidence or the budget:

1. **The clarification fairness rule became unimplementable.** evaluation.md §3 requires "a fixed, pre-written answer
   key per benchmark … so answers are identical across configs". No answer key exists, and none was ever written.
   After ADR-009, `mas plan` ends the plan-only run the moment the planner asks, so a question loses **both** C and D
   of that block: they are recorded as experimental failures and, being experimental, are never rerun. Docs and code
   disagreed, and the docs were the ones nobody could execute.
2. **C and D were about to receive more budget than A and B.** A/B plan inside their run, so planning is charged
   against their tokens, cost and wall-clock. C/D plan outside the run, and then executed with the *full* per-run
   budget again — quietly breaking the "equal **total** budgets" fairness rule in the direction that flatters MAS.
3. **Nothing bounded the matrix.** 100 execution runs + up to 25 planning rounds at `--max-cost-usd 20` is $2,500
   worst case, with no aggregate ceiling, no projection, no kill switch and no running total.
4. **Nothing stopped a provider incident.** A rate-limit storm or an outage produces infrastructure-classified rows,
   whose cells are rerun by the next invocation — so the harness would keep paying to re-fail.
5. **Reasoning effort was unset**, so the provider's default silently decided cost and behaviour on a run whose
   environment fingerprint claims to freeze exactly that.
6. **The frozen schedule was recorded but never audited against the evidence**, and `git` worktree administration
   proceeded with in-process serialization only where the filesystem could not lock across processes — which is
   precisely where workers are separate processes.

## Decision

1. **M3 is unattended and has no clarification answer key.** The width benchmarks are frozen, fully specified and
   machine-checkable; there is nothing legitimate to ask that the goal and the acceptance suite do not already say. A
   planner that asks instead of planning is recording a real property of the planner: it is an **experimental**
   planning outcome and stays as evidence. evaluation.md §3's answer-key rule is replaced accordingly. Clarification
   remains a first-class, tested mechanism (ADR-006) and is exercised deliberately in `scripts/live_smoke.py`, not in
   the matrix.
2. **Equal totals are enforced across the plan boundary.** C and D execute under what the block's shared plan left of
   the run's total budget (`benchmark.execution_budgets`: tokens, cost and wall-clock, per block). If planning consumed
   the whole budget, the cell is recorded as `ABORTED`/`BUDGET_EXHAUSTED` — an experimental result, never a fresh
   budget.
3. **Both readings of cost and latency are recorded.** Each row carries its execution-only `call_cost_usd`/`machine_s`
   *and* `system_call_cost_usd`/`system_machine_s`, which add the block's planning back. C-versus-D is the
   execution-only contrast (the shared planning component is identical and cancels); A/B-versus-C/D must be quoted on
   the system-level figures. Planning is charged to both C and D there, because either configuration deployed alone
   would have paid for it.
4. **The matrix has an aggregate spend ceiling** (`--max-total-cost-usd`, required for a live matrix). Spend is
   recomputed from the raw append-only logs — superseded infrastructure rows and retried planning rounds included,
   because the provider billed those too. Before every operation the harness requires
   `spent + this operation's ceiling <= cap`, prints billed / remaining / worst-case, and stops when an operation
   reported unknown cost (unless `--allow-unpriced` was given). The ceiling is recorded in `experiment.json` with an
   append-only `spend_cap_history`, and `scripts/mvp_gate.py` fails without it or if the recomputed spend exceeds it.
   The same rule bounds `scripts/live_smoke.py` — the *first* thing that spends money — with a default ceiling of
   three runs; it prints each stage's measured cost and the total, which is what the matrix's ceilings should be
   chosen from, and its resume identity includes the request shape (thinking, effort, timeout, retries), because a
   stage that passed at another effort cost something else. **The ping is in that ledger too**: `cli.ping_spec`
   returns its telemetry as data, the evidence records the reported model id and cost under `ping`, and an unpriced
   ping fails the gate — a ping the price table cannot price means every later cost figure is a floor. Its own USD
   cost cannot be bounded before the call, so it is admitted while any ceiling remains and billed afterwards; the
   ceiling is crossed by at most one small call, never bypassed. `live_smoke.py --step ping` is therefore *the*
   first paid call; `mas models --ping` is a diagnostic that bills separately and is recorded nowhere, so running
   both means paying twice for the same proof.
   **Spend is an append-only ledger, separate from qualification.** `evidence["ledger"]` records every billable
   operation as it happens — the ping, every run, failures, aborts and retries alike — and resume carries it whole,
   while `steps`/`runs` carry only what qualified. A failed $5 planner attempt stays $5 on resume: rebuilding the
   total from the stages that passed would let a retry cross the ceiling unseen. The charge is written *before* the
   stage outcome, so a crash between the two loses the flag, never the money, and the gate audits the ledger
   (`live.attempts_audited`): present, covering every run, and every entry priced.
5. **Pacing and a circuit breaker.** `--pace-s` between operations, `--cooldown-s` after a machinery failure, and a
   stop after `--max-consecutive-infrastructure` (default 3) in a row, with the reason printed. Stopping never
   discards anything: rerunning the same command resumes.
6. **Reasoning effort is an explicit experimental parameter.** A live matrix with any `anthropic:` model refuses to
   start unless `MAS_ANTHROPIC_EFFORT` is set; so does the live smoke, and `mas doctor --require-live` reports it as a
   required check so the omission surfaces before anything is spent. The value is frozen in the environment
   fingerprint and audited by the gate. Choosing it is a protocol decision made before the manifest exists, not an environment tweak.
7. **The schedule is audited against the evidence.** The gate regenerates the schedule from the frozen seed and
   requires the recorded one to match exactly, and requires the rows to show it was followed: every row carries its
   block's `schedule_index`, and first-pass rows (not reruns) appear in schedule order.
8. **Worktree creation fails closed without cross-process locking.** `repo_admin_lock(required=True)` raises where the
   filesystem cannot lock; the in-process mutex alone is not a guarantee when workers are separate processes and
   containers. Cleanup/GC (`required=False`) still degrade, since they run outside the risky window.

## Consequences

- One fewer thing the operator can get wrong with real money: the worst case is a number they chose, printed before
  every operation, and enforced from the raw logs rather than from a summary.
- A/B-versus-D claims must now be quoted from `system_*` columns. Reporting the execution-only figure against A/B
  would understate MAS cost by exactly the shared planning round; `analysis.md` says which is which.
- C/D runs may get a *slightly* smaller execution budget than A/B runs get in total — that is the point — and a
  block whose planning was expensive can legitimately produce a `BUDGET_EXHAUSTED` C/D cell. That is a finding about
  the decomposition's cost, not a harness error.
- The spend ceiling is deliberately **not** part of the experiment's frozen `spec`. Freezing it there would make an
  experiment that reaches its ceiling unresumable, so the only way forward would be to discard evidence already paid
  for. It is instead recorded with an append-only history of every change (when, at which commit, from what to what),
  which is auditable without being a trap.
- The circuit breaker can stop a matrix that would have recovered on its own. That is the intended trade: an operator
  reading "3 consecutive infrastructure failures" loses minutes; an unattended harness walking through an outage loses
  money and fills the log with cells to rerun.
- M3 no longer measures clarification behaviour at all. `questions` and `human_wait_s` stay in the metrics, and a
  planner that asks still shows up in the failure classes — but "how good are its questions" is now explicitly a
  live-smoke question, and answering it properly would need a resumable `mas plan` and equal clarification rights for
  A/B. Both are out of MVP scope.
- On a filesystem without advisory locking, runs now fail instead of racing. That is a real behavioural regression for
  exotic bind mounts, and the error says exactly what to change.
- A human who never answers `mas approve` / `mas answer` now costs one wall-clock budget instead of an unbounded
  process: the smoke's wait drives `scheduler.tick`, so I-4 is enforced by the same code path the orchestrator uses
  and the run ends `ABORTED`/`BUDGET_EXHAUSTED` on its own clock (ADR-006 always said the clock runs; nothing was
  ticking it here).
- Evidence files are never overwritten. `--output` that exists is refused before the preflight — which itself writes
  the file — so an unrelated preflight failure cannot erase a paid result. A separate experiment takes a separate
  path; a continuation takes `--resume`.
- `mas models --probe-tools` exists so the riskiest live path — model → tool call → tool result → **second** call — is
  proven for two small calls instead of being discovered mid-run. Our replay of the assistant turn is regression-tested
  only against SDK-shaped doubles; the probe is what tests it against a real API, and it reports the reported model id
  (compare it with the price table) and whether the turn carried signed reasoning that had to be replayed.

## Alternatives considered

- **Write the answer key.** Honest, and it would keep the fairness rule. But it needs a resumable plan-only run
  (`mas plan` currently ends the run when the planner asks), a policy for equal clarification rights in A/B, and a
  per-benchmark key that is itself a specification the planner could be judged against. Larger than it looks, and it
  would delay the evidence the MVP exists to produce.
- **Charge the shared plan to neither C nor D and simply document it** (ADR-009's original position). Kept for the
  C-versus-D contrast; insufficient against A/B, where it silently hands MAS a free planning round.
- **Split the planning cost in half between C and D.** Arithmetically tidy, but no deployment of C alone pays half a
  planner.
- **A per-hour or per-day spend limit.** Easier to reason about for a human, useless as an admission rule: the harness
  needs to know *before* starting an operation whether it can afford it.
- **Retry through provider incidents with exponential backoff instead of stopping.** Hides the incident in the
  evidence timeline and spends money unattended; the operator should decide when the provider is healthy.
