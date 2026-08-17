# Architecture

Status: **frozen for MVP** (2026-08-16). Changes require an ADR.

This document is what you code from. It defines the components, the data model, the three state machines, and the rules for leasing, workspaces, artifacts, integration, verification, planning, re-planning, budgets, and the single-agent baseline.

---

## 1. Overview

```
                          USER GOAL
                              │
                              ▼
                     ┌─────────────────┐
                     │   LLM PLANNER   │  intelligence: what / parallel / depends / capability
                     └────────┬────────┘
                              │ proposed Task DAG (typed JSON)
                              ▼
                     ┌─────────────────┐
                     │  DAG VALIDATOR  │  deterministic: rejects invalid or over-budget plans
                     └────────┬────────┘
                              │ accepted DAG
                              ▼
                     ┌─────────────────┐
                     │  ORCHESTRATOR   │  deterministic: readiness · leases · retries · budgets
                     │ Postgres state  │  state machines · reaper · termination
                     └────────┬────────┘
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
          Worker 1        Worker 2        Worker 3        disposable processes
          worktree        worktree        worktree        scoped context + tools
              │               │               │
              └───────────────┼───────────────┘
                              ▼
                       ARTIFACT STORE      immutable · attempt-versioned · git commit refs
                              │
                              ▼
                     INTEGRATION (task)    required terminal task; merges candidates
                              │
                              ▼
                     EXTERNAL VERIFIER     stage, not a task; fixed acceptance suite
                              │
                     ┌────────┴────────┐
                     ▼                 ▼
                   PASS              FAIL
                     │                 │
                   DONE        retry / bounded re-plan ↺
```

**Planner = intelligence. Orchestrator = correctness.**

Two questions the design must answer separately: *does the runtime work* (architecture) and *does MAS pay off* (value). See [evaluation.md](evaluation.md).

---

## 2. Components

| Component | Kind | Responsibility | Must not |
|---|---|---|---|
| **Planner** | LLM | Turn goal + constraints + available capabilities into a typed Task DAG; produce DAG amendments on re-plan | Execute anything; set any status; see `acceptance/` |
| **Validator** | deterministic | Accept/reject a DAG or amendment against structural, capability, permission and budget rules | Call a model |
| **Orchestrator** | deterministic | Compute readiness, hand out leases, reap dead attempts, apply retries, enforce budgets, run the verifier stage, decide termination | Call a model; interpret task content |
| **Worker runtime** | deterministic loop | Claim a READY task, create an attempt + worktree, assemble scoped context, run the agent, publish artifacts, report | Decide its own task's outcome; touch other worktrees |
| **Agent** | LLM (or stub) | Do the task inside the worktree with the scoped tools/context; return a report | Modify `acceptance/`; call the verifier |
| **Workspace** | deterministic | One git worktree per attempt on branch `run/<run>/<task>/<attempt>` | Share checkouts |
| **Artifact store** | deterministic | Publish immutable artifacts (mostly `git_commit` refs); status transitions; supersession | Mutate content |
| **Integration** | task (worker) | Merge accepted candidate commits into `run/<run>/integration`; resolve merge conflicts | Run the acceptance suite |
| **Verifier** | deterministic | Run the fixed acceptance suite against the integration commit in a clean environment; record verdict | Be a task; be callable by agents |
| **ModelProvider** | interface | `complete()` with usage accounting; concrete providers behind it | Leak model names into architecture code |
| **Event log** | table | Append-only record of every transition and significant action | Be optional |

---

## 3. Data model (PostgreSQL)

Postgres is both the **blackboard** (artifacts, decisions, events) and the **coordination mechanism** (task claiming via `SELECT … FOR UPDATE SKIP LOCKED`, leases). No queue in the MVP (ADR-005).

```sql
runs
  id                    uuid        pk
  goal                  text
  benchmark             text        -- url_shortener | adapters | …
  config                text        -- A | B | C | D  (see evaluation.md)
  status                run_status
  base_ref              text        -- git ref all worktrees start from
  pool                  text        -- workers/orchestrators serve only their pool(s); `mas run` uses local:<pid>,
                                    -- long-running services serve $MAS_POOL (default 'default')
  -- budgets (hard limits, enforced by orchestrator)
  max_concurrency       int         -- 1 for config C, N for D
  max_tasks             int
  max_attempts_per_task int
  max_replans           int
  max_plan_attempts     int         -- planner→validator retries before run FAILED
  max_tokens            bigint
  max_cost_usd          numeric
  max_wallclock_s       int
  max_attempt_runtime_s int         -- per-attempt runtime → TIMEOUT via reaper
  max_attempt_tokens    int         -- per-attempt token allocation: the meter hands each attempt min(this, run remaining);
                                    -- validator rule 8 unit (one funded attempt per open task)
  lease_s               int         -- default lease length for attempts (heartbeat extends)
  max_questions         int         -- clarifying-question batches the planner may ask (ADR-006)
  deadline_at           timestamptz
  -- usage
  tokens_used           bigint      default 0
  cost_used_usd         numeric     default 0
  replans_used          int         default 0
  tasks_created         int         default 0
  questions_asked       int         default 0
  -- outcome
  verdict               text        -- PASS | FAIL:<reason> | ABORTED:<reason>
  verdict_reason        text        -- ADR-008 §6 reason code for non-passing terminal runs (NULL on PASS):
                                    -- BUDGET_EXHAUSTED | NO_PROGRESS | UNSUPPORTED | POLICY_DENIED | INVALID_PLAN | UNRECOVERABLE_FAILURE
  created_at, started_at, finished_at timestamptz

tasks
  id                    uuid        pk
  run_id                uuid        fk runs
  key                   text        -- planner-facing id: T1, T2 … unique per run
  goal                  text
  capability            text        -- architecture | implementation | integration | solve | …
  status                task_status
  input_contract        jsonb       -- what the task may assume exists
  output_contract       jsonb       -- artifact types/paths the attempt must publish
  context_spec          jsonb       -- artifact ids / paths / globs the worker receives (nothing else)
  meta                  jsonb       -- free-form, never load-bearing (e.g. stub-agent script in tests)
  tools                 jsonb       -- allow-list for the agent; validator rule 4 fills/validates from capability→tools
  max_attempts          int         -- ≤ runs.max_attempts_per_task
  created_by            text        -- planner | replan:<n> | system
  created_at, updated_at timestamptz

task_dependencies
  task_id               uuid        fk tasks
  depends_on_task_id    uuid        fk tasks
  pk (task_id, depends_on_task_id)

attempts
  id                    uuid        pk
  task_id               uuid        fk tasks
  attempt_number        int         -- 1..N, unique per task
  status                attempt_status
  worker_id             text
  lease_until           timestamptz -- extended by heartbeat
  workspace_ref         text        -- branch/worktree path
  model                 text        -- provider:model, null for stub
  input_tokens, output_tokens bigint default 0
  cost_usd              numeric     default 0
  token_allocation      bigint      -- reserved at claim: min(run.max_attempt_tokens, worker ceiling, max_tokens - tokens_used
                                    --   - Σ allocations of RUNNING attempts); the meter's budget for the attempt (0008)
  started_at, finished_at timestamptz
  failure_reason        text

artifacts
  id                    uuid        pk
  run_id, task_id, attempt_id       fks
  type                  text        -- git_commit | document | decision | verification | log
  ref                   text        -- commit sha, or path/blob key
  status                artifact_status  -- candidate | accepted | superseded | rejected
  superseded_by         uuid        fk artifacts, null
  meta                  jsonb
  created_at            timestamptz
  -- content immutable: only status / superseded_by may change

events
  id                    bigserial   pk
  run_id                uuid
  task_id, attempt_id   uuid        null
  worker_id             text        null
  type                  text        -- run.created, task.ready, attempt.leased, attempt.abandoned, …
  payload               jsonb
  ts                    timestamptz default now()

model_calls                          -- step 9: one row per model call, written as the call finishes (append-only)
  id                    bigserial   pk
  run_id, task_id, attempt_id uuid  null (planner calls: no attempt; `mas models --ping`: no run)
  role                  text        -- planner | worker | reviewer | ping
  provider, model       text
  seq                   int         -- call index within the attempt / planning round
  settled               boolean     -- attempt-less (planner) rows: charged to runs.tokens_used/cost_used_usd by the driver (0008)
  started_at            timestamptz
  duration_ms           int
  input_tokens, output_tokens, cache_read_tokens, cache_write_tokens bigint
  cost_usd              numeric
  priced                boolean     -- false = no price configured → cost understated, surfaced by `mas status`
  status                text        -- ok | max_tokens | refusal | error | cancelled | deadline | budget (last 3: refused, no call)
  stop_reason, error, request_id text
  meta                  jsonb       -- max_tokens, message/tool counts

exec_requests                        -- step 10: workers' command requests for the execution runner (ids only, bounded command)
  id, run_id, task_id, attempt_id, worker_id, family (shell|python), kind (shell|argv|close), command/argv, timeout_s
  status  pending | leased | done | error | cancelled | abandoned;  runner_id, lease_until, created/started/finished_at
  result jsonb (exit_code, flags, duration_s, output_sha256, output_bytes, error), output (in transit; cleared once consumed)

exec_sessions                        -- one sandbox session per attempt, owned by one live runner (lease); container, image_id
```

Not in MVP: `claims`, `evidence`, `decisions` as tables (ADR-004). Decisions are artifacts of type `decision`.
`attempts.*_tokens/cost_usd` and `runs.tokens_used/cost_used_usd` are the **settlement summary** budgets are enforced on;
`model_calls` is the **evidence** (it survives a worker dying mid-attempt; the two must agree for settled attempts).

---

## 4. State machines

All transitions are performed by the orchestrator (or the worker runtime under orchestrator rules for `RUNNING`). Agents never set a status. Every transition writes an event.

### 4.1 Run

```
CREATED ──► PLANNING ──► RUNNING ──► VERIFYING ──► PASSED
               │  ▲         ▲            │
               │  │         │            ├──► REPLANNING ──► RUNNING   (replans_used < max_replans)
               │  │         │            │        │  ▲
               │  │         │            └──► FAILED                   (no replans left)
               │  │         │                     │  │
               ▼  │         │                     ▼  │
        AWAITING_INPUT ─────┘ (answer → PLANNING, or → REPLANNING if tasks exist)     ADR-006
               │
               └──► FAILED  (planner exceeded max_questions / empty batch / validator rejected max_plan_attempts times)

any non-terminal ──► ABORTED   (budget/deadline exceeded, or operator abort — the clock runs from creation)
```

- `PLANNING → AWAITING_INPUT`: planner returned `Questions` instead of a DAG; `question` artifact + `plan.questions` event; `questions_asked += 1` (bounded by `max_questions`).
- `AWAITING_INPUT → PLANNING | REPLANNING`: `answer` artifact + `plan.answered`; the planner is re-invoked with the full Q&A history.
- `PLANNING → RUNNING`: validator accepted a DAG; tasks inserted.
- `RUNNING → VERIFYING`: the integration task is `COMPLETED`.
- `RUNNING → REPLANNING`: any task reaches `FAILED` (retries exhausted) or a worker reports `new_work_required`, and replans remain. If no replans remain → `FAILED`. *(Full step 13; in 13-lite a task `FAILED` ends the run `UNRECOVERABLE_FAILURE` and `new_work_required` is recorded only.)*
- `VERIFYING → PASSED`: verifier PASS; integration artifact → `accepted`.
- `VERIFYING → REPLANNING | FAILED`: verifier FAIL → **`verify.fingerprint`** event, then the deterministic repair decision (§8c): REPLANNING (`replans_used += 1`, `sm.start_replan`) or FAILED with a reason code. The verifier stage decides without a planner, so the verifier *service* can take this transition; the orchestrator's next tick drives the amendment.
- `REPLANNING → RUNNING`: validator accepted an amendment (rule 9); new tasks inserted; the newest integration task is the run's sink. `REPLANNING → FAILED`: no planner configured (`UNRECOVERABLE_FAILURE`), plan attempts exhausted, or a repeated amendment (`NO_PROGRESS`).
- Terminal: `PASSED`, `FAILED`, `ABORTED`. A run **always** reaches a terminal state within budget, and a non-passing one carries exactly one `verdict_reason` code (ADR-008 §6) besides the human-readable verdict text.

### 4.2 Task

```
PENDING ──► READY ──► RUNNING ──► COMPLETED
   │          ▲          │
   │          │          └──► RETRYABLE ──► READY     (attempts < max_attempts)
   │          │                   │
   │          │                   └──────► FAILED     (attempts exhausted)
   │
   └──► BLOCKED     (an upstream task is FAILED / BLOCKED / CANCELLED)

any non-terminal ──► CANCELLED   (run aborted, or a re-plan amendment removes the task)
```

- `PENDING → READY`: all dependencies `COMPLETED`.
- `READY → RUNNING`: a worker claims it (`FOR UPDATE SKIP LOCKED`) and an attempt is created.
- `RUNNING → COMPLETED`: attempt `SUCCESS` **and** the output contract is satisfied (required artifacts published as `candidate`).
- `RUNNING → RETRYABLE`: attempt `FAILED | TIMEOUT | ABANDONED`, or output contract not satisfied.
- Terminal: `COMPLETED`, `FAILED`, `BLOCKED`, `CANCELLED`.

### 4.3 Attempt

```
RUNNING ──► SUCCESS
   ├──────► FAILED       (agent reported failure, or contract unmet)
   ├──────► TIMEOUT      (exceeded per-attempt max runtime)
   ├──────► ABANDONED    (lease expired — worker presumed dead; set by reaper)
   └──────► CANCELLED    (orchestrator killed it: run aborted / task cancelled)
```

Attempts are never reused. A retry is a new attempt with `attempt_number + 1`.

### 4.4 Artifact status

```
candidate ──► accepted      (chosen by integration / decision, or verifier PASS for integration artifact)
candidate ──► superseded    (a newer artifact for the same slot replaced it; superseded_by set)
candidate ──► rejected      (explicitly rejected in a decision artifact)
```

Content never changes. Artifacts from `FAILED/TIMEOUT/ABANDONED` attempts remain (status `candidate`) and are readable as **hints** by later attempts, but are never treated as authoritative inputs.

**What downstream tasks see.** The *outputs* of a `COMPLETED` task are the `candidate`/`accepted` artifacts of its `SUCCESS` attempt — those are the inputs to dependents (`_dependency_outputs`). `accepted` is a stronger status meaning *chosen for the final result*: set by integration/decision for competing candidates, and by the verifier (PASS) for the integration artifact. So "only accepted artifacts are authoritative" applies to the run's final result and to conflict resolution, not to ordinary upstream→downstream hand-off, where the successful attempt's candidates are the outputs.

---

## 5. Task leasing and failure recovery

**Lock order (every transaction): `run → task → attempt → inserts (artifacts, events)`.** Levels may be skipped, never reversed. Row locks are `FOR NO KEY UPDATE`, which does not conflict with the `FOR KEY SHARE` locks that inserts referencing those rows take — so an orchestrator holding the run row never blocks a worker inserting an event, and no lock cycle can form through foreign keys. Claiming scans unlocked, then per candidate: lock run → check RUNNING + `max_concurrency` → lock task (`SKIP LOCKED`) → claim. The reaper scans unlocked, then per attempt: lock task → lock attempt (`SKIP LOCKED`) → re-check expiry → settle. Reporting is **one transaction**: lock run → task → attempt, verify still `RUNNING` (else `StaleAttempt`, nothing published), publish artifacts, check the output contract, settle. Nothing can be reaped between "artifacts published" and "attempt settled" because they are the same commit.

**The heartbeat runs until the attempt is settled** — through workspace creation, agent execution, git commit, artifact publication and the report commit. A slow publish can never be reaped as ABANDONED. Only a deliberate simulated death stops without reporting.

Worker loop:

```
loop:
    task     = claim_ready_task(my_capabilities)      # SELECT … FOR UPDATE SKIP LOCKED, status READY → RUNNING
    attempt  = create_attempt(task, worker_id, lease_until = now + lease_s)
    ws       = workspace.create(run, task, attempt)   # git worktree from run.base_ref (or integration branch)
    ctx      = context.assemble(task.context_spec)    # only listed accepted artifacts / paths
    start heartbeat(attempt) every lease_s / 3        # extends lease_until
    result   = agent.run(task, ctx, ws, tools)        # bounded by per-attempt runtime + tokens
    arts     = artifacts.publish(ws, task.output_contract)   # commit → artifact rows (candidate)
    report(attempt, result, arts)                     # orchestrator applies transitions
```

Reaper (orchestrator, periodic):

```
for attempt in RUNNING where lease_until < now():
    attempt → ABANDONED
    task    → RETRYABLE  → READY or FAILED per attempt count
    event   attempt.abandoned
```

Killing a worker mid-task is a **standard demo**, not an edge case: lease expires → attempt `ABANDONED` → task back to `READY` → another worker claims it → the run continues. Artifacts published by the dead attempt persist as hints.

Per-attempt limits: `max_attempt_runtime_s`, `max_attempt_tokens` (both run budgets). Exceeding runtime → `TIMEOUT`; the token allocation is what the meter hands the attempt (`min(max_attempt_tokens, tokens the run has left)`, further capped by an optional worker-side ceiling `MAS_ATTEMPT_MAX_TOKENS`).

---

## 6. Workspaces and artifacts

- **One shared bare repository per run** — `repos/<run>.git` on a volume every worker and the orchestrator mount (`MAS_REPO_ROOT`); created idempotently and race-safely on first use with a deterministic base commit on `main` (or fetched from `runs.base_ref`). Multi-host would push/fetch to a remote instead; the MVP is one host / one volume.
- **One git worktree per attempt**: `worktrees/<run>/<task>-<attempt>/`, branch `run/<run>/<task>/<attempt>`, created from base. Then **input assembly**: the dependency tasks' `git_commit` outputs (as narrowed by `context_spec`) are merged in, in order, so the worktree contains what the task depends on. A merge conflict is left in place and handed to the agent as `ctx.conflicts` — resolving it is the agent's job (the integration agent's above all); the stub agent fails such an attempt visibly (`unresolved merge conflicts in [...]`). Nothing from earlier attempts of the *same* task is inherited.
- Workers only write inside their worktree. `acceptance/` is mounted **read-only**; `main` is never touched. Worker containers run **non-root** (uid 1000), with a **read-only root filesystem** (`/data` and `/tmp` writable), **no capabilities**, and on an **internal network with no egress** — they can reach Postgres and nothing else (§13).
- Publishing is done by the **runtime, not the agent**: it stages and commits whatever the agent left in the worktree; if HEAD moved, the commit becomes the `git_commit` artifact (`status=candidate`). Ordinary tasks must move HEAD past their assembled start point; for the integration task the assembled merge itself is the output. Agents name file artifacts `path:<relpath>`; after the commit they are rewritten to `<sha>:<relpath>` (`type=document|decision|…`). No commit → no artifact → the output contract fails → the attempt fails.
- After publishing, the worktree is removed; every attempt's branch and commits stay in the bare repo (audit). Worktrees left by dead workers are pruned on the next attempt.
- On verifier PASS the orchestrator **promotes `run/<run>/integration`** to the accepted integration commit (convenience ref; the artifact row is the truth).
- `mas artifacts <run_id>` lists what a run produced; `git -C repos/<run>.git log --graph --all` shows the whole story.
- Artifacts are **immutable and attempt-versioned**. A better version is a *new* artifact; the old one gets `superseded_by`. See ADR-002.
- Retry semantics: attempt *k+1* starts from a clean worktree; it **may read** attempt *k*'s candidate artifacts (listed in context as hints) but **does not inherit** them. Only `accepted` artifacts are authoritative inputs.
- **Conflict representation (MVP):** two `candidate` artifacts for the same output slot (same task output, or two tasks answering the same question) *are* a conflict. Resolution produces a `decision` artifact naming winner, loser(s), and rationale; losers become `superseded` or `rejected`. Disagreement stays visible in the store. No claims table (ADR-004).

---

## 7. Integration and verification

**Integration is a task; verification is a stage.**

- Every DAG must contain **exactly one** task with `capability = integration` that is the unique sink (all other tasks reach it). The validator enforces this; if the planner omits it, the orchestrator appends `T_integrate` depending on all current sinks (`created_by = system`).
- Integration merges the `accepted`/chosen `git_commit` artifacts of its dependencies into branch `run/<run>/integration` and publishes a `git_commit` artifact (`candidate`). Merge conflicts may require an LLM — that's why it's a task, not orchestrator code.
- **Verifier** (deterministic, `mas/verifier/`): triggered by the orchestrator when the integration task is `COMPLETED`. The orchestrator resolves DB state into a read-only request (run, benchmark, bare repository, exact integration SHA); the verifier receives **no DB connection**. It archives that exact commit, hashes `acceptance/<benchmark>/`, then runs both in an ephemeral Docker container with no network, read-only mounts/rootfs, dropped capabilities, no-new-privileges, and hard wall-clock/CPU/memory/PID/output limits. The strict report must contain every declared check exactly once. Missing/unknown/duplicate checks, bad JSON, absent suite/image/commit, crash, timeout, non-zero exit, or suite mutation all fail closed. The orchestrator alone records the immutable `verification` artifact and `verify.passed|failed` event.
- PASS → run `PASSED`; **only the integration task's SUCCESS attempt's** candidate artifacts become `accepted` (earlier attempts' candidates stay hints). FAIL → `REPLANNING` (with the failure report) or `FAILED`.
- **Re-entrant, never stranded:** any orchestrator that finds a run in `VERIFYING` retries the stage; a Postgres *session* advisory lock (`pg_try_advisory_lock(VERIFY_LOCK_NS, hashtext(run_id))`) ensures one verifier per run at a time. If the process holding the lock dies, Postgres releases it and the next tick — from any orchestrator — completes verification. Budgets still apply while `VERIFYING`.
- The acceptance suite is **written before the run, by humans**, and is never a DAG task and never modifiable by agents. Agents may write their own tests; those are advisory. See ADR-003.
- **Bounded capture:** the sandbox's stdout/stderr are drained through pipes by reader threads that keep at most `max_output_bytes` each and discard the rest; on overflow the container is killed and the verdict is `INVALID`. Nothing is written to the host disk, and `--log-driver none` stops the daemon keeping its own copy. (Review finding: the first version capped on read, not on capture — a flooding suite reached 285 MB on the host.)
- **Trusted adapters (ADR-007 §4a, step 7B):** a suite may be an approved *contract* (`contract.json`) executed by the trusted runner baked into the verifier image (`/opt/mas/adapters/runner.py`, from `mas/verifier/adapters/`). Four typed criterion types only — `build_succeeds`, `tests_required`, `http_status`, `restart_persists` — validated by the same schema module on the host (freeze + verification) and inside the sandbox; unmappable → `INVALID`. The verifier forces the trusted runner command and `expected_checks == contract ids`; per-check timeouts must fit the suite timeout. Service lifecycle for the http checks (start → health → stop → restart, `{state_dir}` survives restarts) is owned by the runner. No generic DSL.
- **Hash pinning (ADR-007):** `VerificationRequest.expected_suite_sha256`, when set, must equal the suite's digest on disk or the result is `INVALID` before the runner is touched. The orchestrator will carry the approved contract's hash once contracts are frozen (7B/11); `AcceptanceVerifier.suite_digest(benchmark)` is what a freeze pins.
- The full report lives in the `verification` artifact; the event log gets a bounded copy (long stdout/stderr truncated).
- **Verifier service (7C).** The hardened Compose orchestrator has no Docker; it runs with `--verifier external` (`DeferredVerification`): when integration completes it moves the run to `VERIFYING` and *leaves it*. A separate `mas verify --watch` process — wherever the sandbox runner (Docker) is available, typically the host — claims `VERIFYING` runs under the existing verify advisory lock, runs the real `AcceptanceVerifier`, publishes the evidence and transitions the run. Bounded (`--parallel`), one connection per job, safe with several instances, re-entrant after a crash (the lock dies with the session; the run stays `VERIFYING`; budgets bound it). The sandbox command is wrapped in a container-side `timeout -s KILL` and started with `--rm`, so an orphaned sandbox ends and is removed even if no verifier is alive. `mas verify --once` verifies whatever is currently `VERIFYING` and exits.

---

## 8. Planning and re-planning

**Planner input:** goal, benchmark constraints, list of registered capabilities and tools, remaining budgets, the Q&A history so far, (on re-plan) current DAG + artifact index + failure report.

**Planner output — typed JSON only, a union of two shapes (ADR-006):** either a **DAG** (below) or a **question batch** `{"questions": ["…"], "context": "…"}` when it needs information before it can plan. Questions move the run to `AWAITING_INPUT`; a human answers with `mas answer`; the planner is called again with the Q&A. Bounded by `max_questions`; the wall-clock keeps running. The single-agent baseline goes through the same driver, so it has the same right to ask.

**DAG shape:**

```json
{
  "tasks": [
    { "id": "T1", "capability": "architecture",   "goal": "Define the adapter interface and shared types",
      "depends_on": [], "output_contract": { "artifacts": ["document:design.md", "git_commit"] },
      "context_spec": { "paths": ["spec/**"] } },
    { "id": "T2", "capability": "implementation", "goal": "Implement adapter A against the interface",
      "depends_on": ["T1"], "output_contract": { "artifacts": ["git_commit"] },
      "context_spec": { "artifacts_from": ["T1"], "paths": ["spec/adapters/a.md"] } },
    { "id": "T3", "capability": "implementation", "goal": "Implement adapter B against the interface",
      "depends_on": ["T1"], "output_contract": { "artifacts": ["git_commit"] },
      "context_spec": { "artifacts_from": ["T1"], "paths": ["spec/adapters/b.md"] } },
    { "id": "T4", "capability": "integration",    "goal": "Merge and reconcile",
      "depends_on": ["T2", "T3"], "output_contract": { "artifacts": ["git_commit"] } }
  ]
}
```

**Validator rejects a plan (or amendment) if:**

1. the graph has a cycle
2. any `depends_on` references a non-existent task
3. any `capability` has no registered worker — checked **after** the integration sink is auto-appended, so a synthesized `T_integrate` is covered too
4. any requested tool is unavailable, not in the capability's allow-list (`mas/planner/capabilities.py`), or prohibited by policy (`FORBIDDEN_TOOLS`); when a task requests none, the capability's default set is filled in and stored on the task — the agent receives it as `ctx.tools`
5. any task lacks an `output_contract`
6. there is not exactly one `integration` sink (orchestrator may auto-append; validator re-checks)
7. task count exceeds `max_tasks` (cumulative across re-plans), or a task's `max_attempts` override lies outside `[1, max_attempts_per_task]` (no bypassing the retry budget)
10. `context_spec.artifacts_from` names a task that is not a (transitive) dependency — a task may not read what it does not depend on
8. **budget allocation** — the plan must fit what the run has left, measured by the driver (`runs.remaining_budget`), never by planner estimates: (a) *tokens:* every open task (this plan's tasks incl. the auto-appended integration sink, plus existing non-terminal tasks on a re-plan) must be fundable for one attempt at the run's per-attempt allocation — `open_tasks × max_attempt_tokens ≤ remaining tokens`, where remaining = `max_tokens − tokens_used − Σ allocations reserved by RUNNING attempts`. This is the allocation the meter actually hands out, so the run can honor it; the rejection names how many tasks would fit. (b) *wall-clock:* there is no per-attempt time allocation (the runtime cap is a timeout). Hard: remaining wall-clock > 0, and the planner's optional per-task `estimate.seconds`, which may only tighten. **Advisory:** this run's shortest successful attempt so far as a per-task floor yields a *warning* (`8-advisory`, on the `plan.validated` event and in the planner's brief as `shortest_attempt_s`), never a rejection — even a true lower bound of past attempts is not a bound on a future repair — weighted critical path ≤ remaining wall-clock, total work / `max_concurrency` ≤ remaining wall-clock, remaining wall-clock > 0. (c) *estimates:* an optional `estimate: {"tokens", "seconds"}` per task is validated; an estimate above `max_attempt_tokens` / `max_attempt_runtime_s` means the task cannot finish within one attempt → rejected ("split it"). (d) *cost:* no price model in the validator (model names never reach it); `max_cost_usd` is enforced at run time; only an already-exhausted cost budget rejects at plan time. Rejections carry the arithmetic so the planner can shrink the plan; hand-written DAG files (`mas run --dag`) get the same check at install.
9. an amendment would exceed `max_replans` (the driver never asks for one then), or removes/alters a task that is `COMPLETED` — implemented as: no new id may collide with *any* recorded task; dependencies / `artifacts_from` may name existing tasks only if `COMPLETED`; a fresh integration sink id; a repeated amendment (same hash) is rejected as no progress (§8c)

Rejection → planner retried with the validation errors, up to `max_plan_attempts`; then run `FAILED`.

**Re-plan triggers (only these three in MVP):**

- verifier FAIL after integration
- a task reaches `FAILED` (retries exhausted)
- a worker report includes `new_work_required`

**Amendment semantics:** add tasks; re-open a `FAILED` task with a new goal (as a new task); cancel `PENDING/READY` tasks. Never touch `COMPLETED` tasks or their artifacts. `max_replans = 2` in MVP (`Budgets` default; the CLI's one-cycle demonstration uses `--max-replans 1`). **`max_replans` is the only repair budget** — there is no separate repair counter.

Re-planning is what makes the DAG *dynamic*; without it this is a static DAG executor with LLM-written nodes.

### 8c. Bounded repair as built (step 13-lite, 2026-08-17)

The verifier-FAIL trigger is implemented end to end (the other two triggers are full step 13):

1. **Failure fingerprint** (`mas/orchestrator/progress.py`, computed by the orchestrator in the verifier stage from system-owned facts only, recorded as a `verify.fingerprint` event with all components): failing acceptance check ids · normalized failure classes (verification status; each failing check's status and detail with hex ids / uuids / paths / durations / numbers folded, so the same *kind* of failure compares equal and a different failure of the same check does not) · integration hash — the verified commit's **tree** hash when a git workspace is available (the diff, not the commit id: a repair that changed nothing observable repeats it), else the opaque ref · a hash of the run's accepted artifacts. Amendments carry their own **amendment hash** (structure + goals with new task ids normalized) on the `plan` artifact.
2. **Decision** (`progress.decide_after_fail`, deterministic, never a model) — taken **only for `FAIL`** (the trusted runner completed and reported failing checks: the code's failure by construction). `TIMEOUT` (the runner itself did not finish — per-check timeouts are check FAILs) and `ERROR` (verifier/sandbox crashed) are infrastructure outcomes: **one bounded retry of the verification itself** (`verify.retry`; the run stays VERIFYING and the next tick re-runs it, inside the run's wall-clock), then a coded terminal verdict `UNRECOVERABLE_FAILURE`; `INVALID` (the suite as frozen/configured could not be executed or validated) is terminal at once — `UNSUPPORTED` (a missing verifier: `UNRECOVERABLE_FAILURE`). None of these is ever a repair trigger or a fingerprint: an amendment cannot fix a sandbox ("verification not completed (<status>): <reason>"). For `FAIL`: fingerprint repeats an earlier cycle's → `FAILED NO_PROGRESS`; `replans_used ≥ max_replans` → `FAILED BUDGET_EXHAUSTED`, or `NO_PROGRESS` when at least one repair ran and the number of failed criteria never went below the first failure's; otherwise → `REPLANNING`. Same failing check ids alone never end the run — a repair can improve the implementation while the same check still fails; only an unchanged fingerprint or an unreduced count within the configured window does.
3. **Amendment protocol** (`runs.plan_run` on a REPLANNING run): the planner receives the recorded tasks with outputs, the bounded failure report, the amendment hashes already tried, and remaining budgets, and must return `kind=dag` with **new tasks only** that may depend on existing COMPLETED tasks (typically the last integration task, so the fix builds on the integrated code) and end in a new integration sink. The validator applies rules 1–8, 10 to the amendment plus **rule 9**: no id may collide with a recorded task (COMPLETED work is never removed or altered), dependencies and `artifacts_from` may name existing tasks only if COMPLETED, an auto-appended sink gets a fresh id; the driver additionally rejects an amendment whose hash repeats an earlier one. Rejections go back as data within `max_plan_attempts`; exhaustion is a verdict (`NO_PROGRESS` when the last rejections were repeats, `INVALID_PLAN` otherwise). The newest integration task is the run's sink for verification and acceptance; the old sink's outputs stay candidates forever.
4. **Verdict reason codes** (ADR-008 §6, `runs.verdict_reason`, migration 0007, set only by `state_machine.fail_run/abort_run`): budget aborts → `BUDGET_EXHAUSTED`; invalid plans/amendments and planner output failures → `INVALID_PLAN` (only-policy rejections → `POLICY_DENIED`; unmappable acceptance criteria → `UNSUPPORTED`); task retries exhausted, no runnable work, verification failed without a planner to repair → `UNRECOVERABLE_FAILURE`; the repair decisions above → `NO_PROGRESS` / `BUDGET_EXHAUSTED`. `mas status`/`mas run` print `reason=`.

Offline demonstration: `mas run --dag benchmarks/url_shortener/dag.json --verifier-fail-times 1 --planner fake --max-replans 1` → FAIL → amendment (`R1`, `R1_integrate`) → PASS with `replans=1`; `--verifier-fail` instead → `NO_PROGRESS`. Tests: `tests/test_repair.py` (FAIL→repair→PASS, repeated tree-hash fingerprint → NO_PROGRESS, exhaustion with/without reduction, `max_replans=0`, repeated amendment through the driver, rule 9 protections, reason codes).

---

## 8b. Planner ↔ driver contract (step 11)

`Planner.plan(PlanRequest) -> DagSpec | Questions | ContractProposal` — **exactly one typed outcome per call**. `PlanRequest` carries the goal, registered capabilities and their tool families, remaining budgets (tasks, questions, tokens, plan attempts, concurrency), the Q&A history, whether the run still needs a contract, the frozen contract once approved, the remaining wall-clock and the previous rejection's errors. The provider-backed `LLMPlanner` (`mas/planner/llm.py`) turns the model's single JSON object into that outcome deterministically (`parse_output`); malformed replies go back to the model as data for a bounded number of parse retries; every call is metered as role `planner` (telemetry, pricing, per-round call/token budget capped by the run's remaining tokens, deadline = remaining wall-clock).

The deterministic driver `runs.plan_run` decides: **questions** → `AWAITING_INPUT` (ADR-006, `mas answer`); **contract** (only for an ad-hoc goal without a frozen contract) → validated against the trusted adapters' schema — unmappable criteria are rejected as data — then `contract_proposal` artifact + `AWAITING_INPUT` for the human's one approval (`mas approve`, optionally an edited contract) → `mas/planner/contracts.py` freezes it: suite directory `acceptance/adhoc-<run>/{contract.json, suite.json}` (trusted runner command, `expected_checks` = the contract's check ids), immutable `acceptance_contract` artifact (`sha256`, `suite_sha256`), `runs.benchmark` set, and the run's verification request pins the digest — the verifier runs exactly what was approved (ADR-007); **DAG** → rejected as data while the goal has no contract; otherwise validated (rules 1–7, 10, shape) and installed with a `plan` artifact (validated DAG + advisory task-shape metadata) and the assumptions artifact. Rejections are returned to the next `plan()` with the errors; when `max_plan_attempts` is spent the run **fails with a verdict**. Benchmark runs (a suite exists) skip the contract step. The planner never writes suite files, freezes anything, changes budgets, sets run state or selects the execution mode; task-shape metadata is recorded for evaluation only (ADR-008; A/B/C/D configuration decides through M3).

## 9. Budgets and termination

Hard limits per run: `max_tokens`, `max_cost_usd`, `max_wallclock_s / deadline_at`, `max_tasks`, `max_attempts_per_task`, `max_replans`, `max_plan_attempts`, `max_concurrency`. Per attempt (also run budgets): `max_attempt_runtime_s`, `max_attempt_tokens`. Budgets must be coherent: rule 8 admits a plan only if `max_tokens` can fund one `max_attempt_tokens` attempt per open task (defaults 2 M / 200 k → up to 10 tasks).

**The token budget is a hard total, held two ways.** (1) *Reservation at claim* (migration 0008): a claim reserves the attempt's allocation from the run's *unreserved* tokens — `min(max_attempt_tokens, worker ceiling, max_tokens − tokens_used − Σ allocations of RUNNING attempts)`, computed under the run lock the claim already holds — and the meter's budget for the attempt *is* that allocation. Reserved tokens are simply the RUNNING attempts' allocations, so nothing is released by hand: settlement moves the attempt out of RUNNING and its real usage into `tokens_used`. Concurrent attempts can therefore never jointly be handed more than what is left; the only overshoot is the meter's one completed call per attempt (usage is known after a response), after which the run aborts. Dead and hung attempts count too: the reaper settles an ABANDONED/TIMEOUT attempt's metered spend (`leases.telemetry_usage`, from its `model_calls` rows) into the attempt and the run — which is why the reaper now takes the run lock first (order run → task → attempt). (2) *Planner rounds are charged*: after every `Planner.plan()` call the driver settles the run's attempt-less `model_calls` rows (`settled = true`, `plan.usage` event) into `tokens_used` / `cost_used_usd` — from telemetry, never from anything the planner reports — and aborts the run with `BUDGET_EXHAUSTED` right there if that crossed a budget (the planner's own call budget is capped by the run's remaining tokens, so this overshoot is also ≤ one call). `mas status` prints `budget used: tokens=used/max (attempts + planner)`.

The orchestrator checks budgets on every transition and on a timer. Exceeding any → run `ABORTED:<reason>`, live attempts `CANCELLED`, non-terminal tasks `CANCELLED`. **No run ends without a verdict.**

---

## 10. Context scoping

A worker receives only what `task.context_spec` names — never "the whole run":
- **artifacts:** by default the outputs of its *direct* dependencies; `context_spec.artifacts_from: [keys]` narrows that to the listed tasks (validator rule 10: they must be dependencies, transitively). Only these are assembled into the worktree and listed in `ctx.inputs`.
- **paths:** `context_spec.paths` (globs) is passed as `ctx.paths`; the tool layer (step 10) restricts what the agent may read to those paths.
This is one of the economic claims under test — measure input tokens per attempt.

---

## 10b. Tool layer and execution boundary (step 10)

An agent acts on the world only through `mas/workers/tools.py`, constructed per attempt from `ctx.tools` (the family allow-list validated by rule 4). Two mechanisms, stated precisely:

- **Filesystem tools are path-jailed in-process** (`Jail`): `read_file` / `write_file` / `list_files` take relative paths only — no absolute paths, no `..`, no symlink escape (resolved and checked against the worktree), `.git/` reserved (the runtime owns commits), `acceptance/` never writable (I-3); reads narrowed further by `context_spec.paths` globs (§10). Size caps on read/write/list.
- **Command tools never run in the worker process.** `run_command` / `run_python` / `run_pytest` go through an `ExecutionBackend` (`mas/workers/execution.py`); without one they are not offered to the model at all (fail closed). `SandboxExecutionBackend` = **one hardened container per attempt**: exactly the attempt's worktree mounted RW at `/work`, nothing else of the host (no shared `/data`), `--network none`, read-only rootfs, tmpfs `/tmp`, non-root, `--cap-drop ALL`, `no-new-privileges`, pids/memory/cpu limits, `--init`, container-side `timeout -s KILL` per command, an outer `max_life_s`, and `docker rm -f` at close — cancel / timeout / output overflow reset the container, so no descendant outlives the attempt. The image is the verifier image (python, pytest, sh, coreutils). `LocalExecutionBackend` is **test-only** (`unsafe_ok=True` required; the runtime and CLI never pass it): bounded — sanitized environment allow-list, output cap that kills the flood, hard timeout clamped to the attempt deadline, cooperative cancel, process-tree termination — but *not confined*.
- `git_status` / `git_diff` are read-only fixed-argv host commands with jailed path arguments. `model` is not a tool (it is `ctx.model`).
- Tool output is data: every result is text handed back as a tool result; denials and failures are error results, not instructions (antipatterns B12).

**The LLM worker loop** (`mas/workers/llm.py`, `--agent llm`) is the only consumer of the above: brief (goal, contract, inputs — all inside explicit `<<DATA …>>` envelopes) → `ctx.model.complete(tools)` → sequential `ToolLayer.dispatch` of the returned tool calls, results back as data → … → the `finish` tool (structured report: success/failure, artifacts as worktree paths, `new_work_required`). Every ending is typed and bounded: `finish` · refusal · malformed calls > N · `max_tokens` truncations > N · "stopped without finish" nudges > N · tool-call budget · turn budget · `AttemptEnded` (budget/deadline/cancel from the meter) · provider error · cancel event before a turn. The runtime creates the execution backend per attempt and closes it in every exit path; the loop never touches state, the verifier or other agents — it reports. Each attempt publishes a bounded **execution trace** artifact (`type=log`, `meta.kind=execution_trace`: turns with stop reason/usage/latency, tool calls with input/output SHA-256 + sizes + status + duration, outcome, model identity, sandbox identity incl. image id/digest — no raw text, no raw output, no reasoning) — the evidence for evaluation and for reproducibility (a mutable image tag alone is not evidence).

**Deployment — the execution-runner path (`mas execute --watch`).** Whoever owns Docker constructs the sandbox: host-side workers (`mas run`, development, `MAS_EXEC_BACKEND=sandbox`) do it directly. Hardened compose workers have no `docker.sock` by design; with `MAS_EXEC_BACKEND=remote` their command tools use `RemoteExecutionBackend` (`mas/workers/exec_remote.py`), which **sends only ids** (run/task/attempt), the tool family and the bounded command as a row in `exec_requests`, and polls for the result. A trusted host-side runner (`mas/workers/exec_runner.py`, next to `mas verify --watch`) claims requests (`SKIP LOCKED` + lease + heartbeat) and — never trusting the worker — checks that the attempt exists and is `RUNNING`, that the family is in the task's persisted allow-list, caps command/argv/timeout sizes and clamps the timeout to the attempt's remaining runtime, **derives the worktree from the ids** (`worktree_root/<run>/<task>-<attempt_number>`), keeps **one sandbox session per attempt** (`exec_sessions`: owned by one live runner; a takeover after the owner's lease expired starts a fresh container with the same name), runs the command in that `SandboxExecutionBackend`, and writes back a bounded result (exit/flags/timing + output SHA-256/size; the capped output text is in transit only and cleared by the worker once consumed). Cancellation flows both ways (worker cancels the request; an attempt leaving `RUNNING` cancels the command and closes the session). A request whose runner died is reaped as **`abandoned`** with a typed error and is **never replayed**; the orphaned container ends by itself (container-side `timeout`, `--rm` + `max_life_s`). Typed outcomes for the worker: TIMED_OUT · CANCELLED · ABANDONED · ERROR (not RUNNING, family not granted, oversize, no runner picked it up). Several runners may run at once; two never execute the same request or own the same attempt. Sandbox identity (container, image id) is recorded on the session and in the attempt's execution trace.

## 11. Model providers, roles and per-call telemetry (step 9)

`ModelProvider.complete(messages, *, max_tokens, tools=None, temperature=None) -> Completion` — `Completion(text, usage: Usage, tool_calls: [ToolCall(id, name, input)], stop_reason ∈ {end_turn, tool_use, max_tokens, refusal, other}, request_id, raw)`; `Usage(model, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, cost_usd, priced)`. Messages and tools are **provider-neutral shapes** (`mas/providers/base.py`: `system|user|assistant(+tool_calls)|tool` messages, `{name, description, input_schema}` tools); each concrete provider translates. Errors are typed: `ProviderRateLimited` / `ProviderUnavailable` (retryable) vs `ProviderRequestError` (not).

**Concrete providers** (`mas/providers/`, the *only* package that may name a vendor): `anthropic` (official SDK; adaptive thinking, optional `effort`, streaming for large outputs, `refusal` surfaced as a stop reason, `temperature` deliberately not forwarded), `openai` (OpenAI-compatible Chat Completions over stdlib HTTP — api.openai.com or any compatible endpoint / in-cluster gateway; retries with backoff on 429/5xx), `fake` (deterministic scripted provider for tests and key-less runs). Selection is by **spec string** in config: `MAS_MODEL_PLANNER / MAS_MODEL_WORKER / MAS_MODEL_REVIEWER = "<provider>:<model>"` (empty = the role has no model; stub agents keep working). Prices are config too: `MAS_MODEL_PRICES` JSON per model id (or id prefix), USD per 1M tokens — see [models.md](models.md).

**Metering.** Agents and the planner never build providers; the runtime hands them a `MeteredProvider` (`TaskContext.model` for workers). It (1) times and prices every call, (2) writes a `model_calls` row *immediately, in its own transaction* (`DbSink`), so cost evidence survives a worker that dies mid-attempt, (3) sums usage into the attempt's settlement (`AgentResult.usage` ← the meter; agents do not self-report), and (4) enforces a **per-attempt call budget** (`MAS_ATTEMPT_MAX_CALLS` calls; tokens = the run's `max_attempt_tokens` allocation, capped by the run's *remaining* token budget and by an optional worker-side ceiling `MAS_ATTEMPT_MAX_TOKENS`): the call-count limit is strict; token usage is accounted after each response (it is only known then), and further calls are refused with `AttemptBudgetExceeded` once the limit is reached — so an agent loop cannot run away (antipatterns E1/C4), overshoot is bounded to one completed call (itself bounded by its `max_tokens`), and settlement then trips the run budget (`ABORTED`) if the run is out of tokens. Unpriced models are never hidden: `priced=false` rows, `unpriced_calls` in metrics and an explicit `UNPRICED` marker in `mas status`; cost claims in the evaluation must rest on priced usage only.

**Deadline and cancellation.** The meter also owns the attempt's clock for model calls: before reserving a call it refuses — `AttemptCancelled` when the runtime's cancel event is set (attempt reaped/cancelled), `AttemptDeadlineExceeded` when less than a minimum viable window (`MIN_CALL_S`) remains before the attempt's runtime deadline (`max_attempt_runtime_s` from claim), `AttemptBudgetExceeded` as above; all are `AttemptEnded`, never retryable. Otherwise the provider receives `timeout_s = min(caller's, per-call cap `MAS_PROVIDER_TIMEOUT_S`, remaining runtime)`, and that budget covers the *whole* call: SDK retries are disabled and every provider retries through `call_with_retries`, which recomputes the remaining time before every request and every backoff / Retry-After sleep and fails fast when a retry cannot fit — so no request or retry outlives the attempt. Refusals are recorded in `model_calls` too (status `cancelled | deadline | budget`, zero tokens); timeouts and other failures are `error` rows with `priced=false`, because what the provider billed for a failed request is unknowable here.

**Roles** (three, no dynamic routing): **planner** → strong model; **worker** → fast/cheap model; **reviewer/re-planner** → strong (preferably a different family). `mas models` shows the configured roles and pricing status; `mas models --ping [--spec …]` makes one small metered call — the smallest end-to-end proof that a provider works.

**Deployment — the model gateway (`mas gateway`, `mas/providers/gateway.py`).** The hardened Compose workers have **no egress** (§13). The one process with a vendor key is the **gateway** service, attached to both the internal `backend` network (workers reach it) and the egress network (it reaches the vendor): it speaks the OpenAI Chat Completions wire (workers use the ordinary `openai:` provider with `MAS_OPENAI_BASE_URL=http://gateway:8080/v1`), requires a bearer token, allow-lists the model names clients may ask for, bounds body size / `max_tokens` / time, translates to the provider-neutral shape and forwards to one upstream `ModelProvider` (`MAS_GATEWAY_UPSTREAM=<provider>:<model>`, e.g. `anthropic:<model>`, or `fake:builder` for the offline demo). It persists nothing; the worker's own meter records and prices usage from the response's real upstream model id. No streaming. Keys are never baked into images, YAML or task metadata (`ANTHROPIC_API_KEY` reaches only the gateway container, from the operator's shell).

---

## 11b. Orchestrator service concurrency (7C)

`orchestrate_forever` is a **bounded executor** (`--parallel N`): every tick runs on its own connection under a **per-run session advisory lock** (`TICK_LOCK_NS`), so two orchestrator processes never tick the same run at once, and an in-flight set stops two local threads from doing so. The verifier is invoked outside any row lock (the tick's transaction ends before verification; only the verify advisory lock is held). A slow verification of run A therefore never blocks run B. `verify_forever` uses the same loop for the verifier service.

## 12. Concurrency knob and the single-agent baseline

- `runs.max_concurrency` bounds simultaneously `RUNNING` attempts. **Config C (sequential MAS) is config D with `max_concurrency = 1`** — same code path, one knob.
- **Single-agent configs (A, B)** run through the *same runtime*: a DAG of one task (`capability = solve`, the whole goal) plus the system-appended integration task; same worktree, tools, and read-only acceptance; same verifier stage; failure report fed back on re-plan. Equal total budgets and equal number of verifier-driven repair cycles. See [evaluation.md](evaluation.md).

---

## 13. Deployment (MVP)

`docker-compose.yml`: `postgres`, `orchestrator`, `worker` (scale ×N). Workers poll Postgres; no queue. Verifier: ephemeral container started by the orchestrator (or isolated subprocess early on).

- **`/data` is a host bind mount** (`${MAS_DATA_DIR:-./.mas}`) holding `repos/` (bare repo per run) and `worktrees/` — mounted by workers and the orchestrator, and readable by the host-side verifier service (which needs the exact integration commits and promotes `run/<run>/integration`). One host by design; a multi-host deployment would push/fetch or containerise the verifier with an API-driven (volume / `docker cp`) transfer instead of bind mounts.
- **Networks:** `backend` is `internal: true` — workers and orchestrator have **no egress**; Postgres also joins `frontend` so the host reaches the published port.
- **Worker/orchestrator containers:** `USER mas` (uid 1000), `read_only: true` rootfs, `tmpfs /tmp`, `cap_drop: [ALL]`, `no-new-privileges`, `./acceptance:/app/acceptance:ro`. Verified: cannot write `/app`, cannot reach the internet, can reach Postgres and `/data`.
- Service-mode model connectivity is still an M2 step-10 gate. The preferred deployment is a narrow configured gateway with allow-listed models and keys held there, rather than general worker egress; host-side development may call a configured provider directly. Agent command tools stay in the networkless execution sandbox in either case.

---

## 14. Explicitly not in MVP

Claims/evidence tables, reviewer panels, automatic execution-mode selection, workflow/profile libraries, generic plan-staleness repair, dynamic model routing, long-term memory, vector DB, swarm messaging, agent spawning, prompt evolution, self-modification, RL, autonomous deployment, Kafka, Kubernetes, any security-domain code.

---

## 15. Post-MVP direction: controlled adaptive execution

This section is **non-normative for M0–M3**. [ADR-008](adr/008-adaptive-execution-modes.md) governs the future work and preserves the existing deterministic substrate.

```text
goal + approved acceptance contract + budgets
                    │
                    ▼
          support and risk classification
                    │
                    ▼
       validated task-shape description
        width · coupling · critical path
          output overlap · uncertainty
                    │
                    ▼
        deterministic mode policy
          ├─ single agent
          ├─ sequential workflow
          └─ parallel centralized MAS
                    │
                    ▼
   existing validator → orchestrator → verifier
```

The progression is deliberately evidence-gated:

1. **M3 first:** run explicit A/B/C/D configurations. Planner-produced task-shape fields are recorded but cannot select the mode.
2. **Workflow templates:** add versioned phase/role/output-contract templates and deterministic phase-exit checks for repeated task families. A template constrains a plan; it is not one fixed DAG for every request. The planner may fill slots or generate a dynamic DAG when no supported template fits. Intermediate checks can block or repair a phase but cannot declare final success.
3. **Deterministic selector:** choose among the three execution modes using inspectable thresholds and reason codes. The choice is stored before execution and evaluated against the fixed-mode corpus.
4. **Application profiles:** expand acceptance adapters, tool policies, sandbox images and workflow templates one profile at a time (API, CLI, web UI, API+database, full stack, then multi-service). “Any app” means an extensible set of explicitly supported profiles, not unbounded autonomy.

The selector never bypasses policy, budgets, DAG validation, external verification or required human approval. Agents may recommend termination, but deterministic code emits terminal reason codes such as `BUDGET_EXHAUSTED`, `NO_PROGRESS`, `UNSUPPORTED`, `POLICY_DENIED`, `INVALID_PLAN` and `UNRECOVERABLE_FAILURE`. `NO_PROGRESS` is based on deterministic evidence fingerprints across bounded repair cycles, not an LLM's opinion. New database run states are added only if implementation proves reason codes insufficient.

Claims/evidence graphs and generic plan-staleness detection remain deferred until a domain or benchmark demonstrates that they are necessary.
