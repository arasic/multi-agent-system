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
  started_at            timestamptz
  duration_ms           int
  input_tokens, output_tokens, cache_read_tokens, cache_write_tokens bigint
  cost_usd              numeric
  priced                boolean     -- false = no price configured → cost understated, surfaced by `mas status`
  status                text        -- ok | max_tokens | refusal | error
  stop_reason, error, request_id text
  meta                  jsonb       -- max_tokens, message/tool counts
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
- `RUNNING → REPLANNING`: any task reaches `FAILED` (retries exhausted) or a worker reports `new_work_required`, and replans remain. If no replans remain → `FAILED`.
- `VERIFYING → PASSED`: verifier PASS; integration artifact → `accepted`.
- `VERIFYING → REPLANNING | FAILED`: verifier FAIL.
- Terminal: `PASSED`, `FAILED`, `ABORTED`. A run **always** reaches a terminal state within budget.

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

Per-attempt limits: `max_runtime_s`, `max_tokens`. Exceeding runtime → `TIMEOUT`.

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
8. estimated cost exceeds remaining budget
9. an amendment would exceed `max_replans`, or removes/alters a task that is `COMPLETED`

Rejection → planner retried with the validation errors, up to `max_plan_attempts`; then run `FAILED`.

**Re-plan triggers (only these three in MVP):**

- verifier FAIL after integration
- a task reaches `FAILED` (retries exhausted)
- a worker report includes `new_work_required`

**Amendment semantics:** add tasks; re-open a `FAILED` task with a new goal (as a new task); cancel `PENDING/READY` tasks. Never touch `COMPLETED` tasks or their artifacts. `max_replans = 2` in MVP.

Re-planning is what makes the DAG *dynamic*; without it this is a static DAG executor with LLM-written nodes.

---

## 9. Budgets and termination

Hard limits per run: `max_tokens`, `max_cost_usd`, `max_wallclock_s / deadline_at`, `max_tasks`, `max_attempts_per_task`, `max_replans`, `max_plan_attempts`, `max_concurrency`. Per attempt: `max_runtime_s`, `max_tokens`.

The orchestrator checks budgets on every transition and on a timer. Exceeding any → run `ABORTED:<reason>`, live attempts `CANCELLED`, non-terminal tasks `CANCELLED`. **No run ends without a verdict.**

---

## 10. Context scoping

A worker receives only what `task.context_spec` names — never "the whole run":
- **artifacts:** by default the outputs of its *direct* dependencies; `context_spec.artifacts_from: [keys]` narrows that to the listed tasks (validator rule 10: they must be dependencies, transitively). Only these are assembled into the worktree and listed in `ctx.inputs`.
- **paths:** `context_spec.paths` (globs) is passed as `ctx.paths`; the tool layer (step 10) restricts what the agent may read to those paths.
This is one of the economic claims under test — measure input tokens per attempt.

---

## 11. Model providers, roles and per-call telemetry (step 9)

`ModelProvider.complete(messages, *, max_tokens, tools=None, temperature=None) -> Completion` — `Completion(text, usage: Usage, tool_calls: [ToolCall(id, name, input)], stop_reason ∈ {end_turn, tool_use, max_tokens, refusal, other}, request_id, raw)`; `Usage(model, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, cost_usd, priced)`. Messages and tools are **provider-neutral shapes** (`mas/providers/base.py`: `system|user|assistant(+tool_calls)|tool` messages, `{name, description, input_schema}` tools); each concrete provider translates. Errors are typed: `ProviderRateLimited` / `ProviderUnavailable` (retryable) vs `ProviderRequestError` (not).

**Concrete providers** (`mas/providers/`, the *only* package that may name a vendor): `anthropic` (official SDK; adaptive thinking, optional `effort`, streaming for large outputs, `refusal` surfaced as a stop reason, `temperature` deliberately not forwarded), `openai` (OpenAI-compatible Chat Completions over stdlib HTTP — api.openai.com or any compatible endpoint / in-cluster gateway; retries with backoff on 429/5xx), `fake` (deterministic scripted provider for tests and key-less runs). Selection is by **spec string** in config: `MAS_MODEL_PLANNER / MAS_MODEL_WORKER / MAS_MODEL_REVIEWER = "<provider>:<model>"` (empty = the role has no model; stub agents keep working). Prices are config too: `MAS_MODEL_PRICES` JSON per model id (or id prefix), USD per 1M tokens — see [models.md](models.md).

**Metering.** Agents and the planner never build providers; the runtime hands them a `MeteredProvider` (`TaskContext.model` for workers). It (1) times and prices every call, (2) writes a `model_calls` row *immediately, in its own transaction* (`DbSink`), so cost evidence survives a worker that dies mid-attempt, (3) sums usage into the attempt's settlement (`AgentResult.usage` ← the meter; agents do not self-report), and (4) enforces a **per-attempt call budget** (`MAS_ATTEMPT_MAX_CALLS`, `MAS_ATTEMPT_MAX_TOKENS`, further capped by the run's *remaining* token budget): the call-count limit is strict; token usage is accounted after each response (it is only known then), and further calls are refused with `AttemptBudgetExceeded` once the limit is reached — so an agent loop cannot run away (antipatterns E1/C4), overshoot is bounded to one completed call (itself bounded by its `max_tokens`), and settlement then trips the run budget (`ABORTED`) if the run is out of tokens. Unpriced models are never hidden: `priced=false` rows, `unpriced_calls` in metrics and an explicit `UNPRICED` marker in `mas status`; cost claims in the evaluation must rest on priced usage only.

**Roles** (three, no dynamic routing): **planner** → strong model; **worker** → fast/cheap model; **reviewer/re-planner** → strong (preferably a different family). `mas models` shows the configured roles and pricing status; `mas models --ping [--spec …]` makes one small metered call — the smallest end-to-end proof that a provider works.

**Deployment note.** The hardened Compose workers have **no egress** (§13), so a real provider cannot be reached from inside them by design. Options, in order of preference: a narrow **model gateway** on the backend network (allow-listed models, keys held only there — the `openai` provider's `base_url` points at it), or workers on the host for development. Keys are never baked into images or task metadata.

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
- Later (step 10): the LLM worker needs a model-API egress — give *that* service a proxy/allow-list, not the network; agent tools stay sandboxed to the worktree.

---

## 14. Explicitly not in MVP

Claims/evidence tables, reviewer panels, dynamic model routing, long-term memory, vector DB, swarm messaging, agent spawning, prompt evolution, self-modification, RL, autonomous deployment, Kafka, Kubernetes, any security-domain code.
