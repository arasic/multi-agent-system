# CLAUDE.md — working rules for this repository

This file is for Claude Code (and any contributor) working in this repo. Read it before changing anything.

## What this project is

A **deliberately small multi-agent runtime experiment**: an LLM planner proposes a task DAG, a deterministic validator + orchestrator coordinate disposable workers over Postgres, workers collaborate through immutable artifacts, and a fixed external verifier decides success. It is later meant to become the orchestration layer of an Autonomous SOC — but **cybersecurity is out of scope for the MVP**.

Source of truth, in order:

1. [docs/invariants.md](docs/invariants.md) — rules that must never be broken
2. [docs/architecture.md](docs/architecture.md) — components, schema, state machines
3. [docs/adr/](docs/adr/) — decisions and their rationale
4. [docs/evaluation.md](docs/evaluation.md) — pass criteria and benchmarks
5. [docs/roadmap.md](docs/roadmap.md) — build order
6. [docs/antipatterns.md](docs/antipatterns.md) — known MAS failure modes (MAST + our own history) → countermeasure → status. **When adding a mechanism, say which entry it addresses; when a change would weaken an entry's countermeasure, stop.**

If code and docs disagree, the docs win until an ADR changes them. If you need to deviate, **write an ADR first** (see `docs/adr/README.md`), then change code.

## Hard rules when writing code

These restate the invariants as coding constraints. Violating one is a bug even if tests pass.

- **No LLM calls in `mas/orchestrator/`, `mas/planner/validator.py`, `mas/verifier/`, or `mas/db/`.** Only `mas/planner/planner.py` and worker agents talk to models, and only through `mas/providers/`.
- **State transitions live only in `mas/orchestrator/state_machine.py`.** Nothing else sets `status` on runs, tasks, or attempts. Agents never decide their own task's outcome; they report, the orchestrator transitions.
- **Artifacts are immutable.** No `UPDATE` on artifact content/ref. Only `status` and `superseded_by` change, via the artifact module.
- **The verifier is not a task and not callable by agents.** `acceptance/` is mounted read-only into every worker. The verdict comes only from `mas/verifier/`.
- **Every state change emits an event.** If it isn't in `events`, it didn't happen.
- **Budgets are enforced in the orchestrator, not requested from agents.** Every run must terminate with a verdict inside its budget.
- **Workers only write inside their own worktree.** No shared checkout, no writes to `main`, no network side effects other than the model API.
- **Command tools run only through an `ExecutionBackend`** (`mas/workers/execution.py`). Real workers use `SandboxExecutionBackend` (one hardened container per attempt: exact worktree, no network, read-only rootfs, non-root, limits, removed at close). `LocalExecutionBackend` is test-only and needs `unsafe_ok=True` — never construct it in runtime or CLI code. Without a backend the tool layer offers no command tools (fail closed). Filesystem tools go through the in-process `Jail`; never open a model-supplied path directly.
- **Model names never appear outside `mas/providers/` and config.** Everything else sees `ModelProvider`. Agents and the planner never construct providers: the runtime hands them a **metered** one (`ctx.model`) — telemetry to `model_calls`, prices from `MAS_MODEL_PRICES`, a per-attempt call/token budget (strict call limit; tokens accounted after each response, further calls refused once exhausted — overshoot ≤ one completed call; `AttemptBudgetExceeded` ends the attempt, never a retry loop). Usage is settled from the meter, not self-reported. Unpriced usage is flagged, never hidden. The meter also enforces the attempt's deadline and cancel event on model calls (`AttemptEnded` before the call; `timeout_s` clamped to the remaining runtime; provider retries only through `call_with_retries` inside that budget — never enable SDK-internal retries).
- **No new core nouns.** Run, Task, Attempt, Artifact, Worker, Capability, Tool, Verifier, Policy/Budget, ModelProvider — adding another needs an ADR.
- **The core test suite must run without any API key.** Stub workers (`mas/workers/stub.py`) exist so the whole pipeline is testable LLM-free.

## Do not build (MVP)

Long-term semantic memory, vector DB, swarm/peer messaging, agents spawning arbitrary agents, prompt evolution, self-modifying orchestration, RL, autonomous deployment, reputation systems, more than ~3 model roles, Kafka, Kubernetes, a claims table (see ADR-004), any cybersecurity-specific code.

If a task seems to need one of these, stop and say so — don't build it quietly.

## Conventions

- **Language:** Python ≥ 3.12, type-hinted, `pyproject.toml`-managed. Async where it helps the worker/orchestrator loops; don't force it.
- **Storage:** PostgreSQL is both the blackboard and the coordination mechanism (`SELECT … FOR UPDATE SKIP LOCKED` for claiming, leases + heartbeats for liveness). No queue.
- **Transactions:** connections are **autocommit**; every atomic unit is an explicit `with conn.transaction():` block. Never rely on implicit transactions — a bare `SELECT` on a non-autocommit connection opens one that never commits, and later `transaction()` blocks silently nest as savepoints (writes invisible to other connections — this bit us on day one). State-machine primitives assume the caller holds a transaction.
- **Stale work:** anything that reports on an attempt must tolerate `StaleAttempt` (the attempt was reaped/cancelled). Log and drop; never retry the report.
- **Lock order:** `run → task → attempt → inserts`, always, with `FOR NO KEY UPDATE` (see `state_machine.py` docstring; `lock_run/lock_task/lock_attempt`). Reversing it once caused a real deadlock. Report = one transaction (artifacts + contract + settlement). The heartbeat covers everything up to and including settlement.
- **Stability gate:** `scripts/stress_step6.py` (100 git diamonds / 50 chaos kills / 20 parallel service-style runs). Run it after touching leases, the state machine, the worker loop, or workspaces. Zero deadlocks, zero stale reports, exactly one abandonment per deliberate death, zero leaked worktrees.
- **Runtime:** Docker Compose — `postgres`, `orchestrator` (`--verifier external`, no Docker), `worker` (scaled ×N, `MAS_EXEC_BACKEND=remote`), plus `mas verify --watch` (verifier service) and `mas execute --watch` (execution runner: workers' command requests → per-attempt sandboxes) on the host (Docker). Workers never get docker.sock and never send paths — the runner derives worktrees from ids. `/data` is the host bind mount `./.mas`. The orchestrator loop is a bounded executor with per-run advisory locks; never tick or verify one run from two places.
- **Workspaces:** one bare repo per run (`$MAS_REPO_ROOT/<run>.git`, default `.mas/repos`, git-ignored) and one worktree per attempt (`$MAS_WORKTREE_ROOT`), branch `run/<run>/<task>/<attempt>`, created from base + **input assembly** (dependency `git_commit` outputs merged in; conflicts handed to the agent via `ctx.conflicts`). The **runtime** commits and mints `git_commit` artifacts; agents name file artifacts `path:<relpath>` → rewritten to `<sha>:<relpath>`. Roots are resolved to absolute paths (git runs with `cwd=<bare repo>`; a relative worktree path would land inside it — this bit us). `--workspace none` for pure in-memory stub tests.
- **Tests:** `pytest`. Unit tests for state machine and validator are mandatory. End-to-end tests use stub workers and a real Postgres. **Each pytest process creates its own database `mas_test_<pid>`** (from the server in `MAS_DATABASE_URL`) and drops it at the end — so concurrent test runs, or tests while the compose services are up, never collide. Never point tests at the shared `mas` database.
- **Pools:** every run has a `pool`; workers and orchestrators serve only their pool(s). In-process `mas run` uses `local:<pid>` (its own workers are additionally pinned by `run_id`), the compose services serve `$MAS_POOL` (`default`), `mas submit --pool` targets a pool, `--pool '*'` serves everything. So the containers never touch a local demo run, and vice versa, on the same database.
- **Config:** environment variables + a small typed settings object. Budgets are per-run parameters, never hard-coded.
- **Logging/telemetry:** structured, and everything meaningful also goes to the `events` table.

## Layout

See "Repository layout" in [README.md](README.md). Keep modules where the layout says; don't create parallel structures.

## Commands

Setup (Windows: `.venv/Scripts/…`; POSIX: `.venv/bin/…`):

```
python -m venv .venv && .venv/Scripts/pip install -e ".[dev]"     # add ",llm" for the Anthropic SDK (real providers)
docker compose up -d postgres            # Postgres 16 on localhost:5432 (mas/mas/mas)
.venv/Scripts/mas migrate
```

Local (in-process orchestrator + N stub worker threads — dev/demo):

```
.venv/Scripts/mas run --dag benchmarks/url_shortener/dag.json --workers 3
.venv/Scripts/mas run --dag benchmarks/url_shortener/dag.json --workers 3 --stub-sleep 1.5 --lease-s 2 --chaos-kill-after 2.2
.venv/Scripts/mas run --dag ... --max-concurrency 1              # config C shape
.venv/Scripts/mas status <run_id> [--json]                       # metrics (evaluation.md §4); pending questions if AWAITING_INPUT
.venv/Scripts/mas replay <run_id>                                # event timeline (I-12)
.venv/Scripts/mas run --dag ... --ask "Which DB?;Which Python?"  # ADR-006 demo: planner asks first, run waits (on the clock)
.venv/Scripts/mas answer <run_id> "sqlite; 3.12"                 # ...answer from another terminal; run continues
.venv/Scripts/mas artifacts <run_id>                             # git_commit shas, <sha>:path documents, verification
.venv/Scripts/mas run --dag ... --agent llm --model fake:demo    # LLM worker loop (fake provider: no key; real: anthropic:<model> / openai:<model>)
.venv/Scripts/mas models                                         # configured model roles + pricing status (MAS_MODEL_*, MAS_MODEL_PRICES)
.venv/Scripts/mas models --ping --spec fake:demo                 # one metered test call (use anthropic:<model> / openai:<model> with a key)
git -C .mas/repos/<run_id>.git log --oneline --graph --all       # the run's whole history (one branch per attempt)
.venv/Scripts/mas run --dag ... --workspace none                 # no filesystem (fastest; opaque stub refs)
.venv/Scripts/mas contract acceptance/url_shortener_contract/contract.json   # validate an ADR-007 contract; prints suite digest
```

Distributed (real separate processes):

```
docker compose build
MAS_WORKER_AGENT=llm docker compose up -d --scale worker=3 postgres orchestrator gateway worker   # LLM workers → gateway (fake:builder by default; set MAS_GATEWAY_UPSTREAM=anthropic:<model> + ANTHROPIC_API_KEY for a real model)
.venv/Scripts/mas verify --watch                                 # verifier service on the host: real sandboxed verdicts
.venv/Scripts/mas execute --watch                                # execution-runner service on the host: compose workers' command tools (sandboxes)
.venv/Scripts/mas submit --dag benchmarks/url_shortener/dag.json --wait   # -> PASSED offline (fake:builder) through the whole service path
.venv/Scripts/mas submit --dag benchmarks/url_shortener/dag.json --wait   # → PASSED offline (fake:builder) through the whole service path
.venv/Scripts/mas submit --dag benchmarks/url_shortener/dag.json --wait
docker kill multi-agent-system-worker-2                          # A5 demo: reaper recovers the task
.venv/Scripts/mas verify --once                                  # verify whatever is VERIFYING right now, then exit
```

Tests and lint (no API key needed; DB tests skip if Postgres is unreachable):

```
.venv/Scripts/python -m pytest -q
.venv/Scripts/ruff check mas tests && .venv/Scripts/ruff format mas tests
```

Verifier/sandbox changes additionally require Docker and the five-fixture gate:

```
docker build -f acceptance/Dockerfile.verifier -t mas-verifier:latest .
.venv/Scripts/python -m pytest tests/test_acceptance.py -q
```

## How to make a design change

1. Check `docs/invariants.md` — if the change breaks an invariant, it needs a very good reason.
2. Write `docs/adr/NNN-title.md` (status: Proposed) with context, decision, consequences.
3. Update `docs/architecture.md` / `docs/evaluation.md` to match.
4. Then change code.

## Working style

- Prefer the deterministic solution. Reach for an LLM only where judgement is genuinely required (planning, coding, merging).
- Small, verifiable steps in roadmap order. Don't skip ahead to LLM integration before the substrate is proven with stub workers.
- Report what actually happened: which tests ran, which failed, what was skipped.
- When something in the plan turns out to be wrong in practice, say so and propose the ADR — don't silently work around it.
