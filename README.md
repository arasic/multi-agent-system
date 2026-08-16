# multi-agent-system

> A small distributed multi-agent runtime in which an LLM planner decomposes a goal into a task DAG, a deterministic validator and orchestrator coordinate disposable, isolated workers over Postgres, workers collaborate through immutable versioned artifacts, and a **fixed external verifier — not agent consensus — determines success.**

## MVP objective

Build a small distributed multi-agent runtime that can take a high-level software-engineering goal, dynamically decompose it into bounded tasks, execute independent tasks concurrently using isolated workers, persist all work as immutable artifacts, recover from worker failure, re-plan within limits, and finish only when a fixed external verifier passes.

And **compare it fairly against an equivalent single-agent system using the same tools, verifier and retry/budget rules.**

The MVP answers two questions, kept deliberately separate (see [docs/evaluation.md](docs/evaluation.md)):

1. **Architecture** — does the runtime reliably do the ten things listed in the pass criteria?
2. **Value** — for a task of width *N*, at what point does parallel MAS beat a single agent on time, cost, or success rate?

We do not need MAS to win everywhere. We need to know *where* it wins.

## What it will do — and won't (honest scope)

The product promise this architecture can justify:

> **Give it a bounded software goal with an approved, machine-checkable definition of done, and it can decompose, build, integrate, repair and verify the result using parallel workers — producing a verified repository.**

| Requested result | Expected capability after M2 |
|---|---|
| CLI, API, small web application | Yes (a single strong agent may be as fast; the gain is reliability + audit) |
| API + database + auth + UI + tests | Yes — likely the sweet spot: real width behind contracts |
| Multi-service codebase / internal platform | Possible, with integration risk; the N-sweep shows where it stops paying |
| Deploy and operate a production platform | **No.** Tool policy forbids deploy/network side effects; that needs secrets, migrations, observability, rollback and approval gates — none exist here |

For ad-hoc goals the definition of done is an **approved, frozen acceptance contract** whose executable checks come from trusted adapters, templates or a human-owned suite — never from the planner ([ADR-007](docs/adr/007-acceptance-contract-freeze.md)).

## What this is not (yet)

Not an Autonomous SOC. Not an autonomous company. Not an agent framework or marketplace. Not self-modifying. Not a swarm. No dozens of specialised agents, no long-term semantic memory, no vector DB, no RL, no autonomous deployment, no Kafka, no Kubernetes.

The MAS runtime built here is intended to become the reasoning/orchestration layer of an **Autonomous SOC** later (see [Later](#later)). Cybersecurity assumptions are deliberately kept *out* of the runtime until the runtime is proven.

## Architecture on one screen

```
                          USER GOAL
                              │
                              ▼
                     ┌─────────────────┐
                     │   LLM PLANNER   │  intelligence
                     └────────┬────────┘
                              │ proposed Task DAG (JSON)
                              ▼
                     ┌─────────────────┐
                     │  DAG VALIDATOR  │  deterministic
                     └────────┬────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │  ORCHESTRATOR   │  deterministic
                     │ Postgres state  │  leases · budgets · state machines
                     └────────┬────────┘
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
          Worker 1        Worker 2        Worker 3       disposable
          worktree        worktree        worktree       isolated
              │               │               │
              └───────────────┼───────────────┘
                              ▼
                       ARTIFACT STORE      immutable · attempt-versioned
                              │
                              ▼
                     INTEGRATION (task)    merges candidate artifacts
                              │
                              ▼
                     EXTERNAL VERIFIER     fixed acceptance suite · never a task
                              │
                     ┌────────┴────────┐
                     ▼                 ▼
                   PASS              FAIL
                     │                 │
                   DONE        retry / bounded re-plan ↺
```

**Planner = intelligence. Orchestrator = correctness.** The LLM proposes; deterministic code decides. Full detail in [docs/architecture.md](docs/architecture.md); the rules that must never be broken are in [docs/invariants.md](docs/invariants.md).

## Core concepts (the only nouns in the MVP)

| Noun | Meaning |
|---|---|
| **Run** | One execution of one goal under one budget. Owns the DAG, the verdict, and the audit trail. |
| **Task** | A bounded unit of work with a capability, dependencies, and an output contract. State transitions are deterministic. |
| **Attempt** | One try at a task by one worker under one lease. A task has 1..N attempts. |
| **Artifact** | An immutable, attempt-versioned output (typically a git commit ref). Superseded, never mutated. |
| **Worker** | A disposable process that claims READY tasks, runs an agent in an isolated worktree, publishes artifacts. |
| **Capability** | What a task needs / a worker offers (`architecture`, `implementation`, `integration`, …). |
| **Tool** | Something an agent may call (filesystem, python, git, model). Scoped per task. |
| **Verifier** | Deterministic code that runs the fixed acceptance suite on the integration artifact. Alone decides PASS/FAIL. |
| **Policy / Budget** | Hard limits: tokens, cost, tasks, attempts, re-plans, deadline, concurrency. Enforced by the orchestrator. |
| **ModelProvider** | Abstract LLM interface. Model names never appear in architecture code. |

## Repository layout (target)

```
multi-agent-system/
├── README.md
├── CLAUDE.md                     # working rules for Claude Code / contributors
├── pyproject.toml
├── docker-compose.yml            # postgres · orchestrator · worker ×N
├── docs/
│   ├── architecture.md           # components, schema, state machines
│   ├── invariants.md             # rules that must never be broken
│   ├── evaluation.md             # pass criteria, benchmarks, configs A–D, metrics
│   ├── roadmap.md                # build order, milestones, time-box
│   ├── models.md                 # dated model roles/prices (never load-bearing)
│   └── adr/                      # architecture decision records
├── mas/
│   ├── cli.py                    # mas migrate | run | submit | orchestrate | worker | status | answer | artifacts | replay
│   ├── metrics.py                # wall-clock, max concurrency, parallelism efficiency, tokens/cost
│   ├── models/                   # enums + row dataclasses: Run, Task, Attempt, Artifact, Event
│   ├── db/                       # connection, migrations (SQL), events
│   ├── orchestrator/             # state_machine (ONLY place statuses change), leases, budgets, scheduler, runs
│   ├── planner/                  # dag spec + deterministic validator; LLM planner (step 11)
│   ├── workers/                  # runtime loop, stub agent, workspace (bare repo per run, worktree per attempt)
│   ├── artifacts/                # publish / accept / supersede / reject
│   ├── verifier/                 # Verifier protocol, StubVerifier; acceptance runner (step 7)
│   └── providers/                # ModelProvider interface; concrete providers (step 9)
├── benchmarks/
│   ├── url_shortener/            # smoke test: spec + fixtures for stub workers
│   └── adapters/                 # width benchmark: N adapters vs fixed interface
├── acceptance/                   # fixed external suites, mounted read-only into runs
└── tests/
```

## Build order

Deterministic substrate first, LLMs last. Full checklist with milestones in [docs/roadmap.md](docs/roadmap.md).

1. Freeze docs (this) → 2. Postgres schema → 3. Orchestrator on a hand-written DAG → 4. Leases, heartbeat, retry, kill workers → 5. Docker workers ×3–4 → 6. Git worktrees + immutable artifacts → 7. Fixed acceptance verifier → 8. End-to-end run with **stub workers, no LLM** → 9. `ModelProvider` → 10. LLM worker → 11. LLM planner (typed JSON DAG) → 12. DAG validator → 13. Bounded re-planning → 14. Fair single-agent harness → 15. N = 1/2/4/8/16 experiments.

## Documents

- [docs/architecture.md](docs/architecture.md) — how it works
- [docs/invariants.md](docs/invariants.md) — what must never change
- [docs/evaluation.md](docs/evaluation.md) — how we know it worked
- [docs/antipatterns.md](docs/antipatterns.md) — how MAS projects fail, and what defends against each here
- [docs/roadmap.md](docs/roadmap.md) — what we do next
- [docs/adr/](docs/adr/) — why we decided what we decided
- [docs/models.md](docs/models.md) — which models play which role (dated, non-load-bearing)

Design changes go through an ADR. Nothing about the design lives only in chat.

## Quickstart

```
python -m venv .venv && .venv/Scripts/pip install -e ".[dev]"     # POSIX: .venv/bin/pip
docker compose up -d postgres
.venv/Scripts/mas migrate
.venv/Scripts/mas run --dag benchmarks/url_shortener/dag.json --workers 3 --stub-verifier
.venv/Scripts/mas replay <run_id>
docker build -f acceptance/Dockerfile.verifier -t mas-verifier:latest .
.venv/Scripts/python -m pytest -q                                   # 89 tests, no API key; uses its own temp DB
```

Runs are tagged with a **pool**: `mas run` uses a private `local:<pid>` pool, the compose services serve `default`, so they never take each other's work even on the same database. Tests are isolated too — each pytest process creates and drops its own `mas_test_<pid>` database, so concurrent test runs can't collide.

Distributed (orchestrator + 3 worker containers, then kill one mid-run):

```
docker compose build orchestrator && docker compose up -d --scale worker=3 orchestrator worker
.venv/Scripts/mas submit --dag benchmarks/url_shortener/dag.json --wait
docker kill multi-agent-system-worker-2      # the reaper reassigns its task; the run still passes
```

## Status

**M1 substrate — mostly done (2026-08-16).** Schema, state machines, deterministic orchestrator on hand-written DAGs, leases/heartbeat/reaper/retry, Compose workers as real processes, immutable artifacts (DB-enforced), fail-closed verifier stage, clarifying questions + assumptions (ADR-006), tool allow-lists, metrics, replay — all LLM-free and covered by 87 tests.

**Step 6 done (2026-08-16):** one bare git repo per run + one worktree per attempt, inputs assembled by merge, conflicts surfaced (never averaged away), the runtime commits and mints `git_commit` / `<sha>:path` artifacts, integration = the merge commit, `run/<run>/integration` promoted on PASS, `context_spec` enforced (rule 10), worker containers non-root / read-only / no egress / no caps. `mas artifacts <run_id>` shows what a run produced. Stabilized after review (heartbeat through settlement, atomic report, one lock order `run → task → attempt`) and gated by `scripts/stress_step6.py` (170 runs, 0 deadlocks). 80 tests.

**Step 7A done (2026-08-16):** the verifier receives no database connection, resolves one exact integration SHA, hashes a human-owned suite, and runs both in an ephemeral Docker sandbox with no network, read-only mounts/rootfs, no capabilities, no-new-privileges, and hard time/CPU/memory/PID/output limits. Missing suite/commit/image/check, malformed output, crash, timeout, and failed checks all fail closed. Five real fixtures plus a worker→Git→verifier→verdict test exercise the path. `StubVerifier` is explicit test mode only.

**Still missing before an LLM goes in:** the broader trusted adapter set (`http_status`, `build_succeeds`, `restart_persists`, `tests_required`) and concurrent run ticking in the orchestrator service. The hardened Compose orchestrator intentionally has no Docker socket; use a host orchestrator for real verification until a separate verifier service/runner API exists. See [docs/roadmap.md](docs/roadmap.md).

## Later

If the MVP passes, the same runtime is pointed at cybersecurity: the goal becomes *"Investigate alert INC-9182, determine whether compromise occurred, produce an evidence-backed conclusion"*, workers get security tools (Zeek, osquery, identity logs, Quickwit search) instead of coding tools, and claims/evidence become first-class (see [ADR-004](docs/adr/004-claims-deferred.md)). If the runtime barely changes, the MAS core was real. That is the **Autonomous SOC**, the first serious application — not part of this MVP.

## License

MIT — see [LICENSE](LICENSE).
