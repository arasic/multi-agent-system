# Roadmap

Build order for the MVP. Deterministic substrate first, LLMs last. Do not skip ahead: MAST's central finding is that system-level failures aren't fixed by stronger models, so the substrate must be proven with stub workers before a model is wired in.

Time-box: ~6 weeks from step 2 (see [evaluation.md §6](evaluation.md#6-time-box)).

## M0 — Design frozen
- [x] 1. Write `architecture.md`, `invariants.md`, `evaluation.md`, ADRs. Freeze Run / Task / Attempt / Artifact and the three state machines. **(done 2026-08-16)**

Legend: `[x]` done · `[~]` partially done (see note) · `[ ]` not started.

## M1 — Deterministic substrate (no LLM anywhere)
- [x] 2. Postgres schema + migrations: `runs`, `tasks`, `task_dependencies`, `attempts`, `artifacts`, `events`. `mas migrate`. **(done 2026-08-16; artifact immutability enforced by DB trigger)**
- [x] 3. Orchestrator on a **hand-written DAG**: readiness, scheduling, dependency respect, `T2/T3/T4` concurrent, budgets, termination. State machine unit tests. **(done; `benchmarks/url_shortener/dag.json`, `mas run`)**
- [x] 4. Leases + heartbeat + reaper + retry. Kill workers deliberately; assert recovery. **(done; `--chaos-kill-after`, TIMEOUT via `max_attempt_runtime_s`, zombie reports rejected)**
- [x] 5. Docker Compose workers ×3–4 (real separate processes), polling Postgres. **(done; `mas submit` + `orchestrator`/`worker` services; `docker kill` mid-attempt recovers)**
- [x] 6. Isolated git worktrees per attempt; real `git_commit` artifacts and a real integration merge commit; **enforced `context_spec`** (`artifacts_from` narrows inputs; validator rule 10; `paths` passed to the tool layer); **non-root, read-only, no-egress worker containers**; supersession; conflict-as-competing-candidates + `decision` artifact. *(built 2026-08-16; **stabilization follow-up** after review found successful attempts reaped as ABANDONED and a real Postgres deadlock: heartbeat now runs through settlement, report is one atomic transaction (artifacts + contract + settle), one lock order `run → task → attempt` with `FOR NO KEY UPDATE`, task-UUID paths/branches, UUID temp init, OS errors → `WorkspaceError`, cleanup in `finally`, unsafe task ids rejected, concurrent repo init tested. **Gate:** `scripts/stress_step6.py` — 100 diamonds / 50 chaos / 20 parallel: zero deadlocks, zero stale reports, exactly one abandonment per deliberate death, zero leaked worktrees. **Gate passed 2026-08-16:** 170 runs (100/50/20), 0 deadlocks, 0 stale, 50 deaths → 50 abandonments, 0 leaked worktrees, 0 error logs, 800 s; full suite 80/80 alongside. Still deferred: `decision` artifact demo (needs a reviewer, M2), agent-level path enforcement (tool layer, step 10). Multi-host = one volume.)*
- [~] 7. **Fail-closed acceptance runner**: `acceptance/<benchmark>/` read-only; ephemeral clean environment; hard timeout; unmappable criteria = FAIL, never "skipped"; `verification` artifact + event. First **trusted adapters** for ADR-007 criteria types (`http_status`, `build_succeeds`, `restart_persists`, `tests_required`). Also: **orchestrator service ticks runs concurrently** (bounded executor, own connection per run, no two threads on one run) — today `orchestrate_forever` ticks sequentially, so a slow verify/plan would block other runs. *(**7A done 2026-08-16:** DB-free structured verifier boundary; exact integration SHA; suite SHA-256; hardened Docker sandbox with no network/read-only/no caps/no-new-privileges and hard time/CPU/memory/PID/output limits; strict complete report; fail closed on missing suite/commit/image/check, unknown/duplicate check, timeout, crash, invalid output, suite mutation, or failed check; immutable evidence includes commit/suite/image/limits/duration. Human-owned URL-shortener pack plus good/bad/hang/network/missing-check fixtures. Stub is explicit test injection only. Pending: reusable adapter library, concurrent ticking, and a separate distributed verifier boundary—the hardened Compose orchestrator intentionally has no Docker socket.)*
- [x] 8. **End-to-end run with stub workers**: hand-written DAG → worker emits a known-good application commit → integration commit → real external verifier PASS → only that commit accepted. Permanent LLM-free CI test; `mas replay` works. **(done 2026-08-16; 87-test suite, including five real Docker fixtures and full worker→Git→verifier→artifact→verdict path; no API key)**

Also landed early (LLM-free): the **structural subset of the DAG validator** (rules 1–7: incl. per-task `max_attempts` bounds, capability check covering the synthesized integration task, and rule 4 capability→tool allow-lists; rules 8, 9 at steps 12/13), and `mas/metrics.py` (wall-clock, max concurrency, parallelism efficiency, tokens/cost) for evaluation.md §4.

**Gate:** A3–A6, A9, A10 pass with no API key. **Status: A3, A4, A5, A6 (artifacts + worktrees), A9, A10 demonstrated (`tests/test_orchestrator.py`, `tests/test_workspace.py`); the real acceptance verifier remains (step 7).**

### Open issues
- **One unreproduced ~300 s stall (2026-08-16).** During a 3× loop of `tests/test_cli.py`, one iteration reported `1 failed … in 308 s`; the run's client-side timeout (then 300 s) most likely fired on a non-terminal run. 32 subsequent reproductions (12× pytest, 20× `mas run --chaos-kill-after`) all passed. Mitigations landed: (a) `run_until_terminal` watchdog logs open tasks / RUNNING attempts (lease left, running time) after 20 s without events; (b) `mas status` prints the same for non-terminal runs; (c) client timeouts now sit *above* `max_wallclock_s`, so the run's own budget aborts with a verdict first (I-4). **If it recurs: capture `mas status <run_id>` and `mas replay <run_id>` before the next test truncates the DB.** Suspects to rule out next: lock interplay between `SELECT … FOR UPDATE` on `runs` (tick/claim) and FK `KEY SHARE` locks taken by inserts into `events`/`attempts`/`artifacts`; a worker thread blocked inside `_publish_and_report` while its heartbeat is already stopped.

### Agreed build sequence from here (2026-08-16 review)
1. ~~Fix the wall-clock budget defect~~ — done: one clock from creation (`budgets.violation`), regression test.
2. ~~Write ADR-007 (Acceptance Contract Freeze)~~ — done.
3. ~~Step 6: real worktrees + commits, `context_spec` enforcement, non-root/no-network workers~~ — built, stabilized (heartbeat through settlement, atomic report, one lock order), stress gate passed.
4. ~~Step 7A: fail-closed acceptance runner~~ — done (+ review fixes: bounded capture / kill-on-overflow / `--log-driver none`, `expected_suite_sha256` pinning, event report truncation). Next: trusted adapter library, then concurrent run ticking.
5. Steps 9–10: model provider + LLM worker (tool layer bound to `ctx.tools`; artifacts rendered as data).
6. Step 11: planner producing DAG + assumptions + **acceptance-contract proposal** (ADR-007) or questions (ADR-006).
7. Step 13-lite: one bounded verifier-driven repair cycle.
8. Demonstrate one simple app; then the parallel adapters benchmark (M3); only afterwards the ticketing-system example.

## M2 — Intelligence, one piece at a time
- [ ] 9. `ModelProvider` interface + usage accounting on attempts.
- [ ] 10. LLM worker (fast/cheap model). URL-shortener smoke test with hand-written DAG. **Injection boundary** (antipatterns B12): artifact content and tool output are presented to the model as *data*, never as instructions; the tool layer binds `ctx.tools` (already validated by rule 4) to implementations and refuses anything else.
- [ ] 11. LLM planner (strong model) → typed JSON: a DAG **or a question batch** (`Planner` protocol, `PlanRequest` with Q&A history).
- [x] 11b. **Clarifying questions** (antipatterns B3 / MAST 2.2, ADR-006): planner may return `questions[]` → run `AWAITING_INPUT` → `mas answer` → planning resumes with the Q&A; `max_questions` budget; wall-clock counts from creation; same driver for the single-agent baseline. **(done 2026-08-16 with `StubPlanner`; tested end-to-end incl. cross-process demo)**
- [ ] 12. Deterministic DAG validator — rules 8 (cost estimate) and 9 (amendments); planner retry on rejection; `max_plan_attempts`. *(rules 1–7 incl. rule 4 tool allow-lists already land in M1)*
- [ ] 13. Bounded re-planning: three triggers, amendment semantics, `max_replans = 2`.

**Gate:** A1, A2, A7, A8 pass; smoke test passes end-to-end with LLM planner + LLM workers. Every entry in [antipatterns.md](antipatterns.md) marked 🟡 for M2 flips to ✅ or gets an explicit reason.

## M3 — Fair comparison
- [ ] 14. Single-agent harness inside the same runtime (configs A, B); `max_concurrency` knob (config C = D with 1). Fairness rules from evaluation.md §3.
- [ ] 15. Width benchmark: N = 1/2/4/8/16 × configs A/B/C/D × ≥ 5 runs. Metrics, plots, write-up.

**Gate:** value question answered with data (either way).

## After the MVP (not now)
- Richer claim/conflict logic (claims table, evidence, reviewer) — when the SOC needs claims about the world.
- Dynamic model routing on (risk, complexity, confidence).
- Autonomous SOC as experiment #2: same runtime, security tools, incident goal.
