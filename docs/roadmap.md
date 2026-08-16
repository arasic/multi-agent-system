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
- [ ] 6. Isolated git worktrees per attempt; immutable artifacts (`git_commit` refs); supersession; conflict-as-competing-candidates + `decision` artifact. *(artifact model + supersession done; worktrees + real commit refs pending — `mas/workers/workspace.py` is a placeholder)*
- [ ] 7. Fixed external acceptance verifier: `acceptance/<benchmark>/` read-only; ephemeral clean environment; `verification` artifact + event. *(verifier stage + `verification` artifact done with `StubVerifier`; acceptance-suite runner pending)*
- [~] 8. **End-to-end run with stub workers**: hand-written DAG → workers emit fixture commits → integration → verifier PASS. This becomes the permanent LLM-free CI test. `mas replay` works. *(e2e with stub artifacts + stub verifier is the current CI test — 49 tests, no API key; becomes complete once 6 and 7 land)*

Also landed early (LLM-free): the **structural subset of the DAG validator** (rules 1, 2, 3, 5, 6, 7 incl. per-task `max_attempts` bounds; capability check covers the synthesized integration task; rules 4, 8, 9 at steps 12/13), and `mas/metrics.py` (wall-clock, max concurrency, parallelism efficiency, tokens/cost) for evaluation.md §4.

**Gate:** A3–A6, A9, A10 pass with no API key. **Status: A3, A4, A5, A6 (artifact side), A9, A10 demonstrated by `tests/test_orchestrator.py`; A6's worktree half and the real acceptance verifier remain (steps 6–7).**

### Open issues
- **One unreproduced ~300 s stall (2026-08-16).** During a 3× loop of `tests/test_cli.py`, one iteration reported `1 failed … in 308 s`; the run's client-side timeout (then 300 s) most likely fired on a non-terminal run. 32 subsequent reproductions (12× pytest, 20× `mas run --chaos-kill-after`) all passed. Mitigations landed: (a) `run_until_terminal` watchdog logs open tasks / RUNNING attempts (lease left, running time) after 20 s without events; (b) `mas status` prints the same for non-terminal runs; (c) client timeouts now sit *above* `max_wallclock_s`, so the run's own budget aborts with a verdict first (I-4). **If it recurs: capture `mas status <run_id>` and `mas replay <run_id>` before the next test truncates the DB.** Suspects to rule out next: lock interplay between `SELECT … FOR UPDATE` on `runs` (tick/claim) and FK `KEY SHARE` locks taken by inserts into `events`/`attempts`/`artifacts`; a worker thread blocked inside `_publish_and_report` while its heartbeat is already stopped.

## M2 — Intelligence, one piece at a time
- [ ] 9. `ModelProvider` interface + usage accounting on attempts.
- [ ] 10. LLM worker (fast/cheap model). URL-shortener smoke test with hand-written DAG.
- [ ] 11. LLM planner (strong model) → typed JSON DAG only.
- [ ] 12. Deterministic DAG validator (all nine rules); planner retry on rejection; `max_plan_attempts`.
- [ ] 13. Bounded re-planning: three triggers, amendment semantics, `max_replans = 2`.

**Gate:** A1, A2, A7, A8 pass; smoke test passes end-to-end with LLM planner + LLM workers.

## M3 — Fair comparison
- [ ] 14. Single-agent harness inside the same runtime (configs A, B); `max_concurrency` knob (config C = D with 1). Fairness rules from evaluation.md §3.
- [ ] 15. Width benchmark: N = 1/2/4/8/16 × configs A/B/C/D × ≥ 5 runs. Metrics, plots, write-up.

**Gate:** value question answered with data (either way).

## After the MVP (not now)
- Richer claim/conflict logic (claims table, evidence, reviewer) — when the SOC needs claims about the world.
- Dynamic model routing on (risk, complexity, confidence).
- Autonomous SOC as experiment #2: same runtime, security tools, incident goal.
