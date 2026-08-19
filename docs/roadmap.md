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
- [x] 6. Isolated git worktrees per attempt; real `git_commit` artifacts and a real integration merge commit; **enforced `context_spec`** (`artifacts_from` narrows inputs; validator rule 10; `paths` enforced by the tool layer); **non-root, read-only, no-egress worker containers**; supersession; conflict-as-competing-candidates + runtime-validated `decision` artifact. Stabilization includes heartbeat-through-settlement, atomic report transactions, one `run → task → attempt` lock order, race-safe repository initialization, terminal worktree GC and the 170-run death/deadlock/worktree stress gate. Multi-host remains intentionally one shared volume for the MVP.
- [x] 7. **Fail-closed acceptance runner** — **7A** (576f95d + dc76304: exact-SHA checkout, hardened Docker sandbox, structured PASS/FAIL/TIMEOUT/ERROR/INVALID, bounded capture, `expected_suite_sha256` pinning, five LLM-free fixtures), **7B** (971cca4: trusted adapters `build_succeeds` / `tests_required` / `http_status` / `restart_persists`, typed schema shared by host and sandbox, trusted runner in the image, contract-based suites, `mas contract`), **7C** (concurrent bounded ticking with per-run advisory locks and one connection per tick; `--verifier external` + **verifier service** `mas verify --watch|--once` giving real sandboxed verdicts in service mode; container-side `timeout` + `--rm` so orphaned sandboxes self-terminate). Compose now runs the orchestrator with `--verifier external`; the host runs `mas verify --watch`. **Demonstrated:** submit through services → worker killed → verifier killed mid-verification → restarted → PASS with one verification artifact.
- [x] 8. **End-to-end run with stub workers**: hand-written DAG → worker emits a known-good application commit → integration commit → real external verifier PASS → only that commit accepted. Permanent LLM-free CI test; `mas replay` works. **(done 2026-08-16; 87-test suite, including five real Docker fixtures and full worker→Git→verifier→artifact→verdict path; no API key)**

Also landed early (LLM-free): the **structural subset of the DAG validator** (rules 1–7: incl. per-task `max_attempts` bounds, capability check covering the synthesized integration task, and rule 4 capability→tool allow-lists; rules 8, 9 at steps 12/13), and `mas/metrics.py` (wall-clock, max concurrency, parallelism efficiency, tokens/cost) for evaluation.md §4.

**Gate:** A3–A6, A9, A10 pass with no API key. **Status: A3, A4, A5, A6 (artifacts + worktrees), A9, A10 demonstrated (`tests/test_orchestrator.py`, `tests/test_workspace.py`, `tests/test_acceptance.py`).**

### Open issues
- **One unreproduced ~300 s stall (2026-08-16).** During a 3× loop of `tests/test_cli.py`, one iteration reported `1 failed … in 308 s`; the run's client-side timeout (then 300 s) most likely fired on a non-terminal run. 32 subsequent reproductions (12× pytest, 20× `mas run --chaos-kill-after`) all passed. Mitigations landed: (a) `run_until_terminal` watchdog logs open tasks / RUNNING attempts (lease left, running time) after 20 s without events; (b) `mas status` prints the same for non-terminal runs; (c) client timeouts now sit *above* `max_wallclock_s`, so the run's own budget aborts with a verdict first (I-4). **If it recurs: capture `mas status <run_id>` and `mas replay <run_id>` before the next test truncates the DB.** Suspects to rule out next: lock interplay between `SELECT … FOR UPDATE` on `runs` (tick/claim) and FK `KEY SHARE` locks taken by inserts into `events`/`attempts`/`artifacts`; a worker thread blocked inside `_publish_and_report` while its heartbeat is already stopped.

### Agreed build sequence from here (2026-08-16 review)
1. ~~Fix the wall-clock budget defect~~ — done: one clock from creation (`budgets.violation`), regression test.
2. ~~Write ADR-007 (Acceptance Contract Freeze)~~ — done.
3. ~~Step 6: real worktrees + commits, `context_spec` enforcement, non-root/no-network workers~~ — built, stabilized (heartbeat through settlement, atomic report, one lock order), stress gate passed.
4. ~~Step 7A~~, ~~7B~~, ~~7C: trusted adapters, concurrent ticking and verifier service~~ — done.
5. ~~Step 9: model provider + per-call telemetry~~ — done. Step 10 implementation and service-mode gates are done; real-provider evidence remains.
6. ~~Step 11: planner producing DAG + assumptions + acceptance-contract proposal (ADR-007) or questions (ADR-006)~~ — done (offline); live-provider planner smoke pending the key.
7. ~~Step 13: all bounded repair triggers, cancellation, validator rule 9 and deterministic progress verdicts~~ — done.
8. Demonstrate one simple app; then the parallel adapters benchmark (M3); only afterwards the ticketing-system example.

## M2 — Intelligence, one piece at a time
- [x] 9. `ModelProvider` interface + usage accounting on attempts — **done 2026-08-16:** provider-neutral message/tool shapes, typed errors; concrete `anthropic` (official SDK), `openai`-compatible (stdlib HTTP, any compatible endpoint/gateway) and `fake` providers selected by `MAS_MODEL_<ROLE>="<provider>:<model>"`; prices from `MAS_MODEL_PRICES`; `MeteredProvider` + `model_calls` table (per-call telemetry written immediately, survives worker death), settlement from the meter, per-attempt call/token budget capped by the run's remaining tokens; `ctx.model` for agents; `mas models [--ping]`; metrics/`mas status` show per-model calls and flag unpriced usage. 20 offline/DB tests. Real providers not yet exercised against a live API from this repo (no key in the dev environment) — the `--ping` path is the smoke test to run first.
- [~] 10. LLM worker (fast/cheap model). URL-shortener smoke test with hand-written DAG. **Implementation and key-less gates are complete:** tool layer, path jail, hardened per-attempt command sandbox, bounded LLM loop, injection containment, execution-runner service, model gateway, offline Compose run, and worker/provider/runner/sandbox death recovery are built and tested. **Open acceptance evidence:** run the real-provider worker stage in `scripts/live_smoke.py`; this cannot be marked `[x]` until a vendor key and current price table are supplied.
- [x] 11. LLM planner (strong model) → exactly one typed outcome: DAG, question batch, or acceptance-contract proposal. The deterministic driver validates, bounds retries and budgets, freezes a human-approved trusted contract, and records non-authoritative task-shape metadata without selecting A/B/C/D. The complete goal → proposal → approval → DAG → workers → frozen-suite verifier path passes offline. Open evidence is only the real-provider planner stage in `scripts/live_smoke.py`; validator rules and bounded repair are complete in steps 12–13.
- [x] 11b. **Clarifying questions** (antipatterns B3 / MAST 2.2, ADR-006): planner may return `questions[]` → run `AWAITING_INPUT` → `mas answer` → planning resumes with the Q&A; `max_questions` budget; wall-clock counts from creation; same driver for the single-agent baseline. **(done 2026-08-16 with `StubPlanner`; tested end-to-end incl. cross-process demo)**
- [x] 12. Deterministic DAG validator — rules 1–10 are implemented. Rule 8 enforces fundable token allocation and hard planner estimates; observed attempt duration uses the shortest successful attempt only as an advisory warning. Rule 9, completed with step 13, permits amendments to build only on recorded completed work and cancel only obsolete PENDING/READY work. Rejections return to the bounded planner as data; task-shape metadata is validated and recorded but cannot select A/B/C/D.
- [x] 13. Bounded re-planning: three triggers, amendment semantics, `max_replans = 2`; deterministic progress fingerprint and terminal verdict reasons `BUDGET_EXHAUSTED|NO_PROGRESS|UNSUPPORTED|POLICY_DENIED|INVALID_PLAN|UNRECOVERABLE_FAILURE` (reason codes, not new run states). `NO_PROGRESS` means repeated failure/amendment fingerprints or no reduction in failed acceptance criteria within the configured repair budget—never an LLM self-report. **13-lite done 2026-08-17** (architecture §8c): the verifier-FAIL trigger end to end — `verify.fingerprint` (failing ids, normalized failure classes, integration *tree* hash, accepted artifacts) → deterministic decision (repeat → NO_PROGRESS; `max_replans` spent → BUDGET_EXHAUSTED, or NO_PROGRESS without reduction) → REPLANNING → amendment protocol (new tasks on COMPLETED work, new sink; **rule 9**; repeated amendment hash rejected) → RUNNING → verify. `max_replans` is the only repair budget (`--max-replans`, CLI default 1 for the one-cycle demo). `verdict_reason` column + all endings coded; `mas status`/`mas run` print it. Offline demo `--verifier-fail-times 1 --planner fake`; `tests/test_repair.py` (11). **Full step 13 done 2026-08-17:** task-`FAILED` and `new_work_required` triggers (planner + budget permitting; quiesce before planning; `repair_handled` / `new_work_handled` so a trigger fires once), `cancel: [keys]` in amendments (rule 9: PENDING/READY only), trigger-typed failure reports; tests: task-failed → repair → PASS, new-work → add + cancel obsolete sink → PASS, no planner → deferred/unrecoverable, cancel validation + hashing. **Review fixes (2026-08-17):** planner rounds charged to the run (`settle_planner_usage`, `plan.usage`, from telemetry rows); attempt allocations are reservations claimed under the run lock (`attempts.token_allocation`, migration 0008) so concurrent attempts never jointly exceed the run's tokens; verifier `TIMEOUT`/`ERROR`/`INVALID` never trigger repair (coded terminal verdicts); a terminal run keeps no worktrees (`gc_run`); `tests/test_budget_guarantee.py` (10).

**Gate:** A1, A2, A7, A8 pass; smoke test passes end-to-end with LLM planner + LLM workers. Every entry in [antipatterns.md](antipatterns.md) marked 🟡 for M2 flips to ✅ or gets an explicit reason.

**Gate status (2026-08-17):** A2/A7/A8 and the complete distributed operator path pass key-less gates. The four-stage direct live smoke and the real-provider distributed smoke remain pending a vendor key and exact prices. `scripts/live_smoke.py` proves ping → worker → human-approved planner → live amendment/re-verification; `scripts/distributed_smoke.py` proves gateway → Compose workers → trusted host executor → external verifier → verified checkout. Both write evidence consumed by `scripts/mvp_gate.py`.

## M3 — Fair comparison
- [x] 14. Single-agent harness inside the same runtime (configs A, B); `max_concurrency` knob (config C = D with 1). **Done 2026-08-17:** config is executable policy (`mas/evaluation.py`), not a stored label: A/B = one `solve` + system integration, C = the same decomposed path forced to concurrency 1, D = requested parallelism. A/B verifier repair remains one fresh `solve` + integration and receives the failure report; it cannot silently become a MAS amendment. CLI and scheduler tests cover initial and repair shapes.
- [~] 15. Width benchmark: N = 1/2/4/8/16 × configs A/B/C/D × ≥ 5 runs. **Harness done 2026-08-17:** deterministic disjoint adapter DAG fixtures, immutable trusted suites, a permanent key-less N=4 real-sandbox/verifier gate, and `scripts/benchmark.py` (resumable immutable experiment manifest, Git/model/price/suite identities, append-only JSONL, duplicate/missing/unpriced detection, aggregate CSV/JSON, SVG plots, `completion.json`, and generated success-preserving time/cost crossover report). **Evidence-integrity hardening (2026-08-17 review):** runs are classified `pass` / `experimental` / `infrastructure` (`classify_run`; infrastructure-invalid cells are rerun, never counted); the report carries evaluation.md §4 distributions (time, critical path, cost/tokens, structure, failures, task shape) per cell; `mas.metrics` computes critical-path duration, worker utilisation, plan rejections, assumptions and attempt-failure classes; `scripts/mvp_gate.py` recomputes completion from the raw `runs.jsonl`, regenerates the report byte for byte, and binds all evidence to the clean, currently checked-out commit; `scripts/live_smoke.py --resume` carries passed stages forward only from the same commit/models/approval/pricing setup. **Experimental-design hardening (2026-08-17 review, ADR-009):** C and D of one (N, repetition) now execute **one** validated plan produced by `mas plan` under the parallel budget (recorded with its SHA-256 in `plans.jsonl`, planner cost reported separately) instead of each planning for itself; cells run on a deterministic randomized block schedule whose seed and order are frozen in the manifest; the manifest also freezes the environment (Python/platform, provider timeout/retries and Anthropic request shape, attempt call budget, sandbox/verifier image ids and limits); the gate additionally checks `matrix.schedule`, `matrix.environment`, `matrix.plans` and `matrix.paired_plans`. **Protocol and safety hardening (2026-08-18 review, ADR-010):** M3 is declared unattended and has no clarification answer key (asking is an experimental planning outcome; clarification is exercised in the live smoke instead); C/D execute under what the block's shared plan left of the equal total budget and every row carries both execution-only and `system_*` (planning added back) cost/latency; the matrix has a required aggregate ceiling (`--max-total-cost-usd`) enforced before every operation from spend recomputed out of the raw logs, plus pacing, a cooldown and a stop after N consecutive infrastructure failures, all resumable; reasoning effort must be an explicit frozen choice; the gate adds `matrix.schedule_followed`, `matrix.spend_cap` and `matrix.frozen_effort`; worktree creation fails closed where the filesystem cannot lock across processes. **Open evidence:** run the real 100-run minimum matrix and review/publish the generated analysis; model/key/prices are not configured in this environment. `scripts/mvp_gate.py` marks the frozen MVP complete only when the four-stage live smoke, the distributed smoke and the priced matrix exist on the same clean, checked-out revision.

**Gate:** value question answered with data (either way).

## Pre-paid readiness (M2 + M3 evidence), as of 2026-08-19 at `904957e`

The MVP finish line is frozen: the four artifacts below on one clean commit, with `scripts/mvp_gate.py` exiting 0.
No further implementation is required. Returning to code is legitimate only for a **reproduced** defect in one of these
gates — never for a hypothetical one.

**Free validation — complete.** Exact-head keyless stability gate PASSED at this commit on the first run (100 diamonds
+ 50 chaos + 20 parallel; 0 deadlocks, 0 leaked worktrees, 0 unexpected failures, `abandoned == died` exactly); tree
clean and `HEAD == origin/main`; images rebuilt; the offline distributed smoke PASSED at this commit
(`mode: offline`, so it proves the plumbing and deliberately does **not** satisfy the live artifact). Note the gate is
host-timing sensitive at `lease_s=1`: an earlier run on the same host produced two spurious abandonments and their
stale reports. Before treating that as a regression, diff the exercised modules (`mas/orchestrator`, `mas/workers`,
`mas/db`, `mas/verifier`, `mas/planner`) against the last green commit — `stress_step6.py` imports neither `mas.cli`
nor `mas/providers/`, so changes there cannot affect it. Report every run, not only the green one.

| Prerequisite | State |
| --- | --- |
| Quiet keyless stability gate | ✅ |
| Final commit clean and frozen | ✅ |
| Models, prices, effort, request shape and budgets frozen | ❌ operator decision |
| Dedicated capped vendor key | ❌ operator action (needs the upstream chosen first) |
| `mas doctor --require-live` fully green | ❌ derived from the two above (7 green / 5 red today) |

| Formal artifact | State |
| --- | --- |
| Four-stage priced live smoke (ping · worker · human-approved planner · bounded repair) | ❌ absent |
| Live distributed smoke through gateway + Compose | ❌ absent |
| 100-cell A/B/C/D matrix, ≥ 5 repetitions | ❌ absent |
| Value conclusion reviewed and recorded | ❌ blocked on the matrix |
| `scripts/mvp_gate.py` exits 0 | ❌ exit 2 |

Explicitly **not** MVP blockers, however reasonable they sound: reviewer roles, prompt-injection susceptibility
measurement, adaptive mode selection, persistent projects, broader profiles, SOC work, or any M4+ item.

## M4 — Controlled adaptive execution (after the fixed-mode MVP; ADR-008)
- [ ] 16. **Workflow template contract + registry:** a template defines versioned phases, slots, invariants and deterministic phase-exit checks—not a fixed app DAG. Record template id/version/hash in the plan. Add one `software-build-v1` template (`acceptance → architecture/contracts → implementation slots → integration → verification → bounded repair`) and prove that template instantiation and free-form planning both pass the same validator. Intermediate checks may block or repair a phase but cannot declare the run `PASSED`; only the frozen external acceptance verifier can do that.
- [ ] 17. **Deterministic execution-mode policy:** planner proposes task shape; policy chooses among `single_agent`, `sequential_workflow` and `parallel_centralized_mas` from independent width, critical-path ratio, dependency density, overlapping outputs, shared-context/tool requirements, integration risk, verifier coverage, budget and M3/history. Unknown/low-confidence cases fall back to the simpler mode or require explicit selection.
- [ ] 18. **Selector evaluation:** replay M3 tasks with the selector, compare its choice with the best fixed configuration, report selection accuracy, regret, cost and failures. The selector ships only if it beats an explicit simple policy and does not reduce success rate.
- [ ] 19. **Dynamic effort scaling:** choose worker count/concurrency from validated width and budget; prevent over-spawning. Dynamic model routing on risk/complexity/confidence is a separate knob and must be evaluated independently.

**Gate:** adaptive selection is evidence-backed, auditable and always overrideable; fixed modes remain available.

## M5 — Project continuity and iterative change runs
- [ ] 20. **Persistent project identity and repository binding:** introduce a project record that owns one repository and an ordered history of accepted baseline commits. A run targets an exact project baseline; run-local worktrees remain disposable. No run may mutate the project's accepted baseline directly.
- [ ] 21. **Change-request runs:** support bounded goals such as add/remove/refactor/migrate against an accepted baseline. Assemble scoped prior context from versioned requirements, architecture decisions, interfaces and relevant artifacts—not an unbounded conversation transcript. Record the requested change, assumptions, impact analysis and resulting commit lineage.
- [ ] 22. **Versioned acceptance and regression policy:** every change gets a newly approved acceptance-contract version that combines retained regression checks with new requirements. Removing or weakening an existing requirement requires an explicit contract amendment and human approval; agents can never delete a failing check to make a run pass.
- [ ] 23. **Atomic promotion, concurrency and rollback:** on PASS, compare-and-swap the project baseline from the run's starting commit to its verified integration commit. If another change has advanced the project, rebase/re-plan/re-verify rather than overwrite it. Preserve immutable release history and support rollback to any previously accepted commit and contract version.
- [ ] 24. **Longitudinal project gate:** demonstrate at least five consecutive changes to one application—add, modify, remove, migrate and repair—including one pair of concurrent changes and one rollback. Every accepted version must pass the applicable regression contract, and a failed change must leave the project baseline untouched.

**Gate:** a project can evolve across many independent, auditable runs without relying on chat history, weakening verification, losing accepted history or allowing concurrent changes to overwrite one another.

## M6 — Broader software coverage, one profile at a time
- [ ] 25. Application/toolchain profile schema: supported runtime/build image, capability/tool mapping, planner constraints, trusted acceptance criteria and explicit exclusions.
- [ ] 26. Add and independently gate profiles in order: Python API → CLI → web UI (browser/accessibility/visual evidence + human quality gate) → API+database → full-stack → multi-service platform. Each profile needs known-good/bad fixtures and fixed single-vs-MAS evaluation; no claim of “any app.”
- [ ] 27. Add richer synchronization or plan-staleness detection only for profiles with changing external state. Static software construction continues to use dependencies, immutable artifacts, integration and repair checkpoints.

## M7 — Domain experiment #2: Autonomous SOC
- [ ] 28. Reuse the runtime with security tools and an incident goal. Add richer claim/evidence/conflict logic (ADR-004), telemetry-backed verification and deterministic action/approval policy only when the domain requires them.

**Gate:** the core runtime changes little; domain capability, evidence and policy modules carry the SOC specialization.
