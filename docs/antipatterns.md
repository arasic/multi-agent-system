# MAS anti-patterns — and what this project does about each

Status: living document (started 2026-08-16). Every entry names a **countermeasure in this design** and its **enforcement status**. New mechanisms should be able to say which entry they address; if a proposed feature makes any entry worse, that's a design smell.

Sources: MAST — *Why Do Multi-Agent LLM Systems Fail?* (Cemri et al., 2025; 14 failure modes in 3 families), Anthropic's multi-agent research write-up (2025), MetaGPT (2023), *More Agents Is All You Need* (2024), and the mistakes visible in our own design discussion (`discuss.md`) — including ones we made and later reversed.

Legend: ✅ enforced and tested today · 🟡 designed, not built yet (roadmap step) · ⚠ gap — added to the roadmap · ✔ avoided by construction

---

## A. Specification & system design (MAST family 1)

| # | Anti-pattern | Why it fails | Our countermeasure | Status |
|---|---|---|---|---|
| A1 | **Agents disobey the task specification** (MAST 1.1) | Free-form goals; no contract on what "done" looks like | Every task has an `output_contract`; the orchestrator checks it before `COMPLETED` (`contracts.missing_outputs`); the run's success is decided by the external verifier | ✅ contract check tested; 🟡 real acceptance verifier (step 7) |
| A2 | **Agents disobey their role** — "You are Alice, a brilliant engineer" (MAST 1.2; discuss.md) | Personality is not a boundary; roles drift | Specialisation by **capability + tools + permissions + contract**, not persona; capability-gated claiming (`claim_task` … `capability = ANY`); per-capability tool allow-list validated by rule 4 and handed to the agent as `ctx.tools` | ✅ gating + allow-list policy; 🟡 tool *implementations* bound to the list (step 10) |
| A3 | **Step repetition / duplicated work** (MAST 1.3) | No shared record of what's done | One task = one lease; `FOR UPDATE SKIP LOCKED` claiming; attempts are numbered; completed tasks are terminal | ✅ |
| A4 | **Loss of history / context** (MAST 1.4) | Conversation *is* the memory; it grows, truncates, distorts | Artifacts + events in Postgres are the memory; agents are stateless between attempts | ✅ |
| A5 | **Unaware of termination conditions** (MAST 1.5) | Nobody owns "are we finished?" | Deterministic run state machine; termination = integration `COMPLETED` → verifier verdict, or budget abort. Never an LLM saying "done" (I-3, I-4) | ✅ |
| A6 | **The LLM orchestrator owns everything** — plans, executes, judges (discuss.md) | Reproduces MAST families 1–3 in one place; unverifiable termination | Planner ≠ Orchestrator (ADR-001): LLM proposes typed JSON; validator + orchestrator decide; no model calls in orchestrator/validator/verifier/db (I-1) | ✅ (import boundary), 🟡 planner itself (step 11) |
| A7 | **Workers reinterpret the global mission** (discuss.md) | Each agent re-solves the whole problem differently | Workers get their task's goal, contract and scoped inputs — never the run goal (I-9); context is `context_spec`, not "the whole repo" | ✅ context built from task row; artifact scoping enforced (`artifacts_from`, rule 10) and assembled into the worktree; 🟡 path/glob read-enforcement in the tool layer (step 10) |
| A8 | **Every agent gets every tool** (discuss.md) | Blast radius; prompt-injection reach; unauditable actions | Tools scoped per capability/task (`capabilities.py`); rule 4 rejects unknown / not-allowed / forbidden (`network`, `deploy`, `git_push`, `acceptance_write`) tools; MVP side effects limited to the worktree + model API (I-11) | ✅ policy + validation + `ctx.tools`; containers non-root, read-only, no egress, no capabilities; 🟡 enforcement inside the tool layer (step 10) |
| A9 | **Verification as a planner-controlled task** — our own earlier draft had "T7 Verification" | The thing being verified controls the verdict | Verification is an orchestrator *stage*, never a DAG task; `acceptance/` read-only; agents' own tests are advisory (ADR-003) | ✅ |
| A10 | **Overrides that bypass budgets** (found in review) | "Just this task gets 99 retries" | Validator rejects per-task `max_attempts` outside `[1, max_attempts_per_task]`; all budgets are run-level hard limits | ✅ |

## B. Coordination & inter-agent alignment (MAST family 2)

| # | Anti-pattern | Why it fails | Our countermeasure | Status |
|---|---|---|---|---|
| B1 | **A room full of agents chatting** — conversation as coordination (discuss.md) | Context growth, distortion, hidden dependencies, no audit, no recovery | Coordination through **artifacts, not messages**; there is no inter-agent messaging primitive (I-8, ADR-002) | ✔ by construction |
| B2 | **Conversation reset** (MAST 2.1) | Agents lose the thread and start over | No conversation to reset; a retry is a new attempt from clean state with prior candidates as hints | ✔ |
| B3 | **Fail to ask for clarification** (MAST 2.2) | Agent guesses instead of asking; work proceeds on a wrong premise | Planner may return `questions[]` instead of a DAG → run `AWAITING_INPUT` → `mas answer` records an `answer` artifact → planning resumes with the Q&A; bounded by `max_questions`; the clock runs from creation; the single-agent baseline goes through the same driver (ADR-006) | ✅ mechanism (tested end-to-end with `StubPlanner`); 🟡 LLM planner actually asking (step 11) |
| B4 | **Task derailment** (MAST 2.3) | Agent wanders off-goal | Bounded task goal + output contract; contract-unmet = failed attempt; per-attempt runtime limit → `TIMEOUT` | ✅ |
| B5 | **Information withholding** (MAST 2.4) | Upstream result never reaches downstream | Downstream context = **outputs of dependencies** (`outputs_of_dependencies`), computed by the runtime, not by the agent's goodwill | ✅ |
| B6 | **Ignoring another agent's input** (MAST 2.5) | Agent re-derives instead of using upstream artifact | Inputs are explicit in the attempt (`ctx.inputs`, recorded in artifact `meta.inputs`); planner-level: `context_spec.artifacts_from` | ✅ plumbing; 🟡 measured in evaluation (context tokens per attempt) |
| B7 | **Reasoning–action mismatch** (MAST 2.6) | Says "done", didn't publish; says "tests pass", verifier disagrees | Output contract checked against *published* artifacts; verifier ignores agent claims | ✅ |
| B8 | **Merging by "ask another LLM to summarise"** (discuss.md) | Averages away contradictions; conflicts vanish | Conflicts stay visible: competing `candidate` artifacts → `decision` artifact with rationale → losers `superseded`/`rejected` (I-10, ADR-002/004) | ✅ representation; 🟡 forced-disagreement demo (evaluation A7, M2) |
| B9 | **Agreement = truth** (discuss.md; *More Agents…* caveat) | Three agents can be wrong together | Nothing becomes `accepted` by vote; only integration/decision or verifier PASS accept; only the SUCCESS attempt's outputs flow downstream | ✅ |
| B10 | **Communication / context explosion** (discuss.md) | Tokens scale with agents²; nobody can afford it | No fan-in of chat; artifacts referenced, not copied; per-task `context_spec`; tokens-in per attempt is a first-class metric | ✅ metrics + artifact scoping; 🟡 path scoping in the tool layer (step 10) |
| B11 | **Hallucination cascade / trust laundering** (discuss.md) | One agent's guess becomes another's premise, then "verified" by a third | Only outputs of *successful* attempts propagate; nothing is accepted without external verification; provenance on every artifact (run/task/attempt/model) | ✅ plumbing; 🟡 real verifier |
| B12 | **Prompt injection through artifacts** (discuss.md challenge #11) | Untrusted content in an upstream artifact steers a downstream agent | Two halves: (a) *containment* — tool allow-lists bound what an injected instruction could do, forbidden tools can never be granted, verifier unreachable from agents; (b) *presentation* — artifact content and tool output rendered to the model as data, never as instructions | ✅ containment policy (rule 4); 🟡 presentation + tool-layer enforcement (step 10) |

## C. Verification & termination (MAST family 3)

| # | Anti-pattern | Why it fails | Our countermeasure | Status |
|---|---|---|---|---|
| C1 | **Premature termination** (MAST 3.1) | Agent stops when it *feels* done | Run ends only via verifier verdict or budget abort; task ends only via contract check | ✅ |
| C2 | **No / incomplete verification** (MAST 3.2) | "Looks good" | Fixed external acceptance suite, written before the run, run by deterministic code (ADR-003) | ✅ fail-closed sandboxed runner (7A) + trusted adapters (7B) + verifier service for service mode (7C) |
| C3 | **Incorrect / circular verification** (MAST 3.3) — agents write the tests that judge them, even with a casual human "approve" | Weak or rubber-stamped tests pass | `acceptance/` is human-written and read-only to workers; agents' tests are advisory. For ad-hoc goals: **Acceptance Contract Freeze** (ADR-007) — planner proposes a manifest, human approves once, hashed + frozen, executable checks come only from trusted adapters / templates / human-owned suites, unmappable criteria fail closed | ✅ rule + ADR; 🟡 mount enforcement + adapters (steps 6/7) |
| C4 | **Runs that never end** ("still thinking…") | No hard limits | Tokens, cost, wall-clock, deadline, tasks, attempts, re-plans, plan attempts — orchestrator aborts with `ABORTED:<reason>`; every run ends with a verdict (I-4) | ✅ |
| C5 | **States with no way out** (found in review: `VERIFYING` could strand) | A crashed process leaves an unrecoverable state | Every non-terminal state must have a re-entrant path: `VERIFYING` retried under a session advisory lock; leases expire; budgets abort | ✅ (tested: crash mid-verify → next tick passes) |
| C6 | **Accepting everything the winner *and* the losers produced** (found in review) | Hints from failed attempts leak into the final result | Only the SUCCESS attempt's candidates are accepted (`outputs_of_task`) | ✅ |
| C7 | **Self-approved self-improvement** (discuss.md) | System promotes its own changes | Out of MVP scope; if ever built: sandbox → benchmark → regression → canary → rollback, external verifier decides | ✔ not built (deliberately) |

## D. Architecture, scope & process

| # | Anti-pattern | Why it fails | Our countermeasure | Status |
|---|---|---|---|---|
| D1 | **The generic-framework trap** — a year of abstractions, no application (discuss.md) | Solves problems nobody has | Ten nouns, six tables; one benchmark family; ~6-week time-box; SOC deferred | ✅ (docs/evaluation.md §6) |
| D2 | **Prompt spaghetti** — `if network: call GPT` (discuss.md) | Unmaintainable | Generic core (Task/Capability/Artifact/Verifier); domain enters via capabilities and tools only | ✅ |
| D3 | **Starting with MAS instead of climbing the ladder** (discuss.md: deterministic → workflow → single agent → agent+verifier → parallel → MAS) | Complexity before evidence | Built in that order: deterministic substrate first, LLM last; single-agent configs A/B are first-class | ✅ |
| D4 | **More agents for their own sake** (discuss.md rates it 2/10) | Cost without benefit | The value question is measured (A/B/C/D, N-sweep); MAS must *earn* each agent | 🟡 M3 |
| D5 | **Parallelising coupled work** (Anthropic: interdependent tasks are poor fits) | Merge pain eats the speedup | Benchmark design separates the coupled smoke test from the width benchmark; integration is an explicit task | 🟡 M3 |
| D6 | **Unfair baseline** — MAS+verifier vs naive single agent | Measures the verifier, not the architecture | Baseline = single agent + same tools + same verifier + same budgets, inside the same runtime | 🟡 step 14 |
| D7 | **Design drift between iterations** (our own discussion lost conflict handling, moved verification) | Decisions evaporate | Frozen docs + ADR process; drift is named and resolved, not absorbed | ✅ (docs/adr) |
| D8 | **Product/architecture confusion** — MAS vs SOC vs "autonomous organisation" (discuss.md) | Building three things at once | MVP = MAS runtime + fair comparison; SOC = experiment #2 | ✅ |
| D9 | **Unverified claims presented as facts** (model names, TPS, prices in discuss.md) | Architecture built on numbers nobody checked | `docs/models.md` is dated, marked unverified, non-load-bearing; `ModelProvider` abstraction | ✅ |
| D10 | **Optimising tokens/s instead of end-to-end latency** (discuss.md) | Fast model, slow system | Metrics are wall-clock, critical path, parallelism efficiency, retries — not TPS | ✅ |
| D11 | **Strongest model everywhere** ("Sol as the orchestrator") | Cost explosion, no gain in correctness | Orchestrator has no model at all; three roles (planner strong, workers fast, reviewer independent) | ✅ by construction; 🟡 roles (step 9–11) |
| D12 | **Overclaiming "fire and forget"** | Autonomy without observability | Stall watchdog, `mas status`/`replay`, budgets; autonomy grows only with verifiability (discuss.md's own rule) | ✅ |
| D13 | **Shared-state collisions** — tests or services trampling each other (found in practice) | Flaky, misleading failures | Per-process test databases; run pools | ✅ |

## E. Cost, scale, observability

| # | Anti-pattern | Why it fails | Our countermeasure | Status |
|---|---|---|---|---|
| E1 | **Cost explosion** (Anthropic: ~15× tokens; discuss.md) | Nobody counts | Usage recorded per attempt and per run; hard `max_tokens`/`max_cost_usd`; cost is a first-class evaluation axis | ✅ accounting + real per-call telemetry (`model_calls`, priced from config, unpriced flagged); per-attempt call/token budget: strict call limit, tokens accounted after each response with further calls refused once exhausted — overshoot bounded to one completed call, then the run aborts (step 9) |
| E2 | **Can't tell which agent broke things** (discuss.md) | No provenance | Every transition and artifact carries run/task/attempt/worker; `mas replay` reconstructs the run | ✅ |
| E3 | **Debugging is impossible** (discuss.md) | Non-deterministic control flow | Deterministic orchestrator; state machine tests; event log | ✅ |
| E4 | **Silent stalls** | Nothing progresses, nothing complains | Watchdog logs open tasks/attempts after 20 s of no events; budgets end it | ✅ (one unreproduced stall on record — roadmap) |
| E5 | **Reaping live work** — a slow commit/publish outlives the lease and a healthy attempt is marked ABANDONED (found in review) | Duplicate work, stale reports, flaky runs | Heartbeat runs through settlement; report is one atomic transaction; the reaper re-checks expiry under the row lock | ✅ (tests: slow publish not reaped; deliberate deaths → exactly one abandonment) |
| E6 | **Inconsistent lock order** → PostgreSQL deadlocks under load (found in review: claim took task→run, everything else run→task) | Random transaction aborts, lost reports | One order `run → task → attempt → inserts`; `FOR NO KEY UPDATE`; concurrency regression test + stress gate | ✅ |
| E7 | **Unbounded output capture from sandboxed code** (found in review of the verifier) | Agent-authored code floods stdout; the host disk fills before any cap applies | Capture through pipes with an in-memory cap; kill on overflow → `INVALID`; `--log-driver none`; flood fixture in the suite | ✅ (peak host disk during a 400 MB flood: 285 MB → 0) |

---

## Open gaps (tracked in roadmap.md)

1. ~~**B3 — clarifying questions**~~ → done (ADR-006, `AWAITING_INPUT`, `mas answer`); remaining: the LLM planner using it (step 11).
2. **B12 — prompt-injection boundary**: containment done (rule 4 allow-lists, forbidden tools); presentation-as-data and tool-layer enforcement land with the LLM worker (step 10).
3. ~~**A8/A2 — validator rule 4**~~ → done (policy half); tool implementations bound to the allow-list at step 10.
4. **C2/C3 — real acceptance-suite verifier and read-only mount** (steps 6–7).
5. **B8 — forced-disagreement demo** (M2, evaluation A7).
6. **D4–D6 — the fair comparison itself** (M3).
