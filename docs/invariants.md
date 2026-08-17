# Invariants

These are the rules the system must never break. They come from the MAS failure literature (MAST's specification / coordination / verification-termination failure classes; MetaGPT's structured-workflow result; Anthropic's parallel-decomposition experience) and from our own design decisions. Each one names how it is enforced in code so it can be tested, not just believed.

Breaking one requires an ADR that says so explicitly.

---

### I-1 · Planner ≠ Orchestrator
The LLM **proposes** (a DAG, an amendment). Deterministic code **decides** (readiness, leases, retries, budgets, termination).
*Enforced:* no model calls in `mas/orchestrator/`, `mas/planner/validator.py`, `mas/verifier/`, `mas/db/`. Planner output is typed JSON that must pass the validator.

### I-2 · Agents never set state
No agent (planner, worker agent, integration agent) writes `status` on a run, task, attempt, or artifact. Agents *report*; the orchestrator *transitions*.
*Enforced:* transitions exist only in `mas/orchestrator/state_machine.py`; DB roles/permissions for worker connections deny direct status updates.

### I-3 · External reality decides success
Completion is determined by the fixed acceptance suite run by the deterministic verifier — never by an agent saying "done", never by agent agreement.
*Enforced:* verifier is a stage, not a task; `acceptance/` is read-only to workers; only `mas/verifier/` may set the run verdict.

### I-4 · Every run terminates with a verdict inside its budget
Tokens, cost, wall-clock, tasks, attempts, re-plans, plan attempts — all hard limits. Exceeding any → `ABORTED:<reason>`. There is no "still running" forever. This binds runs the operator ends too: `mas plan` produces a validated plan and then aborts its run with the reason code `CANCELLED` (ADR-009) rather than leaving it open — and a `CANCELLED` run is never evidence about the system.
*Enforced:* budget checks on every transition and on a timer in the orchestrator; a test that starves each budget and asserts a terminal state.

### I-5 · Artifacts are immutable
Once published, content and `ref` never change. Better versions are new artifacts; the old one is `superseded_by`. Retries start clean and may read prior candidates as hints, never inherit them.
*Enforced:* artifact module exposes publish / set-status / supersede only; no `UPDATE` on `ref`/`meta` content; DB constraint or trigger.

### I-6 · Workers are disposable; the system survives them
Every attempt is leased with a heartbeat. A dead worker's attempt is reaped to `ABANDONED` and the task returns to `READY`. Nothing the system needs lives only in a worker's memory.
*Enforced:* reaper in orchestrator; failure-injection test that kills a worker mid-attempt and asserts the run completes.

### I-7 · Workers are isolated
Own process, own git worktree, only the context named in `context_spec`, only the tools/permissions granted to the task. No shared checkout, no writes to `main`, no cross-worktree access.
*Enforced:* workspace module creates per-attempt worktrees; tool layer scoped per attempt; tokens-in per attempt recorded and reported.

### I-8 · Coordination through artifacts, not conversation
Agents do not message each other. They publish artifacts; other tasks read accepted artifacts named in their context. No all-to-all channels, no agent gets every tool.
*Enforced:* there is no inter-agent messaging primitive in the codebase.

### I-9 · No worker owns the global goal
Workers see their task's goal and contracts, not the whole mission. Only the planner (and the human) reason about the whole objective.
*Enforced:* worker prompt/context is built from the task row, not the run row.

### I-10 · Agreement ≠ truth; disagreement stays visible
Conflicting outputs are kept as competing `candidate` artifacts and resolved by an explicit `decision` artifact with rationale; losers become `superseded`/`rejected`, never deleted or averaged away.
*Enforced:* artifact model (ADR-002, ADR-004); forced-disagreement demo in evaluation.

### I-11 · No external-effect action without a deterministic gate
In the MVP the only side effects are writes inside a worktree and calls to the model API. Anything else (network, deploy, destructive git) is prohibited by policy and rejected by the validator/tool layer.
*Enforced:* tool allow-list per capability; validator rule 4.

### I-12 · Everything is auditable
Every transition and significant action appends to `events`. A run can be replayed from the event log alone.
*Enforced:* transitions and publishes write events in the same transaction; `mas replay <run_id>` exists.

---

### The principle behind all of them

> **Agents are the most disposable part of the system.** The persistent intelligence is not any individual model instance; it is the combination of task state, artifacts, provenance, policies, acceptance criteria, execution history, and feedback loops.
