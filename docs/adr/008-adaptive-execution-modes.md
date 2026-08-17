# 008 — Controlled adaptive execution after the fixed-mode MVP
Status: Accepted (post-MVP direction; does not change the M0–M3 evaluation)
Date: 2026-08-17

## Context
The MVP is intentionally a multi-agent experiment with four fixed configurations (evaluation A–D). That is necessary
to measure decomposition and parallelism rather than allowing a router to hide which architecture produced a result.
It is not, however, the desired long-term product behavior: multi-agent execution can help decomposable work and hurt
sequential or tightly coupled work.

The current evidence points to three complementary lessons:

- [MetaGPT](https://arxiv.org/abs/2308.00352) supports reusable SOP-like workflows and explicit intermediate outputs
  over unstructured agent conversation.
- [Anthropic's production report](https://www.anthropic.com/engineering/multi-agent-research-system) shows why
  open-ended work still needs dynamic decomposition, explicit delegation boundaries and effort scaled to the task.
- [Towards a Science of Scaling Agent Systems, v3](https://arxiv.org/abs/2512.08296) finds that architecture/task
  alignment matters more than agent count: its tested configurations range from large gains on decomposable work to
  severe degradation on sequential planning. The selector must therefore be earned from measurements, not assumed.

MAST also shows that system design, coordination and termination failures cannot be repaired by a final verifier alone.
The runtime needs explicit task contracts, durable state, bounded correction and honest terminal reasons.

## Decision
1. **M0–M3 remain fixed-mode.** Configurations A/B/C/D are selected explicitly and compared with the same tools,
   verifier and budgets. No automatic architecture selector participates in the value benchmark.
2. **Step 11 may propose task-shape metadata, but not choose the runtime architecture.** The planner may estimate
   parallel width, dependency density, shared-output risk, integration risk and a suggested execution mode. The
   validator records or rejects the metadata; the configured A/B/C/D mode remains authoritative through M3.
3. **Known work may instantiate versioned workflow templates after M3.** A template defines phases, slots and
   invariants, not a fixed application DAG. The planner fills/adapts it; the validator checks the resulting DAG. The
   template id, version and content hash are recorded with the plan. Templates may define deterministic phase-exit
   checks that stop or repair invalid intermediate work, but those checks cannot declare the overall run successful;
   only the frozen external acceptance verifier can do that.
4. **Novel work may use dynamic decomposition.** It goes through the same typed task contracts, capability/tool
   policy, budget estimation and deterministic DAG validation as a template instance.
5. **The post-M3 selector is controlled and evidence-based.** The planner proposes; deterministic policy selects or
   rejects an execution mode using measurable features and M3/history: independent width, critical-path ratio,
   dependency density, overlapping outputs, shared-context requirements, tool count, integration risk, verifier
   coverage and budget. Initially supported modes are `single_agent`, `sequential_workflow` and
   `parallel_centralized_mas`; additional modes require their own benchmark evidence.
6. **Terminal causes are verdict reason codes, not a proliferation of run states.** Keep the current small run-state
   machine; where future control flow needs a human approval or input wait, model that explicitly. A non-passing
   terminal run records one of:
   `BUDGET_EXHAUSTED`, `NO_PROGRESS`, `UNSUPPORTED`, `POLICY_DENIED`, `INVALID_PLAN`, or
   `UNRECOVERABLE_FAILURE`.
7. **`NO_PROGRESS` is deterministic.** A progress fingerprint includes failed acceptance check ids, normalized
   failure classes, relevant integration/diff hashes, amendment hash and accepted artifacts. Repeating the same
   fingerprint/amendment or failing to reduce failed criteria for the configured number of repair cycles terminates
   the run. An LLM cannot declare that progress occurred.
8. **Objective success remains externally verified.** Subjective or consequential criteria require an independent
   rubric or human approval. Unsupported verifier criteria fail before execution; the selector cannot route around
   an uncheckable acceptance contract.
9. **Claims/evidence tables and general plan-staleness machinery remain deferred.** Software uses commits, artifacts,
   traces and verification reports for provenance. Rich claims arrive with the SOC/research domain (ADR-004). Rich
   synchronization/staleness detection is added only for changing external environments; task dependencies,
   integration and repair boundaries are the MVP checkpoints.

The target flow after M3 is:

```text
goal
  -> support + acceptance check
  -> task-shape assessment
  -> deterministic mode policy
       |-> known workflow template -> planner fills slots -> validator
       |-> novel dynamic DAG        -> validator
       |-> single-agent solve DAG
  -> deterministic orchestrator
  -> external verification
  -> bounded repair
  -> PASS | human required | terminal reason
```

## Consequences
- The long-term product is a controlled adaptive execution runtime, not an always-MAS planner.
- The current benchmark remains scientifically interpretable.
- Workflow reuse improves consistency without hard-coding one DAG per application or technology stack.
- The selector itself becomes an evaluated component with an explicit fallback to a simpler mode.
- Broader software coverage still requires application/toolchain profiles and trusted acceptance packs; routing alone
  does not create the ability to build arbitrary software.
- New verdict reasons require schema/event/CLI work at step 13, but do not require new core nouns or new run states.

## Alternatives considered
- **Always generate a parallel MAS DAG:** rejected; mismatched coordination can reduce quality, speed and cost.
- **Always use a prewritten workflow:** rejected; novel/open-ended tasks require dynamic decomposition.
- **Let the planner choose the topology directly:** rejected; architecture selection is a policy decision and must be
  validated against measurable structure, budget and historical evidence.
- **Add the selector before M3:** rejected; it would confound the single/sequential/parallel comparison needed to
  train and validate the selector.
- **Make every terminal cause a run state:** rejected; reason codes preserve a small state machine.
