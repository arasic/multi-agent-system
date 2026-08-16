# 001 — Planner ≠ Orchestrator
Status: Accepted
Date: 2026-08-16

## Context
MAST (Cemri et al., 2025) attributes most multi-agent failures to specification/system design, inter-agent coordination, and verification/termination — not to model weakness. Systems where an LLM plans, executes, and judges its own completion exhibit exactly these failures: derailment, repeated work, premature or missing termination. MetaGPT's structured SOPs and Anthropic's orchestrator-worker system both improve on free-form agent conversation by imposing structure the model does not control.

## Decision
Split the intelligent part from the correctness part and never merge them.

- **Planner (LLM):** answers *what needs doing, what can run in parallel, what depends on what, which capability is needed, is more work required.* Output is a typed JSON DAG or amendment. It has no authority.
- **Validator (deterministic):** accepts or rejects the planner's output against structural, capability, permission, and budget rules.
- **Orchestrator (deterministic):** owns readiness, leases, retries, budgets, state transitions, verifier stage, and termination. It never interprets task content and never calls a model.

Enforced as invariants I-1 and I-2: no model calls in orchestrator/validator/verifier/db; all status transitions in one module; agents report, never transition.

## Consequences
- The system stays debuggable: every decision that matters is deterministic and logged.
- The planner can be swapped, downgraded, or replaced by a hand-written DAG (which is how M1 is built).
- Some flexibility is lost: the planner cannot "just do" something outside the DAG; it must propose an amendment that passes validation. That is the point.

## Alternatives considered
- **LLM orchestrator agent** (plans, delegates, judges): rejected — reproduces the MAST failure classes and makes termination unverifiable.
- **Pure static workflow** (no planner): rejected for the MVP goal — dynamic decomposition is one of the things under test — but retained as the M1 substrate and as configs A/B.
