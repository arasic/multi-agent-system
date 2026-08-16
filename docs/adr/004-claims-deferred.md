# 004 — No claims/evidence tables in the MVP
Status: Accepted
Date: 2026-08-16

## Context
Earlier design rounds listed `Claim` and `Evidence` among the core nouns and described a claim/conflict engine (claim A conflicts with claim B → criteria → decision). That is right for the Autonomous SOC, where agents make claims *about the world* ("this was lateral movement") that must be backed by evidence and reconciled. In the MVP's software-construction benchmarks, however, claims arise rarely and unnaturally: outputs are code and design documents, not assertions about external reality. Building the abstraction now would be building it before it is used — the "generic framework" trap. At the same time, "agents can disagree without corrupting state" is one of the architecture pass criteria (A7) and must not silently drop out.

## Decision
- **No `claims`, `evidence`, or `decisions` tables in the MVP.**
- Conflict is represented with the artifact model already in place (ADR-002): two `candidate` artifacts for the same slot *are* a conflict; a `decision` artifact (type `decision`, committed file + rationale) resolves it; losers become `superseded`/`rejected`.
- The forced-disagreement demo (two `architecture` tasks answering the same design question) is retained in the evaluation and satisfies A7 through this mechanism.
- Claims/evidence become first-class **when the SOC application needs them** — at that point add tables for claim (subject, predicate, value, task/attempt, evidence artifact ids, confidence) and conflict detection on (subject, predicate), and write an ADR superseding this one.

## Consequences
- The MVP stays at ten nouns. The schema stays at six tables.
- Conflict resolution logic in the MVP is minimal (a decision artifact produced by an integration/review task); no reviewer panels, no confidence arithmetic.
- The SOC will require this ADR to be superseded — that is expected and is the moment the runtime is tested for reusability.

## Alternatives considered
- **Minimal claims table now (5 columns):** cheap, but unearned by the coding benchmark and one more thing to keep coherent. Deferred.
- **Drop conflict handling from the pass criteria:** rejected — it is one of the things that distinguishes a MAS from parallel task execution.
