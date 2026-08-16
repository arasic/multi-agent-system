# Architecture Decision Records

Design changes go through an ADR. Nothing about the design lives only in chat.

| # | Title | Status |
|---|---|---|
| [001](001-planner-orchestrator.md) | Planner ≠ Orchestrator | Accepted |
| [002](002-artifact-model.md) | Immutable, attempt-versioned artifacts; conflict as competing candidates | Accepted |
| [003](003-external-verification.md) | Fixed external verifier; verification is a stage, never a task | Accepted |
| [004](004-claims-deferred.md) | No claims/evidence tables in the MVP | Accepted |
| [005](005-postgres-coordination.md) | Postgres as blackboard and coordination mechanism; no queue | Accepted |

## Adding one

1. Copy the format below into `NNN-short-title.md` (next number).
2. Status starts as **Proposed**; becomes **Accepted** when merged, **Superseded by NNN** if later replaced.
3. Update `architecture.md` / `invariants.md` / `evaluation.md` in the same change.

```
# NNN — Title
Status: Proposed | Accepted | Superseded by NNN
Date: YYYY-MM-DD

## Context
## Decision
## Consequences
## Alternatives considered
```
