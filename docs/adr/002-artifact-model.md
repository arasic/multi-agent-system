# 002 — Immutable, attempt-versioned artifacts; conflict as competing candidates
Status: Accepted
Date: 2026-08-16

## Context
Agents coordinating through long conversations suffer context growth, information distortion, duplication, hidden dependencies, poor auditability, and poor recovery. Coding tasks add a concrete problem: outputs are files, and several concurrent workers cannot share one checkout. Retries need reproducibility: a second attempt must not silently inherit a dead attempt's half-finished state.

## Decision
1. **Coordination is through artifacts, not messages.** Workers publish artifacts; downstream tasks read the accepted artifacts named in their `context_spec`. There is no inter-agent messaging primitive (invariant I-8).
2. **One git worktree per attempt**, branch `run/<run>/<task>/<attempt>`, created from `runs.base_ref` (or the current integration branch). Workers write only inside it. `main` is never touched; `acceptance/` is read-only.
3. **Artifact = immutable row + immutable ref.** For code, `type=git_commit, ref=<sha>`. For documents/decisions, a committed file: `ref=<sha>:<path>`. Content and ref never change; only `status` (`candidate → accepted | superseded | rejected`) and `superseded_by`.
4. **Attempt-versioned.** A retry is a new attempt from a clean worktree. It may *read* the previous attempt's candidate artifacts as hints (listed in context) but does not inherit them. Only `accepted` artifacts are authoritative inputs.
5. **Integration is a task** with capability `integration`, the unique sink of every DAG. It merges chosen candidate commits into `run/<run>/integration` and publishes a `git_commit` candidate that the verifier then judges.
6. **Conflict representation.** Two `candidate` artifacts for the same output slot are a conflict. Resolution produces a `decision` artifact naming winner, losers, and rationale; losers become `superseded` or `rejected`. Disagreement is never averaged away and never deleted (invariant I-10).

## Consequences
- Worker death is survivable and reproducible: artifacts persist, retries start clean.
- The audit trail is the store: every output is attributable to run/task/attempt/model.
- Git does the heavy lifting for code; Postgres holds pointers and metadata, not source blobs.
- Merging is real work and may need an LLM — hence integration is a task, not orchestrator code.
- Cost: worktree management and a shared repo volume (or bare repo + push) must be built at roadmap step 6.

## Alternatives considered
- **Shared checkout with file locks:** rejected — serialises the parallelism we're testing and makes death recovery messy.
- **Store file blobs in Postgres:** rejected for code — loses git history/merge tooling; acceptable later for small documents if needed.
- **Mutable artifacts (agents "update" their output):** rejected — breaks reproducibility of retries and auditability.
- **A claims table for conflicts now:** deferred — see ADR-004.
