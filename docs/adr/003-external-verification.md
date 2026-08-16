# 003 — Fixed external verifier; verification is a stage, never a task
Status: Accepted
Date: 2026-08-16

## Context
MAST identifies inadequate/incorrect verification and premature termination as a core failure class. Agent self-assessment ("it works") and agent agreement are not evidence. If agents author the tests that judge them, they author weak tests. An earlier draft of our design had "T7 Verification" as a planner-generated task — that puts the verdict inside the thing being verified.

## Decision
1. **The acceptance suite is written by humans before the run**, lives in `acceptance/<benchmark>/`, and is mounted **read-only** into every worker. Agents may write as many of their own tests as they like; those are advisory and never determine the verdict.
2. **Verification is an orchestrator stage, not a DAG task.** The planner cannot create, move, or remove it. It runs when the integration task completes.
3. **The verifier is deterministic code** (`mas/verifier/`). It checks out the integration commit into a clean environment (ephemeral container; isolated subprocess acceptable in early steps), mounts the suite read-only, runs it, and records a `verification` artifact plus a `verify.passed|failed` event with the full report.
4. **Only the verifier sets PASS.** PASS → run `PASSED`, integration artifact `accepted`. FAIL → bounded re-plan (with the report as input) or run `FAILED`.
5. **Verification never calls a model.**

## Consequences
- "Done" is unambiguous and external. Overfitting to a visible suite is acceptable at MVP scale — the suite is effectively the spec.
- The same verifier and suite are used for every configuration (A–D), which is what makes the comparison fair.
- The suite must exist before benchmarking begins; it is part of the benchmark definition, not something produced during the run.

## Alternatives considered
- **LLM reviewer as verifier:** rejected as the deciding authority; may be added later as an advisory pre-check on the verification ladder.
- **Agent-authored acceptance tests:** rejected — circular.
- **Verification as the last DAG task:** rejected — planner-controlled, agent-executed, and therefore not external.
