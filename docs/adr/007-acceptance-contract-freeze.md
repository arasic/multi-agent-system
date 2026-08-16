# 007 — Acceptance Contract Freeze: an approved, machine-checkable definition of done for ad-hoc goals
Status: Accepted (adapters implemented 2026-08-16: `mas/verifier/adapters/`, four criterion types, trusted runner in the verifier image; freeze/approval flow arrives with the planner, step 11)
Date: 2026-08-16

## Context
ADR-003 makes verification external: a fixed acceptance suite written *before* the run, read-only to workers, run by deterministic code. That works for benchmarks, where humans author the suite up front. For **ad-hoc goals** ("build me a ticketing system") nobody has written a suite yet. The obvious shortcut — let the planner write the tests that judge its own work, then have a human "approve" them — reintroduces the circular-verification failure (MAST 3.3; antipatterns C3): rubber-stamped, planner-authored tests are still planner-authored tests.

## Decision
1. **The planner proposes an *acceptance contract*, not tests.** A structured manifest (typed JSON/YAML, validated), containing:
   - `requirements` — behavioural statements in plain language
   - `acceptance` — concrete, checkable criteria (endpoints/status codes, persistence, build success, …)
   - `quality` — non-functional thresholds (`tests_required`, `max_critical_vulnerabilities`, timeouts, …)
   - `assumptions` — what was assumed instead of asked (ADR-006)
   - `exclusions` — explicitly out of scope
2. **The human approves or edits the manifest once**, up front. That is the one meaningful approval: *"here is what I understood, and here is how success will be judged."* After it, execution is unattended.
3. **The approved manifest is hashed and frozen** as an immutable artifact (`type=acceptance_contract`, `meta.sha256`); the run records the hash; changing it after approval is impossible (artifact immutability, I-5).
4. **Executable checks never come from the planner.** They come from one of: (a) a **trusted deterministic adapter** that maps manifest criteria to checks (e.g. `http_status(POST /tickets) == 201`, `build_succeeds`, `restart_persists`), (b) an existing **acceptance pack / test template** for known application shapes, or (c) a **human-owned suite**. Workers cannot modify the manifest, the adapters, or the suite (`acceptance/` read-only, tool policy forbids `acceptance_write`).
5. **The verifier runs the checks in a clean environment with a hard timeout**, records the report as a `verification` artifact, and alone decides PASS/FAIL (ADR-003 unchanged). Unmappable criteria make the manifest **invalid at approval time** — fail closed, never "skipped".
6. **Template shapes may skip approval later** (a trusted acceptance pack already defines success); genuinely arbitrary systems always need an external definition of success.

Example manifest:

```yaml
requirements:
  - users can create and assign tickets
  - only admins can manage users
acceptance:
  - POST /tickets returns 201
  - unauthorized admin requests return 403
  - database state survives restart
  - frontend production build succeeds
quality:
  tests_required: true
  max_critical_vulnerabilities: 0
assumptions:
  - single organization
exclusions:
  - email notifications
```

## Consequences
- "Verified" keeps its meaning for ad-hoc goals: the definition of done is external (human-approved) and its executable form is trusted code, not model output.
- One approval per goal replaces supervising the build; the run then goes to a verdict unattended (I-3, I-4).
- The adapter library becomes a real component (step 7+): each criterion type is a small, tested, deterministic check. Coverage grows over time; anything uncovered is rejected at approval, visibly.
- Honest scope: the product promise is *"give it a bounded software goal with an approved, machine-checkable definition of done, and it can decompose, build, integrate, repair and verify the result with parallel workers."* It produces a **verified repository**. Deploying and operating production is out of scope (tool policy forbids deploy/network side effects; would need secrets, migrations, observability, rollback and approval gates — none of which exist here).

## Alternatives considered
- **Planner writes the acceptance tests; human approves them:** rejected — circular verification with a rubber stamp.
- **No approval; the planner's own criteria are the contract:** rejected — nothing external decides success.
- **Human writes the whole suite for every ad-hoc goal:** correct but expensive; retained as option (c) and as the benchmark method.
