# 005 — Postgres as blackboard and coordination mechanism; no queue
Status: Accepted
Date: 2026-08-16

## Context
The MVP needs a shared, durable store for runs/tasks/attempts/artifacts/events (the blackboard) and a mechanism for workers to claim tasks safely under concurrency and survive worker death. Adding a message queue (Kafka, Redis Streams, RabbitMQ) at this stage adds an operational component and a second source of truth before we know the coordination patterns we actually need.

## Decision
- **PostgreSQL is the single source of truth and the coordination mechanism.**
- Task claiming: `SELECT … FROM tasks WHERE status='READY' AND capability = ANY($caps) FOR UPDATE SKIP LOCKED LIMIT 1`, then transition to `RUNNING` and insert the attempt — all in one transaction.
- Liveness: attempts carry `lease_until`; workers heartbeat to extend; the orchestrator's reaper reclaims expired leases (`ABANDONED`).
- Workers poll (short interval or `LISTEN/NOTIFY` as an optimisation later). No broker.
- Events are appended in the same transaction as the transition they record.

## Consequences
- One stateful component. Everything is queryable with SQL; `mas replay` is a query.
- Throughput is bounded by Postgres, which is far beyond MVP needs (tens of tasks, a handful of workers).
- Exactly-once semantics come from transactions + attempt versioning, not from broker guarantees.
- If a broker is ever needed, the state machine and events model don't change — only the wake-up mechanism.

## Alternatives considered
- **Kafka/Redpanda:** rejected for MVP — no need, and it would obscure whether coordination failures come from our design or from broker semantics.
- **Redis queue:** rejected — introduces a second store whose contents must be reconciled with Postgres.
- **Temporal / durable-execution engines:** attractive later; rejected now because it hides the very state machine we want to prove and test explicitly.
