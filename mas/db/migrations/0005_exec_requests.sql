-- 0005_exec_requests: the execution-runner path (docs/architecture.md §10b).
-- Compose workers have no docker.sock. Their command tools submit bounded requests here; a trusted host-side runner
-- (`mas execute --watch`) claims them, derives the worktree from the ids itself (never from the worker), runs the
-- command in the attempt's sandbox container and writes back a bounded result. Postgres is the transport and the
-- coordination mechanism (SKIP LOCKED claims, leases), like everything else in this system.

CREATE TABLE exec_requests (
    id           bigserial PRIMARY KEY,
    run_id       uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_id      uuid NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id   uuid NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    worker_id    text NOT NULL,
    family       text NOT NULL,                     -- tool family the command needs (shell | python); checked against tasks.tools
    kind         text NOT NULL,                     -- shell | argv | close
    command      text,                              -- kind = shell (size-capped by the runner)
    argv         jsonb,                             -- kind = argv
    timeout_s    numeric(8,2) NOT NULL DEFAULT 60,  -- the worker's request; the runner clamps to the attempt deadline
    status       text NOT NULL DEFAULT 'pending',   -- pending | leased | done | error | cancelled | abandoned
    runner_id    text,
    lease_until  timestamptz,                       -- renewed by the runner while the command runs; expired lease = abandoned
    created_at   timestamptz NOT NULL DEFAULT now(),
    started_at   timestamptz,
    finished_at  timestamptz,
    result       jsonb,                             -- bounded: exit_code, flags, duration_s, output_sha256, output_bytes, error
    output       text,                              -- capped command output in transit to the worker; cleared once consumed
    consumed_at  timestamptz
);

CREATE INDEX exec_requests_pending_idx ON exec_requests (status, id);
CREATE INDEX exec_requests_attempt_idx ON exec_requests (attempt_id, id);

-- One sandbox session per attempt, owned by exactly one live runner (lease). Another runner may take over only
-- after the lease expired; a fresh container replaces the stale one (same name, `docker rm -f` first).
CREATE TABLE exec_sessions (
    attempt_id   uuid PRIMARY KEY REFERENCES attempts(id) ON DELETE CASCADE,
    runner_id    text NOT NULL,
    lease_until  timestamptz NOT NULL,
    container    text,
    image_id     text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
