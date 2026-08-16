-- 0004_model_calls: per-call model telemetry (roadmap step 9, docs/architecture.md §11).
-- One row per model call, written by the metered provider as the call finishes (own transaction), so the evidence
-- survives a worker that dies mid-attempt. attempts.*_tokens/cost_usd remain the settlement summary; runs.tokens_used /
-- cost_used_usd remain what budgets are enforced on. Append-only by convention (like events).

CREATE TABLE model_calls (
    id                 bigserial PRIMARY KEY,
    run_id             uuid REFERENCES runs(id) ON DELETE CASCADE,       -- NULL only for out-of-run calls (mas models --ping)
    task_id            uuid REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id         uuid REFERENCES attempts(id) ON DELETE CASCADE,   -- NULL for planner / reviewer calls
    role               text NOT NULL,                                     -- planner | worker | reviewer | ping
    provider           text NOT NULL,
    model              text NOT NULL,
    seq                int  NOT NULL,                                     -- call index within the attempt / planning round
    started_at         timestamptz NOT NULL,
    duration_ms        int  NOT NULL,
    input_tokens       bigint NOT NULL DEFAULT 0,
    output_tokens      bigint NOT NULL DEFAULT 0,
    cache_read_tokens  bigint NOT NULL DEFAULT 0,
    cache_write_tokens bigint NOT NULL DEFAULT 0,
    cost_usd           numeric(12,6) NOT NULL DEFAULT 0,
    priced             boolean NOT NULL DEFAULT false,                    -- false: no price configured → cost understated
    status             text NOT NULL,                                     -- ok | max_tokens | refusal | error
    stop_reason        text,
    error              text,
    request_id         text,
    meta               jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX model_calls_run_idx     ON model_calls (run_id, id);
CREATE INDEX model_calls_attempt_idx ON model_calls (attempt_id, seq);
