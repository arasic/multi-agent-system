-- 0001_initial: the six MVP tables (docs/architecture.md §3) + immutability trigger for artifacts.

CREATE TYPE run_status      AS ENUM ('CREATED','PLANNING','RUNNING','VERIFYING','REPLANNING','PASSED','FAILED','ABORTED');
CREATE TYPE task_status     AS ENUM ('PENDING','READY','RUNNING','RETRYABLE','COMPLETED','FAILED','BLOCKED','CANCELLED');
CREATE TYPE attempt_status  AS ENUM ('RUNNING','SUCCESS','FAILED','TIMEOUT','ABANDONED','CANCELLED');
CREATE TYPE artifact_status AS ENUM ('candidate','accepted','superseded','rejected');

CREATE TABLE runs (
    id                     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    goal                   text NOT NULL,
    benchmark              text,
    config                 text,
    status                 run_status NOT NULL DEFAULT 'CREATED',
    base_ref               text,
    -- budgets (hard limits, enforced by the orchestrator)
    max_concurrency        int  NOT NULL DEFAULT 4,
    max_tasks              int  NOT NULL DEFAULT 50,
    max_attempts_per_task  int  NOT NULL DEFAULT 3,
    max_replans            int  NOT NULL DEFAULT 2,
    max_plan_attempts      int  NOT NULL DEFAULT 3,
    max_tokens             bigint NOT NULL DEFAULT 2000000,
    max_cost_usd           numeric(12,4) NOT NULL DEFAULT 20,
    max_wallclock_s        int  NOT NULL DEFAULT 3600,
    max_attempt_runtime_s  int  NOT NULL DEFAULT 600,
    lease_s                int  NOT NULL DEFAULT 30,
    deadline_at            timestamptz,
    -- usage
    tokens_used            bigint NOT NULL DEFAULT 0,
    cost_used_usd          numeric(12,4) NOT NULL DEFAULT 0,
    replans_used           int  NOT NULL DEFAULT 0,
    tasks_created          int  NOT NULL DEFAULT 0,
    -- outcome
    verdict                text,
    created_at             timestamptz NOT NULL DEFAULT now(),
    started_at             timestamptz,
    finished_at            timestamptz
);

CREATE TABLE tasks (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id           uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    key              text NOT NULL,                       -- planner-facing id: T1, T2 ...
    goal             text NOT NULL,
    capability       text NOT NULL,
    status           task_status NOT NULL DEFAULT 'PENDING',
    input_contract   jsonb NOT NULL DEFAULT '{}'::jsonb,
    output_contract  jsonb NOT NULL DEFAULT '{}'::jsonb,
    context_spec     jsonb NOT NULL DEFAULT '{}'::jsonb,
    meta             jsonb NOT NULL DEFAULT '{}'::jsonb,   -- free-form (e.g. stub-agent script); never load-bearing
    max_attempts     int  NOT NULL DEFAULT 3,
    created_by       text NOT NULL DEFAULT 'planner',      -- planner | replan:<n> | system
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, key)
);
CREATE INDEX tasks_run_status_idx ON tasks (run_id, status);
CREATE INDEX tasks_ready_idx      ON tasks (status, capability) WHERE status = 'READY';

CREATE TABLE task_dependencies (
    task_id             uuid NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on_task_id  uuid NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, depends_on_task_id),
    CHECK (task_id <> depends_on_task_id)
);
CREATE INDEX task_dependencies_upstream_idx ON task_dependencies (depends_on_task_id);

CREATE TABLE attempts (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id         uuid NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_number  int  NOT NULL,
    status          attempt_status NOT NULL DEFAULT 'RUNNING',
    worker_id       text,
    lease_until     timestamptz,
    workspace_ref   text,
    model           text,
    input_tokens    bigint NOT NULL DEFAULT 0,
    output_tokens   bigint NOT NULL DEFAULT 0,
    cost_usd        numeric(12,6) NOT NULL DEFAULT 0,
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    failure_reason  text,
    UNIQUE (task_id, attempt_number)
);
CREATE INDEX attempts_running_lease_idx ON attempts (lease_until) WHERE status = 'RUNNING';
CREATE INDEX attempts_task_idx ON attempts (task_id);

CREATE TABLE artifacts (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id         uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_id        uuid REFERENCES tasks(id) ON DELETE CASCADE,        -- null for run-level (verification)
    attempt_id     uuid REFERENCES attempts(id) ON DELETE CASCADE,
    type           text NOT NULL,                                       -- git_commit | document | decision | verification | log
    ref            text NOT NULL,                                       -- commit sha, sha:path, or opaque key
    status         artifact_status NOT NULL DEFAULT 'candidate',
    superseded_by  uuid REFERENCES artifacts(id),
    meta           jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX artifacts_run_idx  ON artifacts (run_id);
CREATE INDEX artifacts_task_idx ON artifacts (task_id);

-- Invariant I-5: artifact content is immutable. Only status / superseded_by may change; rows are never deleted.
CREATE OR REPLACE FUNCTION artifacts_immutable() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'artifacts are immutable: DELETE not allowed (id=%)', OLD.id;
    END IF;
    IF NEW.run_id     IS DISTINCT FROM OLD.run_id
    OR NEW.task_id    IS DISTINCT FROM OLD.task_id
    OR NEW.attempt_id IS DISTINCT FROM OLD.attempt_id
    OR NEW.type       IS DISTINCT FROM OLD.type
    OR NEW.ref        IS DISTINCT FROM OLD.ref
    OR NEW.meta       IS DISTINCT FROM OLD.meta
    OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'artifacts are immutable: only status/superseded_by may change (id=%)', OLD.id;
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER artifacts_immutable_upd BEFORE UPDATE ON artifacts FOR EACH ROW EXECUTE FUNCTION artifacts_immutable();
CREATE TRIGGER artifacts_immutable_del BEFORE DELETE ON artifacts FOR EACH ROW EXECUTE FUNCTION artifacts_immutable();

CREATE TABLE events (
    id          bigserial PRIMARY KEY,
    run_id      uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_id     uuid,
    attempt_id  uuid,
    worker_id   text,
    type        text NOT NULL,
    payload     jsonb NOT NULL DEFAULT '{}'::jsonb,
    ts          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX events_run_idx ON events (run_id, id);
