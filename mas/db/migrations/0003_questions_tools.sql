-- 0003_questions_tools: clarifying questions (ADR-006) and per-task tool allow-lists (validator rule 4).

ALTER TYPE run_status ADD VALUE IF NOT EXISTS 'AWAITING_INPUT' AFTER 'PLANNING';

ALTER TABLE runs
    ADD COLUMN max_questions   int NOT NULL DEFAULT 3,   -- budget: how many question batches the planner may ask
    ADD COLUMN questions_asked int NOT NULL DEFAULT 0;

-- Tools a task's agent may use; the validator fills/validates this from the capability→tools registry.
ALTER TABLE tasks
    ADD COLUMN tools jsonb NOT NULL DEFAULT '[]'::jsonb;
