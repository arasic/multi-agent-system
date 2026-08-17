-- 0006_attempt_tokens: the per-attempt token allocation becomes a run budget (docs/architecture.md §9 lists it among
-- the run's hard limits; it used to be a worker-side setting). The meter hands every attempt
-- min(max_attempt_tokens, tokens the run has left); validator rule 8 admits a plan only if the run can still fund one
-- such attempt for every open task (a deterministic budget-allocation check, independent of planner estimates).

ALTER TABLE runs ADD COLUMN IF NOT EXISTS max_attempt_tokens int NOT NULL DEFAULT 200000;
