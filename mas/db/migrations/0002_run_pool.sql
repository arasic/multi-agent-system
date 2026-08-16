-- 0002_run_pool: runs belong to a pool; workers/orchestrators serve only their pool(s).
-- Lets an in-process `mas run` (pool local:<pid>) coexist with the compose services (pool default) on one database.

ALTER TABLE runs ADD COLUMN pool text NOT NULL DEFAULT 'default';
CREATE INDEX runs_pool_status_idx ON runs (pool, status);
