-- 0008_reservations: the hard total token budget (I-4) as a reservation, and planner usage charged to the run.
-- attempts.token_allocation: what the claim reserved for the attempt = min(run.max_attempt_tokens, worker ceiling,
--   max_tokens - tokens_used - SUM(allocations of RUNNING attempts)). Reserved = the RUNNING attempts' allocations, so
--   nothing has to be released (settlement moves an attempt out of RUNNING and its real usage into tokens_used).
--   Concurrent attempts can no longer each be handed "everything that is left"; overshoot stays <= one call/attempt.
-- model_calls.settled: planner (attempt-less) calls are settled into runs.tokens_used / cost_used_usd by the driver
--   after every planning round; worker calls are settled through their attempt as before.
ALTER TABLE attempts ADD COLUMN IF NOT EXISTS token_allocation bigint;
ALTER TABLE model_calls ADD COLUMN IF NOT EXISTS settled boolean NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS model_calls_unsettled_idx ON model_calls (run_id) WHERE NOT settled AND attempt_id IS NULL;
