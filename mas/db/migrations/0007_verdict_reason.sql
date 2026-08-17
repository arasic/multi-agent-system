-- 0007_verdict_reason: terminal causes are verdict *reason codes*, not new run states (ADR-008 §6). A non-passing
-- terminal run records exactly one of the six codes; PASSED leaves it NULL. `runs.verdict` keeps the human-readable
-- text. Written only by mas/orchestrator/state_machine.py (fail_run / abort_run), like every status column.

ALTER TABLE runs ADD COLUMN IF NOT EXISTS verdict_reason text
    CHECK (verdict_reason IS NULL OR verdict_reason IN
        ('BUDGET_EXHAUSTED', 'NO_PROGRESS', 'UNSUPPORTED', 'POLICY_DENIED', 'INVALID_PLAN', 'UNRECOVERABLE_FAILURE'));
