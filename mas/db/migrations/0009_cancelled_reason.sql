-- 0009_cancelled_reason: ADR-009 adds one verdict reason code, CANCELLED — an operator ended the run deliberately,
-- so it says nothing about the system under test (today only `mas plan`, whose run exports a validated plan and is
-- never executed). Still written only by mas/orchestrator/state_machine.py.

ALTER TABLE runs DROP CONSTRAINT IF EXISTS runs_verdict_reason_check;
ALTER TABLE runs ADD CONSTRAINT runs_verdict_reason_check CHECK (
    verdict_reason IS NULL OR verdict_reason IN
        ('BUDGET_EXHAUSTED', 'NO_PROGRESS', 'UNSUPPORTED', 'POLICY_DENIED', 'INVALID_PLAN',
         'UNRECOVERABLE_FAILURE', 'CANCELLED'));
