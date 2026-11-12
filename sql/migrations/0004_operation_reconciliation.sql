-- Operation-level reconciliation (ADR 0006). See docs/storage.md.
--
-- An operator can now resolve an external operation together with its UNKNOWN
-- job: CONFIRMED with the provider evidence, or FAILED when the provider never
-- performed the effect. FAILED is terminal and only an operator sets it.
-- Existing rows satisfy every new constraint.

ALTER TABLE operations DROP CONSTRAINT operations_status_check;
ALTER TABLE operations DROP CONSTRAINT result_iff_confirmed;

-- When an operator decision resolved the operation (NULL: decided by a worker
-- or not yet resolved).
ALTER TABLE operations ADD COLUMN reconciled_at TIMESTAMPTZ;

ALTER TABLE operations
    ADD CONSTRAINT operations_status_check
        CHECK (status IN ('APPLIED', 'STARTED', 'CONFIRMED', 'FAILED')),
    -- `result` is the provider confirmation (CONFIRMED) or the operator's
    -- evidence that nothing happened (FAILED).
    ADD CONSTRAINT result_iff_resolved
        CHECK ((status IN ('CONFIRMED', 'FAILED')) = (result IS NOT NULL)),
    ADD CONSTRAINT failed_is_reconciled
        CHECK (status <> 'FAILED' OR reconciled_at IS NOT NULL);
