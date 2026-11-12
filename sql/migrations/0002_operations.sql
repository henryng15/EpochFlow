-- Idempotent operations and the demonstration ledger. See docs/storage.md.

-- One row per logical side effect, keyed by a caller-chosen operation id that
-- stays the same across retries and attempts (invariant I4).
--   LEDGER:   an effect EpochFlow owns; created together with its ledger entry
--             in one transaction (status APPLIED).
--   EXTERNAL: an effect in a system without idempotency keys; recorded as an
--             intent (STARTED) before the call and CONFIRMED with the
--             provider's answer afterwards.
CREATE TABLE operations (
    operation_id TEXT        PRIMARY KEY CHECK (operation_id <> ''),
    kind         TEXT        NOT NULL CHECK (kind IN ('LEDGER', 'EXTERNAL')),
    -- Canonical request content; reuse with different content is a conflict.
    request      TEXT        NOT NULL,
    status       TEXT        NOT NULL CHECK (status IN ('APPLIED', 'STARTED', 'CONFIRMED')),
    -- The assignment that created the operation.
    job_id       BIGINT      NOT NULL REFERENCES jobs (job_id),
    generation   BIGINT      NOT NULL CHECK (generation >= 1),
    -- Provider confirmation for CONFIRMED external operations.
    result       TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ledger_is_applied CHECK ((kind = 'LEDGER') = (status = 'APPLIED')),
    CONSTRAINT result_iff_confirmed CHECK ((status = 'CONFIRMED') = (result IS NOT NULL))
);

-- The owned side effect: one entry per LEDGER operation, enforced by UNIQUE.
CREATE TABLE ledger_entries (
    entry_id     BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation_id TEXT        NOT NULL UNIQUE REFERENCES operations (operation_id),
    account      TEXT        NOT NULL CHECK (account <> ''),
    amount_cents BIGINT      NOT NULL,
    -- The assignment that applied it.
    job_id       BIGINT      NOT NULL REFERENCES jobs (job_id),
    generation   BIGINT      NOT NULL CHECK (generation >= 1),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
