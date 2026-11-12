-- Job state, leases, and fencing generations. See docs/storage.md.
--
-- Every state change is a single conditional UPDATE issued by PgJobStore
-- (src/store/pg_job_store.cpp); the constraints below are a second line of
-- defense against rows that no valid transition can produce.

CREATE TABLE jobs (
    job_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state        TEXT        NOT NULL DEFAULT 'PENDING'
                 CHECK (state IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'UNKNOWN')),
    -- Fencing token: incremented by every acquire, never decreased.
    generation   BIGINT      NOT NULL DEFAULT 0 CHECK (generation >= 0),
    -- Lease holder and deadline; present exactly while RUNNING.
    worker_id    TEXT,
    lease_until  TIMESTAMPTZ,
    attempts     INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER     NOT NULL CHECK (max_attempts >= 1),
    payload      BYTEA       NOT NULL,
    -- Set when COMPLETED; never changed afterwards.
    result       BYTEA,
    -- Most recent failure or recovery reason.
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT lease_iff_running
        CHECK ((state = 'RUNNING') = (worker_id IS NOT NULL AND lease_until IS NOT NULL)),
    CONSTRAINT attempts_within_limit CHECK (attempts <= max_attempts),
    CONSTRAINT result_iff_completed CHECK ((state = 'COMPLETED') = (result IS NOT NULL)),
    -- Every RUNNING or finished job was acquired at least once.
    CONSTRAINT acquired_before_running
        CHECK (state = 'PENDING' OR generation >= 1)
);

-- Acquire: oldest PENDING job.
CREATE INDEX jobs_pending_idx ON jobs (job_id) WHERE state = 'PENDING';
-- Reclaim: RUNNING jobs by deadline.
CREATE INDEX jobs_running_lease_idx ON jobs (lease_until) WHERE state = 'RUNNING';
