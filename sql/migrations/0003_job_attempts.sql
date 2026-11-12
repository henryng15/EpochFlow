-- One row per assignment: the execution trace that explains retries,
-- recoveries, and which generation produced a job's outcome. Written by
-- PgJobStore in the same transaction as the job change it describes.
-- Not backfilled: jobs assigned before this migration have no rows.
CREATE TABLE job_attempts (
    job_id      BIGINT      NOT NULL REFERENCES jobs (job_id),
    generation  BIGINT      NOT NULL CHECK (generation >= 1),
    worker_id   TEXT        NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    -- How the attempt ended; NULL while it holds the job.
    --   COMPLETED      reported success
    --   FAILED         reported a final failure (or the last allowed attempt failed)
    --   REQUEUED       reported a retryable failure with attempts left
    --   LEASE_EXPIRED  stopped renewing; recovered by reclaim
    --   UNKNOWN        reported that an external outcome cannot be determined
    outcome     TEXT CHECK (outcome IN ('COMPLETED', 'FAILED', 'REQUEUED', 'LEASE_EXPIRED', 'UNKNOWN')),
    PRIMARY KEY (job_id, generation),
    CONSTRAINT finished_iff_outcome CHECK ((outcome IS NULL) = (finished_at IS NULL))
);
