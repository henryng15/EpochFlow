-- EpochFlow invariant checker. One read-only statement, so it sees a single
-- consistent snapshot and is safe to run against a live system:
--   psql "$DSN" -X -q -tA -F '|' -f scripts/check-invariants.sql
-- Prints one line per violation as "invariant|subject|detail"; no output means
-- every check passed. Invariant ids refer to docs/invariants.md. The checks are
-- read-only.

WITH
finishing AS (
  -- Attempts that decided the job's outcome (as opposed to REQUEUED or
  -- LEASE_EXPIRED, after which the job was retried).
  SELECT * FROM job_attempts WHERE outcome IN ('COMPLETED', 'FAILED', 'UNKNOWN')
),
attempt_summary AS (
  SELECT job_id, count(*) AS n, max(generation) AS max_generation
  FROM job_attempts GROUP BY job_id
),
violations AS (
  -- I2: every assignment left exactly one attempt row, numbered 1..generation.
  SELECT 'I2 attempts match generation' AS invariant, 'job ' || j.job_id AS subject,
         format('generation=%s attempts=%s attempt_rows=%s max_attempt_generation=%s',
                j.generation, j.attempts, coalesce(s.n, 0), s.max_generation) AS detail
  FROM jobs j LEFT JOIN attempt_summary s USING (job_id)
  WHERE coalesce(s.n, 0) <> j.attempts
     OR coalesce(s.n, 0) <> j.generation
     OR coalesce(s.max_generation, 0) <> j.generation

  UNION ALL
  -- I2: only the current assignment of a RUNNING job is open.
  SELECT 'I2 single open attempt', 'job ' || a.job_id,
         format('open attempt generation=%s, job state=%s generation=%s',
                a.generation, j.state, j.generation)
  FROM job_attempts a JOIN jobs j USING (job_id)
  WHERE a.outcome IS NULL AND NOT (j.state = 'RUNNING' AND j.generation = a.generation)

  UNION ALL
  SELECT 'I2 running job has open attempt', 'job ' || j.job_id,
         format('generation=%s', j.generation)
  FROM jobs j
  WHERE j.state = 'RUNNING' AND NOT EXISTS (
    SELECT 1 FROM job_attempts a
    WHERE a.job_id = j.job_id AND a.generation = j.generation AND a.outcome IS NULL)

  UNION ALL
  -- I3: at most one attempt decided the outcome, and it is the current one.
  SELECT 'I3 one deciding attempt', 'job ' || job_id,
         format('%s deciding attempts', count(*))
  FROM finishing GROUP BY job_id HAVING count(*) > 1

  UNION ALL
  SELECT 'I3 outcome from current generation', 'job ' || f.job_id,
         format('attempt generation=%s outcome=%s, job generation=%s state=%s',
                f.generation, f.outcome, j.generation, j.state)
  FROM finishing f JOIN jobs j USING (job_id)
  WHERE f.generation <> j.generation

  UNION ALL
  -- I3/I5: a finished or parked job agrees with its deciding attempt.
  SELECT 'I3 state matches deciding attempt', 'job ' || j.job_id,
         format('state=%s deciding outcome=%s', j.state, f.outcome)
  FROM jobs j LEFT JOIN finishing f ON f.job_id = j.job_id AND f.generation = j.generation
  WHERE (j.state = 'UNKNOWN' AND f.outcome IS DISTINCT FROM 'UNKNOWN')
     -- COMPLETED: reported, or reconciled from UNKNOWN.
     OR (j.state = 'COMPLETED' AND f.outcome NOT IN ('COMPLETED', 'UNKNOWN'))
     OR (j.state = 'COMPLETED' AND f.outcome IS NULL)
     -- FAILED: reported, reconciled from UNKNOWN, or the last lease expired.
     OR (j.state = 'FAILED' AND f.outcome IS NULL AND NOT EXISTS (
           SELECT 1 FROM job_attempts a WHERE a.job_id = j.job_id
             AND a.generation = j.generation AND a.outcome = 'LEASE_EXPIRED'))
     OR (j.state = 'FAILED' AND f.outcome = 'COMPLETED')

  UNION ALL
  -- I4: every ledger operation produced exactly one entry (UNIQUE prevents more).
  SELECT 'I4 ledger operation has entry', 'operation ' || o.operation_id, 'no ledger entry'
  FROM operations o
  WHERE o.kind = 'LEDGER'
    AND NOT EXISTS (SELECT 1 FROM ledger_entries l WHERE l.operation_id = o.operation_id)

  UNION ALL
  -- I3/I4: the effect was applied by an attempt that existed.
  SELECT 'I4 effect applied by a real assignment', 'operation ' || l.operation_id,
         format('job=%s generation=%s', l.job_id, l.generation)
  FROM ledger_entries l
  WHERE NOT EXISTS (SELECT 1 FROM job_attempts a
                    WHERE a.job_id = l.job_id AND a.generation = l.generation)

  UNION ALL
  -- Liveness: a lease expired long ago but the job was never reclaimed.
  SELECT 'recovery stalled', 'job ' || job_id,
         format('lease expired at %s', lease_until)
  FROM jobs WHERE state = 'RUNNING' AND lease_until < now() - interval '1 minute'
)
SELECT invariant, subject, detail FROM violations ORDER BY invariant, subject;
