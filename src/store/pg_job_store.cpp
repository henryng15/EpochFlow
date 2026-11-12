#include "store/pg_job_store.h"

#include "store/migrations.h"

#include <pqxx/pqxx>

#include <chrono>
#include <stdexcept>
#include <string>
#include <string_view>

namespace epochflow {
namespace {

// Timestamps cross the boundary as integer microseconds since the Unix epoch,
// computed by the database, so no time-zone or text parsing is involved.
constexpr std::string_view kJobColumns =
    "job_id, state, generation, worker_id, "
    "(extract(epoch FROM lease_until) * 1000000)::bigint AS lease_until_us, "
    "attempts, max_attempts, payload, result, error";

// Claims the oldest PENDING job. SKIP LOCKED makes concurrent claimers pick
// different rows instead of queueing on the same one; the outer UPDATE
// re-checks the state under the row lock.
constexpr std::string_view kAcquireSql = R"sql(
WITH next AS (
  SELECT job_id FROM jobs
  WHERE state = 'PENDING'
  ORDER BY job_id
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE jobs AS j
SET state = 'RUNNING',
    generation = j.generation + 1,
    attempts = j.attempts + 1,
    worker_id = $1,
    lease_until = now() + $2::bigint * interval '1 millisecond',
    updated_at = now()
FROM next
WHERE j.job_id = next.job_id AND j.state = 'PENDING'
RETURNING j.job_id, j.generation, j.payload,
          (extract(epoch FROM j.lease_until) * 1000000)::bigint, j.attempts
)sql";

// Fencing predicate shared by renew, complete, and fail: $1 job, $2 generation.
#define EPOCHFLOW_CURRENT_ASSIGNMENT "job_id = $1 AND generation = $2 AND state = 'RUNNING'"

constexpr std::string_view kRenewSql =
    "UPDATE jobs SET lease_until = now() + $3::bigint * interval '1 millisecond', "
    "updated_at = now() WHERE " EPOCHFLOW_CURRENT_ASSIGNMENT
    " RETURNING (extract(epoch FROM lease_until) * 1000000)::bigint";

constexpr std::string_view kCompleteSql =
    "UPDATE jobs SET state = 'COMPLETED', result = $3, worker_id = NULL, lease_until = NULL, "
    "updated_at = now() WHERE " EPOCHFLOW_CURRENT_ASSIGNMENT;

constexpr std::string_view kFailSql =
    "UPDATE jobs SET state = CASE WHEN $4 AND attempts < max_attempts "
    "                             THEN 'PENDING' ELSE 'FAILED' END, "
    "error = $3, worker_id = NULL, lease_until = NULL, updated_at = now() "
    "WHERE " EPOCHFLOW_CURRENT_ASSIGNMENT;

#undef EPOCHFLOW_CURRENT_ASSIGNMENT

// Strictly past deadlines, matching the in-memory store (a lease is still
// valid at its exact deadline).
constexpr std::string_view kReclaimSql = R"sql(
UPDATE jobs
SET state = CASE WHEN attempts < max_attempts THEN 'PENDING' ELSE 'FAILED' END,
    error = CASE WHEN attempts < max_attempts THEN 'lease expired'
                 ELSE 'lease expired; no attempts left' END,
    worker_id = NULL,
    lease_until = NULL,
    updated_at = now()
WHERE state = 'RUNNING' AND lease_until < now()
)sql";

TimePoint FromMicros(std::int64_t us) { return TimePoint{std::chrono::microseconds{us}}; }

std::string ToString(const pqxx::bytes& b) {
  return std::string(reinterpret_cast<const char*>(b.data()), b.size());
}

JobState ParseState(std::string_view s) {
  if (s == "PENDING") return JobState::kPending;
  if (s == "RUNNING") return JobState::kRunning;
  if (s == "COMPLETED") return JobState::kCompleted;
  if (s == "FAILED") return JobState::kFailed;
  if (s == "UNKNOWN") return JobState::kUnknown;
  throw std::logic_error("unexpected job state in database: " + std::string(s));
}

JobRecord ToRecord(const pqxx::row_ref& r) {
  JobRecord job;
  job.id = r["job_id"].as<JobId>();
  job.state = ParseState(r["state"].view());
  job.generation = r["generation"].as<Generation>();
  if (!r["worker_id"].is_null()) job.worker_id = r["worker_id"].as<std::string>();
  if (!r["lease_until_us"].is_null()) job.lease_until = FromMicros(r["lease_until_us"].as<std::int64_t>());
  job.attempts = r["attempts"].as<int>();
  job.max_attempts = r["max_attempts"].as<int>();
  job.payload = ToString(r["payload"].as<pqxx::bytes>());
  if (!r["result"].is_null()) job.result = ToString(r["result"].as<pqxx::bytes>());
  if (!r["error"].is_null()) job.error = r["error"].as<std::string>();
  return job;
}

void ValidateLease(Duration lease) {
  if (lease <= Duration::zero()) throw std::invalid_argument("lease must be positive");
}

// Explains why a fenced UPDATE matched no row. A finished job is immutable, so
// this follow-up read cannot race with a change that would alter the answer.
// `outcome_column` is "result" (complete) or "error" (fail); `final_state` is
// the state that request would have produced.
CommitStatus ClassifyRejected(pqxx::work& tx, JobId id, Generation generation,
                              std::string_view final_state, std::string_view outcome_column,
                              const std::string& outcome) {
  const pqxx::result rows = tx.exec(
      "SELECT state, generation, " + std::string(outcome_column) + " FROM jobs WHERE job_id = $1",
      pqxx::params{id});
  if (rows.empty()) return CommitStatus::kNotFound;
  const pqxx::row_ref r = rows[0];
  if (r[0].view() != final_state || r[1].as<Generation>() != generation) {
    return CommitStatus::kStale;
  }
  const bool same = outcome_column == "result" ? ToString(r[2].as<pqxx::bytes>()) == outcome
                                               : r[2].as<std::string>() == outcome;
  return same ? CommitStatus::kAccepted : CommitStatus::kConflict;
}

// Locks the job row in share mode if `id` is RUNNING with `generation`, which
// blocks reclaim, reassignment, and completion of the job until the caller's
// transaction ends. That makes "fencing check, then effect" atomic.
CommitStatus LockCurrentAssignment(pqxx::work& tx, JobId id, Generation generation) {
  const pqxx::result locked = tx.exec(
      "SELECT 1 FROM jobs WHERE job_id = $1 AND generation = $2 AND state = 'RUNNING' "
      "FOR SHARE",
      pqxx::params{id, generation});
  if (!locked.empty()) return CommitStatus::kAccepted;
  const bool exists = !tx.exec("SELECT 1 FROM jobs WHERE job_id = $1", pqxx::params{id}).empty();
  return exists ? CommitStatus::kStale : CommitStatus::kNotFound;
}

}  // namespace

PgJobStore::PgJobStore(const std::string& dsn, PgStoreOptions options)
    : pool_(dsn, options.pool_size) {
  auto conn = pool_.Acquire();  // throws pqxx::broken_connection if unreachable
  int version = 0;
  try {
    pqxx::work tx{*conn};
    version = tx.query_value<int>("SELECT coalesce(max(version), 0) FROM schema_migrations");
  } catch (const pqxx::sql_error&) {
    version = 0;  // schema_migrations missing
  }
  if (version != LatestMigrationVersion()) {
    throw std::runtime_error("database schema is at version " + std::to_string(version) +
                             ", expected " + std::to_string(LatestMigrationVersion()) +
                             "; run migrations first");
  }
}

JobId PgJobStore::Submit(const JobSpec& spec) {
  if (spec.max_attempts < 1) throw std::invalid_argument("max_attempts must be >= 1");
  auto conn = pool_.Acquire();
  pqxx::work tx{*conn};
  const auto id = tx.query_value<JobId>(
      "INSERT INTO jobs (max_attempts, payload) VALUES ($1, $2) RETURNING job_id",
      pqxx::params{spec.max_attempts, pqxx::binary_cast(spec.payload)});
  tx.commit();
  return id;
}

std::optional<JobRecord> PgJobStore::Get(JobId id) const {
  auto conn = pool_.Acquire();
  pqxx::read_transaction tx{*conn};
  const pqxx::result rows = tx.exec(
      "SELECT " + std::string(kJobColumns) + " FROM jobs WHERE job_id = $1", pqxx::params{id});
  if (rows.empty()) return std::nullopt;
  return ToRecord(rows[0]);
}

std::optional<Assignment> PgJobStore::Acquire(const WorkerId& worker, Duration lease) {
  ValidateLease(lease);
  auto conn = pool_.Acquire();
  pqxx::work tx{*conn};
  const pqxx::result rows = tx.exec(kAcquireSql, pqxx::params{worker, lease.count()});
  tx.commit();
  if (rows.empty()) return std::nullopt;
  const pqxx::row_ref r = rows[0];
  return Assignment{r[0].as<JobId>(), r[1].as<Generation>(), ToString(r[2].as<pqxx::bytes>()),
                    FromMicros(r[3].as<std::int64_t>()), lease, r[4].as<int>()};
}

RenewResult PgJobStore::Renew(JobId id, Generation generation, Duration lease) {
  ValidateLease(lease);
  auto conn = pool_.Acquire();
  pqxx::work tx{*conn};
  const pqxx::result rows = tx.exec(kRenewSql, pqxx::params{id, generation, lease.count()});
  if (!rows.empty()) {
    tx.commit();
    return {CommitStatus::kAccepted, FromMicros(rows[0][0].as<std::int64_t>())};
  }
  const bool exists = !tx.exec("SELECT 1 FROM jobs WHERE job_id = $1", pqxx::params{id}).empty();
  tx.commit();
  return {exists ? CommitStatus::kStale : CommitStatus::kNotFound, {}};
}

CommitStatus PgJobStore::Complete(JobId id, Generation generation, const std::string& result) {
  auto conn = pool_.Acquire();
  pqxx::work tx{*conn};
  const pqxx::result updated =
      tx.exec(kCompleteSql, pqxx::params{id, generation, pqxx::binary_cast(result)});
  const CommitStatus status =
      updated.affected_rows() == 1
          ? CommitStatus::kAccepted
          : ClassifyRejected(tx, id, generation, "COMPLETED", "result", result);
  tx.commit();
  return status;
}

CommitStatus PgJobStore::Fail(JobId id, Generation generation, const std::string& reason,
                              bool retryable) {
  auto conn = pool_.Acquire();
  pqxx::work tx{*conn};
  const pqxx::result updated = tx.exec(kFailSql, pqxx::params{id, generation, reason, retryable});
  const CommitStatus status = updated.affected_rows() == 1
                                  ? CommitStatus::kAccepted
                                  : ClassifyRejected(tx, id, generation, "FAILED", "error", reason);
  tx.commit();
  return status;
}

OperationOutcome PgJobStore::ApplyLedger(JobId id, Generation generation,
                                         const std::string& operation_id,
                                         const LedgerEntry& entry) {
  if (operation_id.empty()) throw std::invalid_argument("operation_id is required");
  if (entry.account.empty()) throw std::invalid_argument("account is required");
  const std::string request = entry.account + "\n" + std::to_string(entry.amount_cents);

  auto conn = pool_.Acquire();
  pqxx::work tx{*conn};
  if (const CommitStatus s = LockCurrentAssignment(tx, id, generation);
      s != CommitStatus::kAccepted) {
    return {s, {}, false};  // rolled back; nothing was written
  }
  // A concurrent insert of the same id makes this wait for the other
  // transaction; if it commits, this inserts nothing and the read below sees
  // its row.
  const bool created =
      !tx.exec(
             "INSERT INTO operations (operation_id, kind, request, status, job_id, generation) "
             "VALUES ($1, 'LEDGER', $2, 'APPLIED', $3, $4) "
             "ON CONFLICT (operation_id) DO NOTHING RETURNING 1",
             pqxx::params{operation_id, request, id, generation})
          .empty();
  if (created) {
    const auto entry_id = tx.query_value<std::int64_t>(
        "INSERT INTO ledger_entries (operation_id, account, amount_cents, job_id, generation) "
        "VALUES ($1, $2, $3, $4, $5) RETURNING entry_id",
        pqxx::params{operation_id, entry.account, entry.amount_cents, id, generation});
    tx.commit();
    return {CommitStatus::kAccepted, std::to_string(entry_id), false};
  }
  const pqxx::result existing = tx.exec(
      "SELECT o.kind, o.request, l.entry_id FROM operations o "
      "LEFT JOIN ledger_entries l USING (operation_id) WHERE o.operation_id = $1",
      pqxx::params{operation_id});
  tx.commit();
  const pqxx::row_ref r = existing[0];
  if (r[0].view() != "LEDGER" || r[1].view() != request) return {CommitStatus::kConflict, {}, false};
  return {CommitStatus::kAccepted, std::to_string(r[2].as<std::int64_t>()), true};
}

ExternalBegin PgJobStore::BeginExternal(JobId id, Generation generation,
                                        const std::string& operation_id,
                                        const std::string& request) {
  if (operation_id.empty()) throw std::invalid_argument("operation_id is required");
  auto conn = pool_.Acquire();
  pqxx::work tx{*conn};
  if (const CommitStatus s = LockCurrentAssignment(tx, id, generation);
      s != CommitStatus::kAccepted) {
    return {s, ExternalState::kStart, {}};
  }
  const bool created =
      !tx.exec(
             "INSERT INTO operations (operation_id, kind, request, status, job_id, generation) "
             "VALUES ($1, 'EXTERNAL', $2, 'STARTED', $3, $4) "
             "ON CONFLICT (operation_id) DO NOTHING RETURNING 1",
             pqxx::params{operation_id, request, id, generation})
          .empty();
  if (created) {
    tx.commit();
    return {CommitStatus::kAccepted, ExternalState::kStart, {}};
  }
  const pqxx::result existing = tx.exec(
      "SELECT kind, request, status, job_id, generation, result FROM operations "
      "WHERE operation_id = $1",
      pqxx::params{operation_id});
  tx.commit();
  const pqxx::row_ref r = existing[0];
  if (r[0].view() != "EXTERNAL" || r[1].view() != request) {
    return {CommitStatus::kConflict, ExternalState::kStart, {}};
  }
  if (r[2].view() == "CONFIRMED") {
    return {CommitStatus::kAccepted, ExternalState::kConfirmed, r[5].as<std::string>()};
  }
  const bool same_attempt = r[3].as<JobId>() == id && r[4].as<Generation>() == generation;
  return {CommitStatus::kAccepted, same_attempt ? ExternalState::kStart : ExternalState::kInDoubt,
          {}};
}

CommitStatus PgJobStore::ConfirmExternal(const std::string& operation_id,
                                         const std::string& request,
                                         const std::string& confirmation) {
  auto conn = pool_.Acquire();
  pqxx::work tx{*conn};
  const pqxx::result updated = tx.exec(
      "UPDATE operations SET status = 'CONFIRMED', result = $3, updated_at = now() "
      "WHERE operation_id = $1 AND kind = 'EXTERNAL' AND request = $2 AND status = 'STARTED'",
      pqxx::params{operation_id, request, confirmation});
  CommitStatus status = CommitStatus::kAccepted;
  if (updated.affected_rows() == 0) {
    const pqxx::result existing = tx.exec(
        "SELECT kind, request, status, result FROM operations WHERE operation_id = $1",
        pqxx::params{operation_id});
    if (existing.empty()) {
      status = CommitStatus::kNotFound;
    } else {
      const pqxx::row_ref r = existing[0];
      const bool same = r[0].view() == "EXTERNAL" && r[1].view() == request &&
                        r[2].view() == "CONFIRMED" && r[3].view() == confirmation;
      status = same ? CommitStatus::kAccepted : CommitStatus::kConflict;
    }
  }
  tx.commit();
  return status;
}

CommitStatus PgJobStore::MarkUnknown(JobId id, Generation generation, const std::string& reason) {
  auto conn = pool_.Acquire();
  pqxx::work tx{*conn};
  const pqxx::result updated = tx.exec(
      "UPDATE jobs SET state = 'UNKNOWN', error = $3, worker_id = NULL, lease_until = NULL, "
      "updated_at = now() WHERE job_id = $1 AND generation = $2 AND state = 'RUNNING'",
      pqxx::params{id, generation, reason});
  const CommitStatus status = updated.affected_rows() == 1
                                  ? CommitStatus::kAccepted
                                  : ClassifyRejected(tx, id, generation, "UNKNOWN", "error", reason);
  tx.commit();
  return status;
}

CommitStatus PgJobStore::Reconcile(JobId id, JobState outcome, const std::string& detail) {
  if (outcome != JobState::kCompleted && outcome != JobState::kFailed) {
    throw std::invalid_argument("reconcile outcome must be COMPLETED or FAILED");
  }
  const bool completed = outcome == JobState::kCompleted;
  auto conn = pool_.Acquire();
  pqxx::work tx{*conn};
  const pqxx::result updated =
      completed ? tx.exec("UPDATE jobs SET state = 'COMPLETED', result = $2, updated_at = now() "
                          "WHERE job_id = $1 AND state = 'UNKNOWN'",
                          pqxx::params{id, pqxx::binary_cast(detail)})
                : tx.exec("UPDATE jobs SET state = 'FAILED', error = $2, updated_at = now() "
                          "WHERE job_id = $1 AND state = 'UNKNOWN'",
                          pqxx::params{id, detail});
  CommitStatus status = CommitStatus::kAccepted;
  if (updated.affected_rows() == 0) {
    const pqxx::result rows =
        tx.exec("SELECT state, result, error FROM jobs WHERE job_id = $1", pqxx::params{id});
    if (rows.empty()) {
      status = CommitStatus::kNotFound;
    } else {
      // Repeating the decision that resolved the job is idempotent.
      const pqxx::row_ref r = rows[0];
      const bool same =
          completed ? r[0].view() == "COMPLETED" && ToString(r[1].as<pqxx::bytes>()) == detail
                    : r[0].view() == "FAILED" && !r[2].is_null() && r[2].view() == detail;
      status = same ? CommitStatus::kAccepted : CommitStatus::kStale;
    }
  }
  tx.commit();
  return status;
}

std::size_t PgJobStore::ReclaimExpired() {
  auto conn = pool_.Acquire();
  pqxx::work tx{*conn};
  const pqxx::result updated = tx.exec(kReclaimSql);
  tx.commit();
  return static_cast<std::size_t>(updated.affected_rows());
}

}  // namespace epochflow
