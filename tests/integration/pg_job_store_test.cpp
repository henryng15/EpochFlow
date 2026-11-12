// PgJobStore against the PostgreSQL named by EPOCHFLOW_DSN: the shared JobStore
// contract plus storage-specific behavior (migrations, durability, database
// clock, connection loss, schema constraints). Skipped without EPOCHFLOW_DSN.

#include "contract/job_store_contract.h"
#include "store/migrations.h"
#include "store/pg_job_store.h"
#include "support/forwarding_job_store.h"
#include "support/pg_test_schema.h"
#include "worker/tasks.h"
#include "worker/work_source.h"

#include <gtest/gtest.h>
#include <pqxx/pqxx>

#include <chrono>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <sstream>
#include <thread>
#include <vector>

namespace epochflow::testing {
namespace {

using std::chrono::milliseconds;
using std::chrono::seconds;

// Rows of scripts/check-invariants.sql as "invariant|subject|detail".
std::vector<std::string> InvariantViolations(const std::string& dsn) {
  std::ifstream file(EPOCHFLOW_SOURCE_DIR "/scripts/check-invariants.sql");
  std::stringstream sql;
  sql << file.rdbuf();
  pqxx::connection conn{dsn};
  pqxx::read_transaction tx{conn};
  std::vector<std::string> rows;
  for (const auto& [invariant, subject, detail] :
       tx.query<std::string, std::string, std::string>(sql.str())) {
    rows.push_back(invariant + "|" + subject + "|" + detail);
  }
  return rows;
}

// Moves "now" forward for the store by moving every lease deadline back by
// the same amount: lease expiry is the only time-dependent rule, so the store
// observes exactly what it would if the database clock jumped.
class PgHarness final : public StoreHarness {
 public:
  explicit PgHarness(const std::string& base_dsn) : schema_(base_dsn) {
    MigrateDatabase(schema_.dsn());
    store_ = std::make_unique<PgJobStore>(schema_.dsn());
  }

  JobStore& store() override { return *store_; }

  // Every contract scenario must leave a database that passes the invariant
  // checker. "recovery stalled" is a liveness check; tests that move time far
  // ahead without reclaiming trip it on purpose.
  void VerifyInvariants() override {
    for (const std::string& v : InvariantViolations(schema_.dsn())) {
      if (!v.starts_with("recovery stalled")) ADD_FAILURE() << "invariant violation: " << v;
    }
  }

  void AdvanceTime(Duration by) override {
    pqxx::connection conn{schema_.dsn()};
    pqxx::work tx{conn};
    tx.exec("UPDATE jobs SET lease_until = lease_until - $1::bigint * interval '1 millisecond' "
            "WHERE lease_until IS NOT NULL",
            pqxx::params{by.count()});
    tx.commit();
  }

 private:
  PgTestSchema schema_;
  std::unique_ptr<PgJobStore> store_;
};

std::unique_ptr<StoreHarness> MakePgHarness() {
  const auto dsn = TestDsn();
  if (!dsn) return nullptr;
  return std::make_unique<PgHarness>(*dsn);
}

INSTANTIATE_TEST_SUITE_P(Postgres, JobStoreContractTest,
                         ::testing::Values(StoreHarnessFactory{&MakePgHarness}));

class PgJobStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const auto dsn = TestDsn();
    if (!dsn) GTEST_SKIP() << "EPOCHFLOW_DSN is not set";
    schema_ = std::make_unique<PgTestSchema>(*dsn);
  }

  std::string Dsn() const { return schema_->dsn(); }

  std::unique_ptr<PgTestSchema> schema_;
};

TEST_F(PgJobStoreTest, MigrationIsIdempotent) {
  MigrateDatabase(Dsn());
  MigrateDatabase(Dsn());

  pqxx::connection conn{Dsn()};
  pqxx::work tx{conn};
  EXPECT_EQ(tx.query_value<int>("SELECT count(*) FROM schema_migrations"),
            static_cast<int>(EmbeddedMigrations().size()));
}

TEST_F(PgJobStoreTest, ConcurrentMigrationsDoNotConflict) {
  // Several coordinators starting at once must not race on DDL.
  std::vector<std::jthread> threads;
  std::atomic<int> failures{0};
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([&] {
      try {
        MigrateDatabase(Dsn());
      } catch (...) {
        ++failures;
      }
    });
  }
  threads.clear();
  EXPECT_EQ(failures.load(), 0);
}

TEST_F(PgJobStoreTest, StoreRejectsUnmigratedDatabase) {
  EXPECT_THROW(PgJobStore{Dsn()}, std::runtime_error);
}

TEST_F(PgJobStoreTest, ConstructionFailsFastWhenDatabaseIsUnreachable) {
  EXPECT_THROW(PgJobStore{"host=127.0.0.1 port=1 connect_timeout=2"}, pqxx::broken_connection);
}

TEST_F(PgJobStoreTest, StateSurvivesStoreRestart) {
  MigrateDatabase(Dsn());
  JobId id = 0;
  Assignment assignment;
  {
    PgJobStore store{Dsn()};
    id = store.Submit({"durable", 3});
    assignment = *store.Acquire("w1", seconds{30});
  }  // the coordinator process "exits"

  PgJobStore restarted{Dsn()};
  const auto job = restarted.Get(id);
  ASSERT_TRUE(job.has_value());
  EXPECT_EQ(job->state, JobState::kRunning);
  EXPECT_EQ(job->generation, assignment.generation);
  EXPECT_EQ(job->worker_id, "w1");
  EXPECT_EQ(job->lease_until, assignment.lease_until);
  // The worker that held the lease before the restart can still finish.
  EXPECT_EQ(restarted.Complete(id, assignment.generation, "ok"), CommitStatus::kAccepted);
}

TEST_F(PgJobStoreTest, LeaseExpiryFollowsTheDatabaseClock) {
  MigrateDatabase(Dsn());
  PgJobStore store{Dsn()};
  const JobId id = store.Submit({"a", 3});
  store.Acquire("w1", milliseconds{100});

  EXPECT_EQ(store.ReclaimExpired(), 0u);
  std::this_thread::sleep_for(milliseconds{300});
  EXPECT_EQ(store.ReclaimExpired(), 1u);
  EXPECT_EQ(store.Get(id)->state, JobState::kPending);
}

TEST_F(PgJobStoreTest, RecoversAfterItsConnectionIsTerminated) {
  MigrateDatabase(Dsn());
  PgStoreOptions options;
  options.pool_size = 1;
  PgJobStore store{Dsn(), options};
  const JobId id = store.Submit({"a", 3});

  {
    pqxx::connection admin{schema_->base_dsn()};
    pqxx::work tx{admin};
    const auto killed = tx.query_value<int>(
        "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
        "WHERE application_name = $1",
        pqxx::params{schema_->application_name()});
    tx.commit();
    ASSERT_GE(killed, 1);
  }

  // The first call may fail on the dead connection (outcome reported as an
  // exception); the store must then reconnect rather than stay broken.
  std::optional<JobRecord> job;
  int failures = 0;
  for (int i = 0; i < 2 && !job; ++i) {
    try {
      job = store.Get(id);
    } catch (const pqxx::broken_connection&) {
      ++failures;
    }
  }
  ASSERT_TRUE(job.has_value());
  EXPECT_LE(failures, 1);
  EXPECT_EQ(job->payload, "a");
}

TEST_F(PgJobStoreTest, SchemaRejectsRowsNoTransitionCanProduce) {
  MigrateDatabase(Dsn());
  pqxx::connection conn{Dsn()};
  const auto insert_fails = [&](const std::string& sql) {
    pqxx::work tx{conn};
    try {
      tx.exec(sql);
    } catch (const pqxx::check_violation&) {
      return true;
    }
    return false;
  };
  // RUNNING without a lease.
  EXPECT_TRUE(insert_fails(
      "INSERT INTO jobs (state, generation, max_attempts, payload) "
      "VALUES ('RUNNING', 1, 3, 'x')"));
  // COMPLETED without a result.
  EXPECT_TRUE(insert_fails(
      "INSERT INTO jobs (state, generation, max_attempts, payload) "
      "VALUES ('COMPLETED', 1, 3, 'x')"));
  // More attempts than allowed.
  EXPECT_TRUE(insert_fails(
      "INSERT INTO jobs (attempts, max_attempts, payload) VALUES (4, 3, 'x')"));
  // Unknown state.
  EXPECT_TRUE(insert_fails("INSERT INTO jobs (state, max_attempts, payload) VALUES ('DONE', 3, 'x')"));
}


TEST_F(PgJobStoreTest, AttemptsTraceEveryAssignment) {
  MigrateDatabase(Dsn());
  PgJobStore store{Dsn()};
  const JobId id = store.Submit({"a", 3});
  store.Acquire("crashes", milliseconds{50});
  std::this_thread::sleep_for(milliseconds{150});
  ASSERT_EQ(store.ReclaimExpired(), 1u);
  const auto second = store.Acquire("fails", seconds{30});
  store.Fail(id, second->generation, "transient", true);
  const auto third = store.Acquire("finishes", seconds{30});
  store.Complete(id, third->generation, "ok");

  pqxx::connection conn{Dsn()};
  pqxx::read_transaction tx{conn};
  std::vector<std::string> trace;
  for (const auto& [generation, worker, outcome] : tx.query<int, std::string, std::string>(
           "SELECT generation, worker_id, outcome FROM job_attempts WHERE job_id = $1 "
           "ORDER BY generation",
           pqxx::params{id})) {
    trace.push_back(std::to_string(generation) + ":" + worker + ":" + outcome);
  }
  EXPECT_EQ(trace, (std::vector<std::string>{"1:crashes:LEASE_EXPIRED", "2:fails:REQUEUED",
                                             "3:finishes:COMPLETED"}));
}

TEST_F(PgJobStoreTest, InvariantCheckerPassesAfterValidOperations) {
  MigrateDatabase(Dsn());
  PgJobStore store{Dsn()};
  // A mix of every terminal path, a running job, and a deduplicated effect.
  const JobId done = store.Submit({"done", 3});
  const JobId failed = store.Submit({"failed", 1});
  const JobId unknown = store.Submit({"unknown", 3});
  const JobId reconciled = store.Submit({"reconciled", 3});
  const JobId running = store.Submit({"running", 3});
  const auto d = store.Acquire("w", seconds{30});
  store.ApplyLedger(done, d->generation, "op-1", LedgerEntry{"alice", 5});
  store.ApplyLedger(done, d->generation, "op-1", LedgerEntry{"alice", 5});
  store.Complete(done, d->generation, "ok");
  const auto f = store.Acquire("w", seconds{30});
  store.Fail(failed, f->generation, "fatal", true);  // last attempt
  const auto u = store.Acquire("w", seconds{30});
  store.BeginExternal(unknown, u->generation, "ext-1", "req");
  store.MarkUnknown(unknown, u->generation, "timeout");
  const auto r = store.Acquire("w", seconds{30});
  store.MarkUnknown(reconciled, r->generation, "timeout");
  store.Reconcile(reconciled, JobState::kCompleted, "evidence");
  ASSERT_EQ(store.Acquire("w", seconds{30})->job_id, running);

  EXPECT_EQ(InvariantViolations(Dsn()), std::vector<std::string>{});
}

TEST_F(PgJobStoreTest, InvariantCheckerReportsInjectedViolations) {
  MigrateDatabase(Dsn());
  PgJobStore store{Dsn()};
  const JobId id = store.Submit({"a", 3});
  const auto a = store.Acquire("w", seconds{30});
  store.Complete(id, a->generation, "ok");
  {
    // Forge what a broken store could produce: a second deciding attempt from
    // a stale generation, and a ledger operation without its entry.
    pqxx::connection conn{Dsn()};
    pqxx::work tx{conn};
    tx.exec("UPDATE jobs SET generation = 2, attempts = 2 WHERE job_id = $1", pqxx::params{id});
    tx.exec("INSERT INTO job_attempts (job_id, generation, worker_id, finished_at, outcome) "
            "VALUES ($1, 2, 'ghost', now(), 'COMPLETED')",
            pqxx::params{id});
    tx.exec("INSERT INTO operations (operation_id, kind, request, status, job_id, generation) "
            "VALUES ('orphan', 'LEDGER', 'x', 'APPLIED', $1, 1)",
            pqxx::params{id});
    tx.commit();
  }

  const auto violations = InvariantViolations(Dsn());
  const auto has = [&](const std::string& prefix) {
    return std::any_of(violations.begin(), violations.end(),
                       [&](const std::string& v) { return v.starts_with(prefix); });
  };
  EXPECT_TRUE(has("I3 one deciding attempt|job " + std::to_string(id)));
  EXPECT_TRUE(has("I3 outcome from current generation|job " + std::to_string(id)));
  EXPECT_TRUE(has("I4 ledger operation has entry|operation orphan"));
}

// The confirmation write never reaches PostgreSQL, as when every
// ConfirmExternalOperation RPC times out.
class UnreachableConfirmations final : public ForwardingJobStore {
 public:
  using ForwardingJobStore::ForwardingJobStore;
  CommitStatus ConfirmExternal(const std::string&, const std::string&,
                               const std::string&) override {
    throw std::runtime_error("UNAVAILABLE: coordinator unreachable");
  }
};

class OneChargeProvider final : public ExternalProvider {
 public:
  int charges = 0;
  std::string Charge(const std::string&, const std::string&) override {
    return "ch_" + std::to_string(++charges);
  }
};

TEST_F(PgJobStoreTest, UnrecordedConfirmationLeavesTheJobUnknownNotCompleted) {
  MigrateDatabase(Dsn());
  PgJobStore store{Dsn()};
  UnreachableConfirmations lossy{store};
  StoreWorkSource source{lossy, seconds{30}};
  OneChargeProvider provider;
  TaskOptions options;
  options.provider = &provider;

  const JobId id = store.Submit({"task=external;op=charge-1;account=a;amount_cents=5", 3});
  const auto a = store.Acquire("w", seconds{30});
  const HandlerResult result = MakeTaskHandler(source, options)(*a, std::stop_token{});
  ASSERT_TRUE(std::holds_alternative<JobUnknown>(result));
  // What the worker does with that result.
  ASSERT_EQ(store.MarkUnknown(id, a->generation, std::get<JobUnknown>(result).reason),
            CommitStatus::kAccepted);

  EXPECT_EQ(provider.charges, 1);
  EXPECT_EQ(store.Get(id)->state, JobState::kUnknown);
  EXPECT_EQ(InvariantViolations(Dsn()), std::vector<std::string>{});
}

TEST_F(PgJobStoreTest, InvariantCheckerReportsCompletionWithUnconfirmedExternalOperation) {
  MigrateDatabase(Dsn());
  PgJobStore store{Dsn()};
  // What a worker that ignores its confirmation result (Version 1) produced.
  const JobId id = store.Submit({"a", 3});
  const auto a = store.Acquire("w", seconds{30});
  store.BeginExternal(id, a->generation, "charge-1", "req");
  store.Complete(id, a->generation, "charge=ch_1");

  EXPECT_EQ(InvariantViolations(Dsn()),
            std::vector<std::string>{"I8 completed job has unresolved external operation|job " +
                                     std::to_string(id) + "|operation charge-1 is STARTED"});
}

}  // namespace
}  // namespace epochflow::testing
