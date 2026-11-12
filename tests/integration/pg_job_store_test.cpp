// PgJobStore against the PostgreSQL named by EPOCHFLOW_DSN: the shared JobStore
// contract plus storage-specific behavior (migrations, durability, database
// clock, connection loss, schema constraints). Skipped without EPOCHFLOW_DSN.

#include "contract/job_store_contract.h"
#include "store/migrations.h"
#include "store/pg_job_store.h"
#include "support/pg_test_schema.h"

#include <gtest/gtest.h>
#include <pqxx/pqxx>

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

namespace epochflow::testing {
namespace {

using std::chrono::milliseconds;
using std::chrono::seconds;

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

}  // namespace
}  // namespace epochflow::testing
