// Verifies that the test PostgreSQL instance named by EPOCHFLOW_DSN is reachable
// and runs a supported server version. Skipped when EPOCHFLOW_DSN is unset so a
// plain unit-test run does not require Docker; see docs/testing.md.

#include <gtest/gtest.h>
#include <pqxx/pqxx>

#include <cstdlib>
#include <string>

namespace {

const char* TestDsn() { return std::getenv("EPOCHFLOW_DSN"); }

}  // namespace

TEST(PostgresSmoke, ConnectsAndRunsQuery) {
  const char* dsn = TestDsn();
  if (dsn == nullptr) GTEST_SKIP() << "EPOCHFLOW_DSN is not set";

  pqxx::connection conn{dsn};
  pqxx::work tx{conn};
  EXPECT_EQ(tx.query_value<int>("SELECT 1"), 1);
}

TEST(PostgresSmoke, ServerIsVersion16OrNewer) {
  const char* dsn = TestDsn();
  if (dsn == nullptr) GTEST_SKIP() << "EPOCHFLOW_DSN is not set";

  pqxx::connection conn{dsn};
  EXPECT_GE(conn.server_version(), 160000);
}
