// Verifies that the third-party runtimes EpochFlow depends on are found by the
// build and link into a test binary. These tests need no external services.

#include <google/protobuf/stubs/common.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <pqxx/pqxx>

#include <string>

TEST(DependencySmoke, GrpcRuntimeLinks) {
  EXPECT_FALSE(grpc::Version().empty());
}

TEST(DependencySmoke, ProtobufRuntimeLinks) {
  // Aborts the process if headers and the linked library disagree.
  GOOGLE_PROTOBUF_VERIFY_VERSION;
  SUCCEED();
}

TEST(DependencySmoke, PqxxReportsUnreachableServerAsBrokenConnection) {
  // Port 1 is never a PostgreSQL server, so libpq fails fast without a timeout.
  const std::string dsn = "host=127.0.0.1 port=1 connect_timeout=2";
  EXPECT_THROW(pqxx::connection{dsn}, pqxx::broken_connection);
}
