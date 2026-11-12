#pragma once

#include <pqxx/pqxx>

#include <atomic>
#include <cstdlib>
#include <optional>
#include <random>
#include <string>

#include <unistd.h>

namespace epochflow::testing {

// The test database named by EPOCHFLOW_DSN, or nullopt (tests then skip).
inline std::optional<std::string> TestDsn() {
  const char* dsn = std::getenv("EPOCHFLOW_DSN");
  if (dsn == nullptr || *dsn == '\0') return std::nullopt;
  return std::string(dsn);
}

// A uniquely named schema that exists for the lifetime of this object, so
// tests running in parallel processes never see each other's rows. dsn()
// returns a connection string whose search_path is this schema; every
// connection made from it also carries a unique application_name, which lets a
// test find (and terminate) exactly its own backends.
class PgTestSchema {
 public:
  explicit PgTestSchema(std::string base_dsn) : base_dsn_(std::move(base_dsn)) {
    static std::atomic<int> counter{0};
    std::random_device rd;
    name_ = "epochflow_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
            "_" + std::to_string(rd() % 100000);
    pqxx::connection admin{base_dsn_};
    pqxx::work tx{admin};
    tx.exec("CREATE SCHEMA " + tx.quote_name(name_));
    tx.commit();
  }

  ~PgTestSchema() {
    try {
      pqxx::connection admin{base_dsn_};
      pqxx::work tx{admin};
      tx.exec("DROP SCHEMA IF EXISTS " + tx.quote_name(name_) + " CASCADE");
      tx.commit();
    } catch (...) {
      // Best effort: a leftover schema does not affect other tests.
    }
  }

  PgTestSchema(const PgTestSchema&) = delete;
  PgTestSchema& operator=(const PgTestSchema&) = delete;

  const std::string& name() const { return name_; }
  const std::string& application_name() const { return name_; }
  std::string dsn() const {
    return base_dsn_ + " application_name=" + name_ + " options='-csearch_path=" + name_ + "'";
  }
  const std::string& base_dsn() const { return base_dsn_; }

 private:
  std::string base_dsn_;
  std::string name_;
};

}  // namespace epochflow::testing
