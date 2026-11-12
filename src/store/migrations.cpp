#include "store/migrations.h"

#include <pqxx/pqxx>

#include <set>

namespace epochflow {
namespace {

// First key of the two-key advisory lock; the second is the schema name hash.
constexpr int kMigrationLockClass = 0x45464C57;  // "EFLW"

}  // namespace

int LatestMigrationVersion() {
  const auto& all = EmbeddedMigrations();
  return all.empty() ? 0 : all.back().version;
}

void MigrateDatabase(const std::string& dsn) {
  pqxx::connection conn{dsn};
  pqxx::work tx{conn};
  tx.exec("SELECT pg_advisory_xact_lock($1, hashtext(current_schema()))",
          pqxx::params{kMigrationLockClass});
  tx.exec(
      "CREATE TABLE IF NOT EXISTS schema_migrations ("
      "  version    INTEGER PRIMARY KEY,"
      "  name       TEXT NOT NULL,"
      "  applied_at TIMESTAMPTZ NOT NULL DEFAULT now())");

  std::set<int> applied;
  for (const auto& [version] : tx.query<int>("SELECT version FROM schema_migrations")) {
    applied.insert(version);
  }
  for (const Migration& m : EmbeddedMigrations()) {
    if (applied.contains(m.version)) continue;
    tx.exec(m.sql);
    tx.exec("INSERT INTO schema_migrations (version, name) VALUES ($1, $2)",
            pqxx::params{m.version, std::string(m.name)});
  }
  tx.commit();
}

}  // namespace epochflow
