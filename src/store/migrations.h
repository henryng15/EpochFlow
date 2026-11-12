#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace epochflow {

// One schema change from sql/migrations/NNNN_name.sql, embedded at build time.
struct Migration {
  int version;
  std::string_view name;
  std::string_view sql;
};

// All migrations in ascending version order.
const std::vector<Migration>& EmbeddedMigrations();

// Brings the schema selected by `dsn`'s search_path up to date. Applies every
// missing migration and records it in schema_migrations, all in one
// transaction, so a failure leaves the schema unchanged. Holds a transaction-
// scoped advisory lock keyed by schema, so concurrent callers (several
// coordinators starting at once) apply each migration exactly once.
// Throws pqxx exceptions on connection or SQL errors.
void MigrateDatabase(const std::string& dsn);

// Highest version in EmbeddedMigrations(); what PgJobStore requires.
int LatestMigrationVersion();

}  // namespace epochflow
