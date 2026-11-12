#pragma once

#include <pqxx/pqxx>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace epochflow {

// Fixed-size pool of PostgreSQL connections shared by concurrent store calls.
// A pqxx::connection is not thread-safe, so each caller holds one exclusively
// for the duration of a transaction.
//
// Connections are opened lazily. A connection that is no longer open when it
// is returned (server restart, terminated backend, network loss) is discarded
// and replaced on a later Acquire, so the pool heals without a restart.
//
// Thread-safe. Lease objects must not outlive the pool.
class PgConnectionPool {
 public:
  class Lease {
   public:
    Lease(PgConnectionPool& pool, std::unique_ptr<pqxx::connection> conn)
        : pool_(&pool), conn_(std::move(conn)) {}
    Lease(Lease&& other) noexcept = default;
    Lease& operator=(Lease&&) = delete;
    ~Lease() {
      if (pool_ != nullptr && conn_ != nullptr) pool_->Release(std::move(conn_));
    }
    pqxx::connection& operator*() const { return *conn_; }
    pqxx::connection* operator->() const { return conn_.get(); }

   private:
    PgConnectionPool* pool_;
    std::unique_ptr<pqxx::connection> conn_;
  };

  // Throws std::invalid_argument if size < 1. Does not connect.
  PgConnectionPool(std::string dsn, int size);

  // Blocks until a connection is free. Throws pqxx::broken_connection if a
  // new connection cannot be opened.
  Lease Acquire();

 private:
  void Release(std::unique_ptr<pqxx::connection> conn);

  const std::string dsn_;
  const int size_;
  std::mutex mu_;
  std::condition_variable available_;
  std::vector<std::unique_ptr<pqxx::connection>> idle_;
  // Connections that exist (idle or leased) or are being opened.
  int open_ = 0;
};

}  // namespace epochflow
