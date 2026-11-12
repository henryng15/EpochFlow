#include "store/pg_connection_pool.h"

#include <stdexcept>
#include <utility>

namespace epochflow {

PgConnectionPool::PgConnectionPool(std::string dsn, int size) : dsn_(std::move(dsn)), size_(size) {
  if (size_ < 1) throw std::invalid_argument("pool size must be >= 1");
}

PgConnectionPool::Lease PgConnectionPool::Acquire() {
  std::unique_lock lock(mu_);
  available_.wait(lock, [&] { return !idle_.empty() || open_ < size_; });
  if (!idle_.empty()) {
    auto conn = std::move(idle_.back());
    idle_.pop_back();
    return Lease(*this, std::move(conn));
  }
  // Reserve the slot, then connect without holding the lock.
  ++open_;
  lock.unlock();
  try {
    return Lease(*this, std::make_unique<pqxx::connection>(dsn_));
  } catch (...) {
    lock.lock();
    --open_;
    available_.notify_one();
    throw;
  }
}

void PgConnectionPool::Release(std::unique_ptr<pqxx::connection> conn) {
  std::lock_guard lock(mu_);
  if (conn->is_open()) {
    idle_.push_back(std::move(conn));
  } else {
    --open_;  // dropped; a later Acquire opens a replacement
  }
  available_.notify_one();
}

}  // namespace epochflow
