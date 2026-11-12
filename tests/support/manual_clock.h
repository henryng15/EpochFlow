#pragma once

#include "core/clock.h"

#include <mutex>

namespace epochflow::testing {

// Clock that moves only when a test calls Advance. Thread-safe.
class ManualClock final : public Clock {
 public:
  explicit ManualClock(TimePoint start = TimePoint{} + std::chrono::hours{24 * 365 * 50})
      : now_(start) {}

  TimePoint Now() const override {
    std::lock_guard lock(mu_);
    return now_;
  }

  void Advance(Duration by) {
    std::lock_guard lock(mu_);
    now_ += by;
  }

 private:
  mutable std::mutex mu_;
  TimePoint now_;
};

}  // namespace epochflow::testing
