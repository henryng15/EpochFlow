#pragma once

#include <chrono>

namespace epochflow {

// Wall-clock time, matching PostgreSQL timestamptz so lease deadlines mean the
// same thing in every store.
using TimePoint = std::chrono::system_clock::time_point;
using Duration = std::chrono::milliseconds;

// Time source for stores that judge lease expiry in-process. Injected so tests
// can move time deterministically instead of sleeping. Implementations must be
// safe to call from multiple threads.
class Clock {
 public:
  virtual ~Clock() = default;
  virtual TimePoint Now() const = 0;
};

// Uses std::chrono::system_clock. A backward wall-clock jump can lengthen a
// lease and a forward jump can expire one early; both are safe because fencing
// does not depend on lease timing (docs/invariants.md, I3).
class SystemClock final : public Clock {
 public:
  TimePoint Now() const override {
    return std::chrono::time_point_cast<Duration>(std::chrono::system_clock::now());
  }
};

}  // namespace epochflow
