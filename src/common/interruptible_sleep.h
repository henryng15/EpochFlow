#pragma once

#include <condition_variable>
#include <mutex>
#include <stop_token>

namespace epochflow {

// Sleeps for `duration` or until a stop is requested, whichever comes first.
template <typename Duration>
void InterruptibleSleep(std::stop_token stop, Duration duration) {
  std::mutex mu;
  std::condition_variable_any wake;
  std::unique_lock lock(mu);
  wake.wait_for(lock, stop, duration, [] { return false; });
}

}  // namespace epochflow
