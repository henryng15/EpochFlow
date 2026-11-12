#pragma once

#include <chrono>
#include <thread>

namespace epochflow::testing {

// Polls `condition` until it holds or `timeout` passes. Returns the final value.
// For tests of real threads only; store tests advance a ManualClock instead.
template <typename Condition>
bool WaitUntil(Condition condition,
               std::chrono::milliseconds timeout = std::chrono::seconds{5}) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!condition()) {
    if (std::chrono::steady_clock::now() >= deadline) return condition();
    std::this_thread::sleep_for(std::chrono::milliseconds{2});
  }
  return true;
}

}  // namespace epochflow::testing
