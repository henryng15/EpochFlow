#include "worker/lease_keeper.h"

#include <condition_variable>
#include <mutex>
#include <utility>

namespace epochflow {

LeaseKeeper::LeaseKeeper(RenewFn renew, std::chrono::milliseconds interval)
    : renew_(std::move(renew)), interval_(interval) {
  // Started last, after every member the thread reads is initialized.
  thread_ = std::jthread([this](std::stop_token stop) { Run(stop); });
}

LeaseKeeper::~LeaseKeeper() { Stop(); }

void LeaseKeeper::Stop() {
  thread_.request_stop();
  if (thread_.joinable()) thread_.join();
}

void LeaseKeeper::Run(std::stop_token stop) {
  std::mutex mu;
  std::condition_variable_any wake;
  while (true) {
    {
      std::unique_lock lock(mu);
      // The predicate never holds, so this returns after `interval_` or as
      // soon as a stop is requested.
      wake.wait_for(lock, stop, interval_, [] { return false; });
    }
    if (stop.stop_requested()) return;

    bool renewed = false;
    try {
      renewed = renew_();
    } catch (...) {
      renewed = false;
    }
    if (!renewed) {
      lost_.request_stop();
      return;
    }
  }
}

}  // namespace epochflow
