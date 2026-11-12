#include "coordinator/reclaimer.h"

#include "common/interruptible_sleep.h"

#include <exception>
#include <format>
#include <stdexcept>
#include <utility>

namespace epochflow {

Reclaimer::Reclaimer(JobStore& store, Duration interval, LogFn log)
    : store_(store), interval_(interval), log_(std::move(log)) {
  if (interval_ <= Duration::zero()) throw std::invalid_argument("reclaim interval must be positive");
  thread_ = std::jthread([this](std::stop_token stop) { Run(stop); });
}

Reclaimer::~Reclaimer() { Stop(); }

void Reclaimer::Stop() {
  thread_.request_stop();
  if (thread_.joinable()) thread_.join();
}

void Reclaimer::Run(std::stop_token stop) {
  while (!stop.stop_requested()) {
    try {
      const std::size_t n = store_.ReclaimExpired();
      if (n > 0 && log_) log_(std::format("reclaimed {} expired lease(s)", n));
    } catch (const std::exception& e) {
      if (log_) log_(std::format("reclaim failed: {}", e.what()));
    } catch (...) {
      if (log_) log_("reclaim failed");
    }
    InterruptibleSleep(stop, interval_);
  }
}

}  // namespace epochflow
