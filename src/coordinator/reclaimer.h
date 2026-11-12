#pragma once

#include "common/log.h"
#include "core/clock.h"
#include "core/job_store.h"

#include <thread>

namespace epochflow {

// Calls JobStore::ReclaimExpired every `interval` on a background thread, so
// jobs whose workers stopped renewing return to PENDING (or FAILED when out of
// attempts). Store errors are logged and retried on the next tick.
//
// Running more than one Reclaimer against the same store is safe (each pass is
// atomic in the store) but wasteful. The store must outlive the Reclaimer.
class Reclaimer {
 public:
  // Starts immediately. Throws std::invalid_argument if interval <= 0.
  Reclaimer(JobStore& store, Duration interval, LogFn log = {});
  ~Reclaimer();

  Reclaimer(const Reclaimer&) = delete;
  Reclaimer& operator=(const Reclaimer&) = delete;

  // Idempotent. Must not race with destruction.
  void Stop();

 private:
  void Run(std::stop_token stop);

  JobStore& store_;
  Duration interval_;
  LogFn log_;
  // Declared last: stopped and joined before the members it uses go away.
  std::jthread thread_;
};

}  // namespace epochflow
