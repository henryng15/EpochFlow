#pragma once

#include "coordinator/reclaimer.h"
#include "core/clock.h"
#include "core/job_store.h"
#include "worker/work_source.h"
#include "worker/worker.h"

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

namespace epochflow {

struct RunnerOptions {
  int workers = 4;
  // Granted on every acquire and renewal; workers renew every third of it.
  Duration lease = std::chrono::seconds{10};
  // Wait after finding no PENDING job.
  Duration idle_backoff = std::chrono::milliseconds{100};
  // Period of the expired-lease recovery loop.
  Duration reclaim_interval = std::chrono::seconds{1};
};

// Runs jobs from a JobStore inside one process: `workers` Worker threads that
// talk to the store directly (StoreWorkSource) plus one Reclaimer. Uses the same
// Worker and Reclaimer as the distributed coordinator and worker processes.
//
// Threads start in the constructor. Stop() (also run by the destructor) lets
// in-flight handlers finish and report, then joins every thread. The store
// must outlive the runner.
class LocalRunner {
 public:
  // Throws std::invalid_argument for workers < 1 or non-positive durations.
  LocalRunner(JobStore& store, JobHandler handler, RunnerOptions options);
  ~LocalRunner();

  LocalRunner(const LocalRunner&) = delete;
  LocalRunner& operator=(const LocalRunner&) = delete;

  // Idempotent. Must not race with destruction.
  void Stop();

 private:
  StoreWorkSource source_;
  std::vector<std::unique_ptr<Worker>> workers_;
  std::vector<std::jthread> threads_;
  std::unique_ptr<Reclaimer> reclaimer_;
};

}  // namespace epochflow
