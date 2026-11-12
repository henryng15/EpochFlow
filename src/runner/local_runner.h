#pragma once

#include "core/clock.h"
#include "core/job.h"
#include "core/job_store.h"

#include <chrono>
#include <functional>
#include <stop_token>
#include <string>
#include <thread>
#include <variant>
#include <vector>

namespace epochflow {

struct JobSucceeded {
  std::string result;
};

struct JobFailed {
  std::string reason;
  // Retryable failures re-queue the job while attempts remain.
  bool retryable = true;
};

using HandlerResult = std::variant<JobSucceeded, JobFailed>;

// Executes one assignment. `lease_lost` is stop-requested when the lease can no
// longer be renewed; the handler should then return as soon as it can, and its
// result is discarded. An exception counts as a retryable failure. Called
// concurrently from several worker threads.
using JobHandler = std::function<HandlerResult(const Assignment&, std::stop_token lease_lost)>;

struct RunnerOptions {
  int workers = 4;
  Duration lease = std::chrono::seconds{10};
  // Must be shorter than `lease`; about a third leaves room for two misses.
  Duration renew_interval = std::chrono::seconds{3};
  // Wait after finding no PENDING job.
  Duration idle_backoff = std::chrono::milliseconds{100};
  // Period of the expired-lease recovery loop.
  Duration reclaim_interval = std::chrono::seconds{1};
};

// Runs jobs from a JobStore on worker threads inside one process. This is the
// milestone-1 stand-in for separate coordinator and worker processes; it uses
// the same acquire / renew / report protocol they will.
//
// Each worker thread loops: acquire a job, renew its lease in the background
// (LeaseKeeper), run the handler, then report the result with the assignment's
// generation. A result is never reported once the lease is lost, and a kStale
// report is dropped. One more thread calls JobStore::ReclaimExpired every
// `reclaim_interval`.
//
// Threads start in the constructor. Stop() (also run by the destructor) lets
// in-flight handlers finish and report, then joins every thread. The store
// must outlive the runner.
class LocalRunner {
 public:
  // Throws std::invalid_argument for workers < 1, non-positive durations, or
  // renew_interval >= lease.
  LocalRunner(JobStore& store, JobHandler handler, RunnerOptions options);
  ~LocalRunner();

  LocalRunner(const LocalRunner&) = delete;
  LocalRunner& operator=(const LocalRunner&) = delete;

  // Idempotent. Must not race with destruction.
  void Stop();

 private:
  void WorkerLoop(std::stop_token stop, const WorkerId& worker);
  void RunAssignment(const Assignment& assignment);
  void ReclaimLoop(std::stop_token stop);

  JobStore& store_;
  JobHandler handler_;
  RunnerOptions options_;
  std::vector<std::jthread> threads_;
};

}  // namespace epochflow
