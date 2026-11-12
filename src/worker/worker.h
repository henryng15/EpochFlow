#pragma once

#include "common/log.h"
#include "core/clock.h"
#include "core/job.h"
#include "worker/work_source.h"

#include <chrono>
#include <functional>
#include <stop_token>
#include <string>
#include <variant>

namespace epochflow {

struct JobSucceeded {
  std::string result;
};

struct JobFailed {
  std::string reason;
  // Retryable failures re-queue the job while attempts remain.
  bool retryable = true;
};

// The outcome of an external effect cannot be determined (for example the
// provider timed out after the request was sent). The job becomes UNKNOWN and
// waits for reconciliation instead of being retried.
struct JobUnknown {
  std::string reason;
};

using HandlerResult = std::variant<JobSucceeded, JobFailed, JobUnknown>;

// Executes one assignment. `lease_lost` is stop-requested when the lease can no
// longer be renewed; the handler should then return as soon as it can, and its
// result is discarded. An exception counts as a retryable failure.
using JobHandler = std::function<HandlerResult(const Assignment&, std::stop_token lease_lost)>;

struct WorkerOptions {
  // Wait after finding no PENDING job or failing to reach the source.
  Duration idle_backoff = std::chrono::milliseconds{100};
  // Tries per result report when the source throws (outcome unknown).
  int report_attempts = 3;
  Duration report_backoff = std::chrono::milliseconds{100};
  // Optional event log (acquired, reported, stale, gave up).
  LogFn log;
  // Optional hooks at the points where failures matter most, used for fault
  // injection: right after a job is acquired, and after the handler finished
  // with the lease still held, immediately before the outcome is reported.
  std::function<void(const Assignment&)> after_acquire;
  std::function<void(const Assignment&)> before_report;
};

// One worker: acquires a job, renews its lease in the background every third
// of the granted lease (LeaseKeeper), runs the handler, and reports the outcome
// with the assignment's generation.
//
// Failure handling:
// - Lease lost (renewal rejected or failed): the handler is cancelled through
//   its stop token and no result is reported.
// - Report throws (request or acknowledgement lost): retried up to
//   report_attempts times. Safe because the store treats a duplicate report for
//   the same generation as idempotent. If every try fails the job stays RUNNING
//   until its lease expires and it is reclaimed.
// - kStale / kConflict report: logged and dropped; a newer assignment owns the
//   job.
//
// Not thread-safe: one Worker is driven by one thread.
class Worker {
 public:
  // `source` must outlive the worker.
  Worker(WorkSource& source, JobHandler handler, WorkerId id, WorkerOptions options);

  // Loops until `stop` is requested. An in-flight job finishes (and reports)
  // before Run returns. Source errors during acquire are absorbed with backoff.
  void Run(std::stop_token stop);

  // Acquires and runs at most one job. Returns false if none was available.
  // Throws if Acquire throws.
  bool RunOnce();

  const WorkerId& id() const { return id_; }

 private:
  void Execute(const Assignment& assignment);
  void Report(const Assignment& assignment, const HandlerResult& outcome);
  void Log(const std::string& line) const;

  WorkSource& source_;
  JobHandler handler_;
  WorkerId id_;
  WorkerOptions options_;
};

}  // namespace epochflow
