#include "worker/worker.h"

#include "common/interruptible_sleep.h"
#include "worker/lease_keeper.h"

#include <algorithm>
#include <exception>
#include <format>
#include <thread>
#include <utility>

namespace epochflow {
namespace {

HandlerResult InvokeHandler(const JobHandler& handler, const Assignment& assignment,
                            std::stop_token lease_lost) {
  try {
    return handler(assignment, lease_lost);
  } catch (const std::exception& e) {
    return JobFailed{std::string("handler threw: ") + e.what(), true};
  } catch (...) {
    return JobFailed{"handler threw a non-standard exception", true};
  }
}

// Renew three times per lease so that one or two slow renewals still land
// before the deadline.
Duration RenewInterval(Duration lease) {
  return std::max(Duration{1}, lease / 3);
}

}  // namespace

Worker::Worker(WorkSource& source, JobHandler handler, WorkerId id, WorkerOptions options)
    : source_(source), handler_(std::move(handler)), id_(std::move(id)), options_(options) {}

void Worker::Run(std::stop_token stop) {
  while (!stop.stop_requested()) {
    bool ran = false;
    try {
      ran = RunOnce();
    } catch (const std::exception& e) {
      Log(std::format("acquire failed: {}", e.what()));
    } catch (...) {
      Log("acquire failed");
    }
    if (!ran) InterruptibleSleep(stop, options_.idle_backoff);
  }
}

bool Worker::RunOnce() {
  const auto assignment = source_.Acquire(id_);
  if (!assignment) return false;
  Execute(*assignment);
  return true;
}

void Worker::Execute(const Assignment& a) {
  Log(std::format("acquired job={} generation={} attempt={}", a.job_id, a.generation, a.attempt));
  if (options_.after_acquire) options_.after_acquire(a);
  LeaseKeeper keeper(
      [this, &a] { return source_.Renew(a.job_id, a.generation) == CommitStatus::kAccepted; },
      RenewInterval(a.lease));

  const HandlerResult outcome = InvokeHandler(handler_, a, keeper.lost_token());
  keeper.Stop();
  if (keeper.lost()) {
    // Another assignment may already own the job; never report.
    Log(std::format("lease lost job={} generation={}; result discarded", a.job_id, a.generation));
    return;
  }
  if (options_.before_report) options_.before_report(a);
  Report(a, outcome);
}

void Worker::Report(const Assignment& a, const HandlerResult& outcome) {
  const char* kind = std::holds_alternative<JobSucceeded>(outcome) ? "success"
                     : std::holds_alternative<JobFailed>(outcome)  ? "failure"
                                                                   : "unknown";
  for (int attempt = 1; attempt <= options_.report_attempts; ++attempt) {
    try {
      CommitStatus status;
      if (const auto* ok = std::get_if<JobSucceeded>(&outcome)) {
        status = source_.Complete(a.job_id, a.generation, ok->result);
      } else if (const auto* failed = std::get_if<JobFailed>(&outcome)) {
        status = source_.Fail(a.job_id, a.generation, failed->reason, failed->retryable);
      } else {
        status = source_.MarkUnknown(a.job_id, a.generation, std::get<JobUnknown>(outcome).reason);
      }
      Log(std::format("reported {} job={} generation={} status={}", kind, a.job_id, a.generation,
                      ToString(status)));
      return;
    } catch (const std::exception& e) {
      Log(std::format("report attempt {}/{} failed job={} generation={}: {}", attempt,
                      options_.report_attempts, a.job_id, a.generation, e.what()));
    } catch (...) {
      Log(std::format("report attempt {}/{} failed job={} generation={}", attempt,
                      options_.report_attempts, a.job_id, a.generation));
    }
    if (attempt < options_.report_attempts) std::this_thread::sleep_for(options_.report_backoff);
  }
  Log(std::format("gave up reporting job={} generation={}; lease expiry will recover it",
                  a.job_id, a.generation));
}

void Worker::Log(const std::string& line) const {
  if (options_.log) options_.log(std::format("worker={} {}", id_, line));
}

}  // namespace epochflow
