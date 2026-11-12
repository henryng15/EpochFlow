#include "runner/local_runner.h"

#include "worker/lease_keeper.h"

#include <condition_variable>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace epochflow {
namespace {

// Sleeps for `duration` or until a stop is requested, whichever comes first.
void InterruptibleSleep(std::stop_token stop, Duration duration) {
  std::mutex mu;
  std::condition_variable_any wake;
  std::unique_lock lock(mu);
  wake.wait_for(lock, stop, duration, [] { return false; });
}

void Validate(const RunnerOptions& o) {
  if (o.workers < 1) throw std::invalid_argument("workers must be >= 1");
  if (o.lease <= Duration::zero() || o.renew_interval <= Duration::zero() ||
      o.idle_backoff <= Duration::zero() || o.reclaim_interval <= Duration::zero()) {
    throw std::invalid_argument("runner durations must be positive");
  }
  if (o.renew_interval >= o.lease) {
    throw std::invalid_argument("renew_interval must be shorter than lease");
  }
}

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

}  // namespace

LocalRunner::LocalRunner(JobStore& store, JobHandler handler, RunnerOptions options)
    : store_(store), handler_(std::move(handler)), options_(options) {
  Validate(options_);
  threads_.reserve(static_cast<std::size_t>(options_.workers) + 1);
  for (int i = 0; i < options_.workers; ++i) {
    threads_.emplace_back([this, worker = "local-worker-" + std::to_string(i)](
                              std::stop_token stop) { WorkerLoop(stop, worker); });
  }
  threads_.emplace_back([this](std::stop_token stop) { ReclaimLoop(stop); });
}

LocalRunner::~LocalRunner() { Stop(); }

void LocalRunner::Stop() {
  for (auto& t : threads_) t.request_stop();
  for (auto& t : threads_) {
    if (t.joinable()) t.join();
  }
}

void LocalRunner::WorkerLoop(std::stop_token stop, const WorkerId& worker) {
  while (!stop.stop_requested()) {
    try {
      const auto assignment = store_.Acquire(worker, options_.lease);
      if (!assignment) {
        InterruptibleSleep(stop, options_.idle_backoff);
        continue;
      }
      RunAssignment(*assignment);
    } catch (...) {
      // A store error leaves any held lease to expire and be reclaimed; the
      // worker backs off and keeps serving.
      InterruptibleSleep(stop, options_.idle_backoff);
    }
  }
}

void LocalRunner::RunAssignment(const Assignment& assignment) {
  LeaseKeeper keeper(
      [this, &assignment] {
        return store_.Renew(assignment.job_id, assignment.generation, options_.lease).status ==
               CommitStatus::kAccepted;
      },
      options_.renew_interval);

  const HandlerResult outcome = InvokeHandler(handler_, assignment, keeper.lost_token());
  keeper.Stop();
  // Without a confirmed lease another assignment may already own the job;
  // reporting would be rejected anyway, so skip it.
  if (keeper.lost()) return;

  // kStale or kConflict here means a newer assignment owns the job; the
  // result is dropped. Fencing in the store makes that safe.
  if (const auto* ok = std::get_if<JobSucceeded>(&outcome)) {
    store_.Complete(assignment.job_id, assignment.generation, ok->result);
  } else {
    const auto& failed = std::get<JobFailed>(outcome);
    store_.Fail(assignment.job_id, assignment.generation, failed.reason, failed.retryable);
  }
}

void LocalRunner::ReclaimLoop(std::stop_token stop) {
  while (!stop.stop_requested()) {
    try {
      store_.ReclaimExpired();
    } catch (...) {
      // Retried on the next tick.
    }
    InterruptibleSleep(stop, options_.reclaim_interval);
  }
}

}  // namespace epochflow
