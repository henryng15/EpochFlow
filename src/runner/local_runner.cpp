#include "runner/local_runner.h"

#include <stdexcept>
#include <string>
#include <utility>

namespace epochflow {
namespace {

const RunnerOptions& Validate(const RunnerOptions& o) {
  if (o.workers < 1) throw std::invalid_argument("workers must be >= 1");
  if (o.lease <= Duration::zero() || o.idle_backoff <= Duration::zero() ||
      o.reclaim_interval <= Duration::zero()) {
    throw std::invalid_argument("runner durations must be positive");
  }
  return o;
}

}  // namespace

LocalRunner::LocalRunner(JobStore& store, JobHandler handler, RunnerOptions options)
    : source_(store, Validate(options).lease) {
  WorkerOptions worker_options;
  worker_options.idle_backoff = options.idle_backoff;
  for (int i = 0; i < options.workers; ++i) {
    workers_.push_back(std::make_unique<Worker>(source_, handler,
                                                "local-worker-" + std::to_string(i),
                                                worker_options));
  }
  for (auto& worker : workers_) {
    threads_.emplace_back([w = worker.get()](std::stop_token stop) { w->Run(stop); });
  }
  reclaimer_ = std::make_unique<Reclaimer>(store, options.reclaim_interval);
}

LocalRunner::~LocalRunner() { Stop(); }

void LocalRunner::Stop() {
  for (auto& t : threads_) t.request_stop();
  for (auto& t : threads_) {
    if (t.joinable()) t.join();
  }
  if (reclaimer_) reclaimer_->Stop();
}

}  // namespace epochflow
