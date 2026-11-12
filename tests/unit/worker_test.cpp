#include "worker/worker.h"
#include "store/in_memory_job_store.h"
#include "support/manual_clock.h"
#include "support/wait.h"
#include "worker/work_source.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace epochflow::testing {
namespace {

using std::chrono::milliseconds;
using std::chrono::seconds;

// Delegates to a StoreWorkSource and injects transport failures on Complete.
class FlakySource final : public WorkSource {
 public:
  explicit FlakySource(JobStore& store) : inner_(store, seconds{10}) {}

  // Complete throws this many times before reaching the store.
  int fail_before_apply = 0;
  // Complete reaches the store, then throws this many times (lost ack).
  int fail_after_apply = 0;
  bool acquire_throws = false;
  std::atomic<int> complete_calls{0};

  std::optional<Assignment> Acquire(const WorkerId& w) override {
    if (acquire_throws) throw std::runtime_error("coordinator unreachable");
    return inner_.Acquire(w);
  }
  CommitStatus Renew(JobId id, Generation g) override { return inner_.Renew(id, g); }
  CommitStatus Complete(JobId id, Generation g, const std::string& r) override {
    ++complete_calls;
    if (fail_before_apply > 0) {
      --fail_before_apply;
      throw std::runtime_error("request lost");
    }
    const CommitStatus status = inner_.Complete(id, g, r);
    if (fail_after_apply > 0) {
      --fail_after_apply;
      throw std::runtime_error("acknowledgement lost");
    }
    return status;
  }
  CommitStatus Fail(JobId id, Generation g, const std::string& r, bool retry) override {
    return inner_.Fail(id, g, r, retry);
  }
  CommitStatus MarkUnknown(JobId id, Generation g, const std::string& r) override {
    return inner_.MarkUnknown(id, g, r);
  }
  OperationOutcome ApplyLedger(JobId id, Generation g, const std::string& op,
                               const LedgerEntry& e) override {
    return inner_.ApplyLedger(id, g, op, e);
  }
  ExternalBegin BeginExternal(JobId id, Generation g, const std::string& op,
                              const std::string& req) override {
    return inner_.BeginExternal(id, g, op, req);
  }
  CommitStatus ConfirmExternal(const std::string& op, const std::string& req,
                               const std::string& conf) override {
    return inner_.ConfirmExternal(op, req, conf);
  }

 private:
  StoreWorkSource inner_;
};

HandlerResult Succeed(const Assignment& a, std::stop_token) {
  return JobSucceeded{"done:" + a.payload};
}

WorkerOptions FastOptions() {
  WorkerOptions options;
  options.idle_backoff = milliseconds{5};
  options.report_attempts = 3;
  options.report_backoff = milliseconds{1};
  return options;
}

class WorkerTest : public ::testing::Test {
 protected:
  ManualClock clock_;
  InMemoryJobStore store_{clock_};
  FlakySource source_{store_};
};

TEST_F(WorkerTest, RunOnceReturnsFalseWhenNoJobIsAvailable) {
  Worker worker(source_, Succeed, "w1", FastOptions());
  EXPECT_FALSE(worker.RunOnce());
}

TEST_F(WorkerTest, RunOnceExecutesAndCompletesOneJob) {
  const JobId id = store_.Submit({"a", 3});
  Worker worker(source_, Succeed, "w1", FastOptions());

  EXPECT_TRUE(worker.RunOnce());
  EXPECT_EQ(store_.Get(id)->state, JobState::kCompleted);
  EXPECT_EQ(store_.Get(id)->result, "done:a");
}

TEST_F(WorkerTest, RetriesReportWhenRequestIsLost) {
  const JobId id = store_.Submit({"a", 3});
  source_.fail_before_apply = 2;
  Worker worker(source_, Succeed, "w1", FastOptions());

  worker.RunOnce();
  EXPECT_EQ(source_.complete_calls.load(), 3);
  EXPECT_EQ(store_.Get(id)->state, JobState::kCompleted);
}

TEST_F(WorkerTest, RetriesReportWhenAcknowledgementIsLost) {
  const JobId id = store_.Submit({"a", 3});
  // The first Complete is applied but its response never arrives; the retry
  // must be recognized as a duplicate, not rejected as stale.
  source_.fail_after_apply = 1;
  Worker worker(source_, Succeed, "w1", FastOptions());

  worker.RunOnce();
  EXPECT_EQ(source_.complete_calls.load(), 2);
  const auto job = store_.Get(id);
  EXPECT_EQ(job->state, JobState::kCompleted);
  EXPECT_EQ(job->result, "done:a");
}

TEST_F(WorkerTest, GivesUpReportingAfterMaxAttemptsWithoutThrowing) {
  const JobId id = store_.Submit({"a", 3});
  source_.fail_before_apply = 100;
  Worker worker(source_, Succeed, "w1", FastOptions());

  EXPECT_NO_THROW(worker.RunOnce());
  EXPECT_EQ(source_.complete_calls.load(), 3);
  // Left RUNNING; lease expiry and reclaim will recover it.
  EXPECT_EQ(store_.Get(id)->state, JobState::kRunning);
}

TEST_F(WorkerTest, RunSurvivesAcquireErrorsAndStopsOnRequest) {
  source_.acquire_throws = true;
  Worker worker(source_, Succeed, "w1", FastOptions());

  std::jthread thread([&](std::stop_token stop) { worker.Run(stop); });
  std::this_thread::sleep_for(milliseconds{30});
  thread.request_stop();
  thread.join();
  SUCCEED();
}


TEST_F(WorkerTest, UnknownOutcomeParksTheJob) {
  const JobId id = store_.Submit({"a", 3});
  Worker worker(source_, [](const Assignment&, std::stop_token) -> HandlerResult {
    return JobUnknown{"provider timed out"};
  }, "w1", FastOptions());

  worker.RunOnce();
  const auto job = store_.Get(id);
  EXPECT_EQ(job->state, JobState::kUnknown);
  EXPECT_EQ(job->error, "provider timed out");
}

TEST_F(WorkerTest, HooksRunAfterAcquireAndBeforeReport) {
  const JobId id = store_.Submit({"a", 3});
  std::vector<std::string> events;
  WorkerOptions options = FastOptions();
  options.after_acquire = [&](const Assignment& a) {
    events.push_back("acquired " + std::to_string(a.job_id));
  };
  options.before_report = [&](const Assignment& a) {
    events.push_back("reporting " + std::to_string(a.job_id) + " state=" +
                     std::string(ToString(store_.Get(a.job_id)->state)));
  };
  Worker worker(source_, [&](const Assignment&, std::stop_token) -> HandlerResult {
    events.push_back("handler");
    return JobSucceeded{"ok"};
  }, "w1", options);

  worker.RunOnce();
  EXPECT_EQ(events, (std::vector<std::string>{"acquired " + std::to_string(id), "handler",
                                              "reporting " + std::to_string(id) + " state=RUNNING"}));
  EXPECT_EQ(store_.Get(id)->state, JobState::kCompleted);
}

}  // namespace
}  // namespace epochflow::testing
