// LocalRunner runs real threads against the in-memory store and the system
// clock. Leases and intervals are short; waits are long, so tests check
// outcomes rather than timing.

#include "runner/local_runner.h"
#include "store/in_memory_job_store.h"
#include "support/forwarding_job_store.h"
#include "support/wait.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace epochflow::testing {
namespace {

using std::chrono::hours;
using std::chrono::milliseconds;

RunnerOptions FastOptions() {
  RunnerOptions options;
  options.workers = 4;
  options.lease = milliseconds{200};
  options.idle_backoff = milliseconds{5};
  options.reclaim_interval = milliseconds{20};
  return options;
}

bool AllTerminal(const JobStore& store, const std::vector<JobId>& ids) {
  for (JobId id : ids) {
    if (!IsTerminal(store.Get(id)->state)) return false;
  }
  return true;
}

// Delegates to an in-memory store but rejects every renewal of generation 1,
// as if the first worker's renewals never reached the store.
class RejectFirstGenerationRenewals final : public ForwardingJobStore {
 public:
  using ForwardingJobStore::ForwardingJobStore;
  RenewResult Renew(JobId id, Generation generation, Duration lease) override {
    if (generation == 1) return {CommitStatus::kStale, {}};
    return inner_.Renew(id, generation, lease);
  }
};

class LocalRunnerTest : public ::testing::Test {
 protected:
  SystemClock clock_;
  InMemoryJobStore store_{clock_};
};

TEST_F(LocalRunnerTest, CompletesEveryJobExactlyOnce) {
  std::vector<JobId> ids;
  for (int i = 0; i < 100; ++i) ids.push_back(store_.Submit({"p" + std::to_string(i), 3}));

  std::mutex mu;
  std::map<JobId, int> calls;
  LocalRunner runner(store_, [&](const Assignment& a, std::stop_token) -> HandlerResult {
    std::lock_guard lock(mu);
    ++calls[a.job_id];
    return JobSucceeded{"done:" + a.payload};
  }, FastOptions());

  ASSERT_TRUE(WaitUntil([&] { return AllTerminal(store_, ids); }));
  runner.Stop();
  for (JobId id : ids) {
    const auto job = store_.Get(id);
    EXPECT_EQ(job->state, JobState::kCompleted);
    EXPECT_EQ(job->result, "done:" + job->payload);
    EXPECT_EQ(job->attempts, 1);
    EXPECT_EQ(calls[id], 1);
  }
}

TEST_F(LocalRunnerTest, RetriesWhenHandlerThrows) {
  const JobId id = store_.Submit({"a", 3});
  LocalRunner runner(store_, [](const Assignment& a, std::stop_token) -> HandlerResult {
    if (a.attempt == 1) throw std::runtime_error("transient");
    return JobSucceeded{"ok"};
  }, FastOptions());

  ASSERT_TRUE(WaitUntil([&] { return IsTerminal(store_.Get(id)->state); }));
  const auto job = store_.Get(id);
  EXPECT_EQ(job->state, JobState::kCompleted);
  EXPECT_EQ(job->attempts, 2);
}

TEST_F(LocalRunnerTest, NonRetryableFailureIsNotRetried) {
  const JobId id = store_.Submit({"a", 5});
  LocalRunner runner(store_, [](const Assignment&, std::stop_token) -> HandlerResult {
    return JobFailed{"bad input", false};
  }, FastOptions());

  ASSERT_TRUE(WaitUntil([&] { return IsTerminal(store_.Get(id)->state); }));
  const auto job = store_.Get(id);
  EXPECT_EQ(job->state, JobState::kFailed);
  EXPECT_EQ(job->error, "bad input");
  EXPECT_EQ(job->attempts, 1);
}

TEST_F(LocalRunnerTest, RetryableFailureStopsAtMaxAttempts) {
  const JobId id = store_.Submit({"a", 3});
  LocalRunner runner(store_, [](const Assignment&, std::stop_token) -> HandlerResult {
    return JobFailed{"still failing", true};
  }, FastOptions());

  ASSERT_TRUE(WaitUntil([&] { return IsTerminal(store_.Get(id)->state); }));
  const auto job = store_.Get(id);
  EXPECT_EQ(job->state, JobState::kFailed);
  EXPECT_EQ(job->attempts, 3);
}

TEST_F(LocalRunnerTest, RenewalKeepsLongJobOnItsFirstAttempt) {
  const JobId id = store_.Submit({"a", 3});
  // Runs for three lease periods; only renewal keeps it from being reclaimed.
  LocalRunner runner(store_, [](const Assignment&, std::stop_token) -> HandlerResult {
    std::this_thread::sleep_for(milliseconds{600});
    return JobSucceeded{"ok"};
  }, FastOptions());

  ASSERT_TRUE(WaitUntil([&] { return IsTerminal(store_.Get(id)->state); }));
  const auto job = store_.Get(id);
  EXPECT_EQ(job->state, JobState::kCompleted);
  EXPECT_EQ(job->attempts, 1);
}

TEST_F(LocalRunnerTest, LostLeaseCancelsTaskAndDiscardsItsResult) {
  RejectFirstGenerationRenewals store(store_);
  const JobId id = store.Submit({"a", 3});
  std::atomic<bool> first_attempt_cancelled{false};

  LocalRunner runner(store, [&](const Assignment& a, std::stop_token lease_lost) -> HandlerResult {
    if (a.generation == 1) {
      // Simulates a worker that would keep going; it must notice the lost lease.
      while (!lease_lost.stop_requested()) std::this_thread::sleep_for(milliseconds{1});
      first_attempt_cancelled = true;
      return JobSucceeded{"from-stale-attempt"};
    }
    return JobSucceeded{"from-attempt-" + std::to_string(a.attempt)};
  }, FastOptions());

  ASSERT_TRUE(WaitUntil([&] { return IsTerminal(store.Get(id)->state); }));
  const auto job = store.Get(id);
  EXPECT_TRUE(first_attempt_cancelled.load());
  EXPECT_EQ(job->state, JobState::kCompleted);
  EXPECT_EQ(job->result, "from-attempt-2");
  EXPECT_EQ(job->generation, 2);
}

TEST_F(LocalRunnerTest, StopReturnsPromptlyWhenIdle) {
  RunnerOptions options = FastOptions();
  options.idle_backoff = hours{1};
  options.reclaim_interval = hours{1};
  LocalRunner runner(store_, [](const Assignment&, std::stop_token) -> HandlerResult {
    return JobSucceeded{"ok"};
  }, options);
  std::this_thread::sleep_for(milliseconds{20});

  const auto start = std::chrono::steady_clock::now();
  runner.Stop();
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds{1});
}

TEST_F(LocalRunnerTest, RejectsInvalidOptions) {
  const auto handler = [](const Assignment&, std::stop_token) -> HandlerResult {
    return JobSucceeded{"ok"};
  };
  RunnerOptions no_workers = FastOptions();
  no_workers.workers = 0;
  EXPECT_THROW(LocalRunner(store_, handler, no_workers), std::invalid_argument);

  RunnerOptions no_lease = FastOptions();
  no_lease.lease = milliseconds{0};
  EXPECT_THROW(LocalRunner(store_, handler, no_lease), std::invalid_argument);
}

}  // namespace
}  // namespace epochflow::testing
