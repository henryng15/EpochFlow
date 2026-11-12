// Behavioral contract for JobStore. Tests observe behavior only through the
// interface and the harness clock, never absolute timestamps, so the same suite
// applies to stores whose time comes from elsewhere (for example the database).

#include "contract/job_store_contract.h"

#include <atomic>
#include <chrono>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

namespace epochflow::testing {
namespace {

using std::chrono::seconds;

constexpr Duration kLease = seconds{10};

JobSpec Spec(std::string payload, int max_attempts = 3) {
  return JobSpec{std::move(payload), max_attempts};
}

// --- Submit and Get -----------------------------------------------------------

TEST_P(JobStoreContractTest, SubmitCreatesPendingJobWithGenerationZero) {
  const JobId id = store().Submit(Spec("payload-a", 5));

  const auto job = store().Get(id);
  ASSERT_TRUE(job.has_value());
  EXPECT_EQ(job->id, id);
  EXPECT_EQ(job->state, JobState::kPending);
  EXPECT_EQ(job->generation, 0);
  EXPECT_EQ(job->attempts, 0);
  EXPECT_EQ(job->max_attempts, 5);
  EXPECT_EQ(job->payload, "payload-a");
  EXPECT_FALSE(job->worker_id.has_value());
  EXPECT_FALSE(job->lease_until.has_value());
  EXPECT_FALSE(job->result.has_value());
}

TEST_P(JobStoreContractTest, SubmitAssignsDistinctIds) {
  const JobId a = store().Submit(Spec("a"));
  const JobId b = store().Submit(Spec("b"));
  EXPECT_NE(a, b);
}

TEST_P(JobStoreContractTest, SubmitRejectsMaxAttemptsBelowOne) {
  EXPECT_THROW(store().Submit(Spec("x", 0)), std::invalid_argument);
}

TEST_P(JobStoreContractTest, GetUnknownJobReturnsNothing) {
  EXPECT_FALSE(store().Get(987654).has_value());
}

// --- Acquire ------------------------------------------------------------------

TEST_P(JobStoreContractTest, AcquireReturnsNothingWhenNoJobIsPending) {
  EXPECT_FALSE(store().Acquire("w1", kLease).has_value());
}

TEST_P(JobStoreContractTest, AcquireRejectsNonPositiveLease) {
  store().Submit(Spec("a"));
  EXPECT_THROW(store().Acquire("w1", Duration{0}), std::invalid_argument);
}

TEST_P(JobStoreContractTest, AcquireAssignsOldestPendingJobAsGenerationOne) {
  const JobId first = store().Submit(Spec("first"));
  store().Submit(Spec("second"));

  const auto a = store().Acquire("w1", kLease);
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(a->job_id, first);
  EXPECT_EQ(a->generation, 1);
  EXPECT_EQ(a->attempt, 1);
  EXPECT_EQ(a->payload, "first");
  EXPECT_EQ(a->lease, kLease);

  const auto job = store().Get(first);
  EXPECT_EQ(job->state, JobState::kRunning);
  EXPECT_EQ(job->generation, 1);
  EXPECT_EQ(job->attempts, 1);
  EXPECT_EQ(job->worker_id, "w1");
  EXPECT_EQ(job->lease_until, a->lease_until);
}

TEST_P(JobStoreContractTest, RunningJobIsNotAssignedAgain) {
  const JobId first = store().Submit(Spec("first"));
  const JobId second = store().Submit(Spec("second"));

  EXPECT_EQ(store().Acquire("w1", kLease)->job_id, first);
  EXPECT_EQ(store().Acquire("w2", kLease)->job_id, second);
  EXPECT_FALSE(store().Acquire("w3", kLease).has_value());
}

TEST_P(JobStoreContractTest, ConcurrentAcquiresAssignEachJobExactlyOnce) {
  constexpr int kJobs = 200;
  constexpr int kThreads = 8;
  for (int i = 0; i < kJobs; ++i) store().Submit(Spec("job"));

  std::vector<std::vector<JobId>> acquired(kThreads);
  std::atomic<bool> go{false};
  {
    std::vector<std::jthread> threads;
    for (int t = 0; t < kThreads; ++t) {
      threads.emplace_back([&, t] {
        while (!go.load()) std::this_thread::yield();
        const WorkerId worker = "w" + std::to_string(t);
        while (auto a = store().Acquire(worker, kLease)) acquired[t].push_back(a->job_id);
      });
    }
    go.store(true);
  }

  std::vector<JobId> all;
  for (const auto& ids : acquired) all.insert(all.end(), ids.begin(), ids.end());
  const std::set<JobId> unique(all.begin(), all.end());
  EXPECT_EQ(all.size(), static_cast<std::size_t>(kJobs));
  EXPECT_EQ(unique.size(), static_cast<std::size_t>(kJobs));
}

TEST_P(JobStoreContractTest, ConcurrentAcquiresFindAJobWhileAnyIsUnclaimed) {
  // Liveness: an acquirer must not come back empty just because another
  // acquirer is claiming a different row at the same moment. Each thread makes
  // exactly its share of calls, so every call has an unclaimed job available.
  constexpr int kThreads = 8;
  constexpr int kPerThread = 25;
  for (int i = 0; i < kThreads * kPerThread; ++i) store().Submit(Spec("job"));

  std::atomic<int> empty{0};
  std::atomic<bool> go{false};
  {
    std::vector<std::jthread> threads;
    for (int t = 0; t < kThreads; ++t) {
      threads.emplace_back([&, t] {
        while (!go.load()) std::this_thread::yield();
        for (int i = 0; i < kPerThread; ++i) {
          if (!store().Acquire("w" + std::to_string(t), kLease)) ++empty;
        }
      });
    }
    go.store(true);
  }
  EXPECT_EQ(empty.load(), 0);
}

// --- Complete -----------------------------------------------------------------

TEST_P(JobStoreContractTest, CompleteWithCurrentGenerationStoresResult) {
  const JobId id = store().Submit(Spec("a"));
  const auto a = store().Acquire("w1", kLease);

  EXPECT_EQ(store().Complete(id, a->generation, "done"), CommitStatus::kAccepted);

  const auto job = store().Get(id);
  EXPECT_EQ(job->state, JobState::kCompleted);
  EXPECT_EQ(job->result, "done");
  EXPECT_FALSE(job->worker_id.has_value());
  EXPECT_FALSE(job->lease_until.has_value());
}

TEST_P(JobStoreContractTest, CompleteUnknownJobReturnsNotFound) {
  EXPECT_EQ(store().Complete(987654, 1, "x"), CommitStatus::kNotFound);
}

TEST_P(JobStoreContractTest, CompleteOfPendingJobIsStale) {
  const JobId id = store().Submit(Spec("a"));
  EXPECT_EQ(store().Complete(id, 0, "x"), CommitStatus::kStale);
  EXPECT_EQ(store().Get(id)->state, JobState::kPending);
}

TEST_P(JobStoreContractTest, CompleteWithWrongGenerationIsStale) {
  const JobId id = store().Submit(Spec("a"));
  const auto a = store().Acquire("w1", kLease);

  EXPECT_EQ(store().Complete(id, a->generation + 1, "x"), CommitStatus::kStale);
  EXPECT_EQ(store().Get(id)->state, JobState::kRunning);
}

TEST_P(JobStoreContractTest, StaleWorkerCannotCompleteAfterReassignment) {
  const JobId id = store().Submit(Spec("a"));
  const auto old_assignment = store().Acquire("slow-worker", kLease);
  AdvanceTime(kLease + seconds{1});
  ASSERT_EQ(store().ReclaimExpired(), 1u);
  const auto new_assignment = store().Acquire("fresh-worker", kLease);
  ASSERT_GT(new_assignment->generation, old_assignment->generation);

  // The paused worker resumes and reports with its old generation.
  EXPECT_EQ(store().Complete(id, old_assignment->generation, "stale"), CommitStatus::kStale);
  EXPECT_EQ(store().Complete(id, new_assignment->generation, "fresh"), CommitStatus::kAccepted);
  EXPECT_EQ(store().Get(id)->result, "fresh");
}

TEST_P(JobStoreContractTest, CompleteAfterLeaseDeadlineButBeforeReclaimIsAccepted) {
  const JobId id = store().Submit(Spec("a"));
  const auto a = store().Acquire("w1", kLease);
  AdvanceTime(kLease + seconds{1});

  // No newer assignment exists, so the generation is still current.
  EXPECT_EQ(store().Complete(id, a->generation, "late"), CommitStatus::kAccepted);
  EXPECT_EQ(store().ReclaimExpired(), 0u);
  EXPECT_EQ(store().Get(id)->state, JobState::kCompleted);
}

TEST_P(JobStoreContractTest, CompleteAfterReclaimBeforeReacquireIsStale) {
  const JobId id = store().Submit(Spec("a"));
  const auto a = store().Acquire("w1", kLease);
  AdvanceTime(kLease + seconds{1});
  ASSERT_EQ(store().ReclaimExpired(), 1u);

  EXPECT_EQ(store().Complete(id, a->generation, "late"), CommitStatus::kStale);
  EXPECT_EQ(store().Get(id)->state, JobState::kPending);
}

TEST_P(JobStoreContractTest, DuplicateCompleteWithSameResultIsIdempotent) {
  const JobId id = store().Submit(Spec("a"));
  const auto a = store().Acquire("w1", kLease);
  ASSERT_EQ(store().Complete(id, a->generation, "done"), CommitStatus::kAccepted);

  // A retried request whose first acknowledgement was lost.
  EXPECT_EQ(store().Complete(id, a->generation, "done"), CommitStatus::kAccepted);
  EXPECT_EQ(store().Get(id)->result, "done");
}

TEST_P(JobStoreContractTest, DuplicateCompleteWithDifferentResultConflicts) {
  const JobId id = store().Submit(Spec("a"));
  const auto a = store().Acquire("w1", kLease);
  ASSERT_EQ(store().Complete(id, a->generation, "done"), CommitStatus::kAccepted);

  EXPECT_EQ(store().Complete(id, a->generation, "other"), CommitStatus::kConflict);
  EXPECT_EQ(store().Get(id)->result, "done");
}

// --- Renew --------------------------------------------------------------------

TEST_P(JobStoreContractTest, RenewKeepsJobFromBeingReclaimed) {
  const JobId id = store().Submit(Spec("a"));
  const auto a = store().Acquire("w1", kLease);

  AdvanceTime(kLease / 2);
  const auto renewed = store().Renew(id, a->generation, kLease);
  ASSERT_EQ(renewed.status, CommitStatus::kAccepted);
  EXPECT_GT(renewed.lease_until, a->lease_until);
  EXPECT_EQ(store().Get(id)->lease_until, renewed.lease_until);

  // Past the original deadline, inside the renewed one.
  AdvanceTime(kLease * 3 / 4);
  EXPECT_EQ(store().ReclaimExpired(), 0u);
  EXPECT_EQ(store().Get(id)->state, JobState::kRunning);
}

TEST_P(JobStoreContractTest, RenewUnknownJobReturnsNotFound) {
  EXPECT_EQ(store().Renew(987654, 1, kLease).status, CommitStatus::kNotFound);
}

TEST_P(JobStoreContractTest, RenewWithWrongGenerationIsStale) {
  const JobId id = store().Submit(Spec("a"));
  const auto a = store().Acquire("w1", kLease);
  EXPECT_EQ(store().Renew(id, a->generation - 1, kLease).status, CommitStatus::kStale);
}

TEST_P(JobStoreContractTest, RenewAfterReclaimIsStale) {
  const JobId id = store().Submit(Spec("a"));
  const auto a = store().Acquire("w1", kLease);
  AdvanceTime(kLease + seconds{1});
  ASSERT_EQ(store().ReclaimExpired(), 1u);

  EXPECT_EQ(store().Renew(id, a->generation, kLease).status, CommitStatus::kStale);
}

TEST_P(JobStoreContractTest, RenewOfCompletedJobIsStale) {
  const JobId id = store().Submit(Spec("a"));
  const auto a = store().Acquire("w1", kLease);
  ASSERT_EQ(store().Complete(id, a->generation, "done"), CommitStatus::kAccepted);
  EXPECT_EQ(store().Renew(id, a->generation, kLease).status, CommitStatus::kStale);
}

// --- ReclaimExpired -----------------------------------------------------------

TEST_P(JobStoreContractTest, ReclaimIgnoresUnexpiredLeases) {
  const JobId id = store().Submit(Spec("a"));
  store().Acquire("w1", kLease);
  AdvanceTime(kLease - seconds{1});

  EXPECT_EQ(store().ReclaimExpired(), 0u);
  EXPECT_EQ(store().Get(id)->state, JobState::kRunning);
}

TEST_P(JobStoreContractTest, ReclaimReturnsExpiredJobToPendingForNextGeneration) {
  const JobId id = store().Submit(Spec("a", 3));
  store().Acquire("w1", kLease);
  AdvanceTime(kLease + seconds{1});

  EXPECT_EQ(store().ReclaimExpired(), 1u);
  const auto job = store().Get(id);
  EXPECT_EQ(job->state, JobState::kPending);
  EXPECT_EQ(job->generation, 1);
  EXPECT_EQ(job->attempts, 1);
  EXPECT_FALSE(job->worker_id.has_value());
  EXPECT_FALSE(job->lease_until.has_value());
  EXPECT_TRUE(job->error.has_value());

  const auto again = store().Acquire("w2", kLease);
  EXPECT_EQ(again->generation, 2);
  EXPECT_EQ(again->attempt, 2);
}

TEST_P(JobStoreContractTest, ReclaimFailsExpiredJobWithNoAttemptsLeft) {
  const JobId id = store().Submit(Spec("a", 1));
  store().Acquire("w1", kLease);
  AdvanceTime(kLease + seconds{1});

  EXPECT_EQ(store().ReclaimExpired(), 1u);
  EXPECT_EQ(store().Get(id)->state, JobState::kFailed);
  EXPECT_FALSE(store().Acquire("w2", kLease).has_value());
}

TEST_P(JobStoreContractTest, ReclaimOnlyTouchesExpiredRunningJobs) {
  // t=0: `expired` and `done` are acquired; `done` completes.
  const JobId expired = store().Submit(Spec("expired"));
  const JobId done = store().Submit(Spec("done"));
  ASSERT_EQ(store().Acquire("w1", kLease)->job_id, expired);
  const auto d = store().Acquire("w2", kLease);
  ASSERT_EQ(d->job_id, done);
  ASSERT_EQ(store().Complete(done, d->generation, "ok"), CommitStatus::kAccepted);

  // t=6s: `fresh` is acquired with a later deadline; `pending` is never acquired.
  AdvanceTime(seconds{6});
  const JobId fresh = store().Submit(Spec("fresh"));
  const JobId pending = store().Submit(Spec("pending"));
  ASSERT_EQ(store().Acquire("w3", kLease)->job_id, fresh);

  // t=11s: only the lease of `expired` (deadline 10s) has passed.
  AdvanceTime(seconds{5});
  EXPECT_EQ(store().ReclaimExpired(), 1u);
  EXPECT_EQ(store().Get(expired)->state, JobState::kPending);
  EXPECT_EQ(store().Get(done)->state, JobState::kCompleted);
  EXPECT_EQ(store().Get(fresh)->state, JobState::kRunning);
  EXPECT_EQ(store().Get(pending)->state, JobState::kPending);
  EXPECT_EQ(store().Get(pending)->generation, 0);
}

// --- Fail ---------------------------------------------------------------------

TEST_P(JobStoreContractTest, RetryableFailureWithAttemptsLeftRequeuesJob) {
  const JobId id = store().Submit(Spec("a", 2));
  const auto a = store().Acquire("w1", kLease);

  EXPECT_EQ(store().Fail(id, a->generation, "transient", true), CommitStatus::kAccepted);
  const auto job = store().Get(id);
  EXPECT_EQ(job->state, JobState::kPending);
  EXPECT_EQ(job->error, "transient");
  EXPECT_FALSE(job->worker_id.has_value());

  EXPECT_EQ(store().Acquire("w2", kLease)->generation, a->generation + 1);
}

TEST_P(JobStoreContractTest, RetryableFailureOnLastAttemptFailsJob) {
  const JobId id = store().Submit(Spec("a", 1));
  const auto a = store().Acquire("w1", kLease);

  EXPECT_EQ(store().Fail(id, a->generation, "transient", true), CommitStatus::kAccepted);
  EXPECT_EQ(store().Get(id)->state, JobState::kFailed);
}

TEST_P(JobStoreContractTest, NonRetryableFailureFailsJobImmediately) {
  const JobId id = store().Submit(Spec("a", 5));
  const auto a = store().Acquire("w1", kLease);

  EXPECT_EQ(store().Fail(id, a->generation, "bad input", false), CommitStatus::kAccepted);
  const auto job = store().Get(id);
  EXPECT_EQ(job->state, JobState::kFailed);
  EXPECT_EQ(job->error, "bad input");
  EXPECT_FALSE(store().Acquire("w2", kLease).has_value());
}

TEST_P(JobStoreContractTest, FailWithWrongGenerationIsStale) {
  const JobId id = store().Submit(Spec("a"));
  const auto a = store().Acquire("w1", kLease);
  EXPECT_EQ(store().Fail(id, a->generation + 1, "x", false), CommitStatus::kStale);
  EXPECT_EQ(store().Get(id)->state, JobState::kRunning);
}

TEST_P(JobStoreContractTest, FailUnknownJobReturnsNotFound) {
  EXPECT_EQ(store().Fail(987654, 1, "x", false), CommitStatus::kNotFound);
}

TEST_P(JobStoreContractTest, DuplicateFinalFailureIsIdempotent) {
  const JobId id = store().Submit(Spec("a"));
  const auto a = store().Acquire("w1", kLease);
  ASSERT_EQ(store().Fail(id, a->generation, "bad input", false), CommitStatus::kAccepted);

  EXPECT_EQ(store().Fail(id, a->generation, "bad input", false), CommitStatus::kAccepted);
  EXPECT_EQ(store().Fail(id, a->generation, "other", false), CommitStatus::kConflict);
  EXPECT_EQ(store().Get(id)->error, "bad input");
}

TEST_P(JobStoreContractTest, CompleteAfterFinalFailureIsStale) {
  const JobId id = store().Submit(Spec("a"));
  const auto a = store().Acquire("w1", kLease);
  ASSERT_EQ(store().Fail(id, a->generation, "bad input", false), CommitStatus::kAccepted);
  EXPECT_EQ(store().Complete(id, a->generation, "done"), CommitStatus::kStale);
}

}  // namespace
}  // namespace epochflow::testing
