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


// --- ApplyLedger (idempotent owned side effect) -------------------------------

const LedgerEntry kPayment{"alice", 1000};

TEST_P(JobStoreContractTest, ApplyLedgerCreatesEntryOnce) {
  const JobId id = store().Submit(Spec("pay"));
  const auto a = store().Acquire("w1", kLease);

  const auto first = store().ApplyLedger(id, a->generation, "order-1", kPayment);
  EXPECT_EQ(first.status, CommitStatus::kAccepted);
  EXPECT_FALSE(first.deduplicated);
  EXPECT_FALSE(first.result.empty());

  // Retry by the same attempt (for example after a lost acknowledgement).
  const auto again = store().ApplyLedger(id, a->generation, "order-1", kPayment);
  EXPECT_EQ(again.status, CommitStatus::kAccepted);
  EXPECT_TRUE(again.deduplicated);
  EXPECT_EQ(again.result, first.result);
}

TEST_P(JobStoreContractTest, ApplyLedgerAfterCrashAndReassignmentDeduplicates) {
  const JobId id = store().Submit(Spec("pay"));
  const auto crashed = store().Acquire("w1", kLease);
  const auto first = store().ApplyLedger(id, crashed->generation, "order-1", kPayment);
  // The worker dies before reporting; the job is recovered and re-run.
  AdvanceTime(kLease + seconds{1});
  ASSERT_EQ(store().ReclaimExpired(), 1u);
  const auto retry = store().Acquire("w2", kLease);

  const auto second = store().ApplyLedger(id, retry->generation, "order-1", kPayment);
  EXPECT_EQ(second.status, CommitStatus::kAccepted);
  EXPECT_TRUE(second.deduplicated);
  EXPECT_EQ(second.result, first.result);
}

TEST_P(JobStoreContractTest, ApplyLedgerFromStaleGenerationChangesNothing) {
  const JobId id = store().Submit(Spec("pay"));
  const auto paused = store().Acquire("w1", kLease);
  AdvanceTime(kLease + seconds{1});
  ASSERT_EQ(store().ReclaimExpired(), 1u);
  const auto fresh = store().Acquire("w2", kLease);

  // The paused worker resumes and tries to apply the effect first.
  EXPECT_EQ(store().ApplyLedger(id, paused->generation, "order-1", kPayment).status,
            CommitStatus::kStale);
  // So the current attempt is the one that creates it.
  const auto current = store().ApplyLedger(id, fresh->generation, "order-1", kPayment);
  EXPECT_EQ(current.status, CommitStatus::kAccepted);
  EXPECT_FALSE(current.deduplicated);
}

TEST_P(JobStoreContractTest, ApplyLedgerRequiresARunningAssignment) {
  const JobId id = store().Submit(Spec("pay"));
  EXPECT_EQ(store().ApplyLedger(id, 0, "order-1", kPayment).status, CommitStatus::kStale);
  EXPECT_EQ(store().ApplyLedger(987654, 1, "order-1", kPayment).status, CommitStatus::kNotFound);
}

TEST_P(JobStoreContractTest, ApplyLedgerWithDifferentContentConflicts) {
  const JobId id = store().Submit(Spec("pay"));
  const auto a = store().Acquire("w1", kLease);
  const auto first = store().ApplyLedger(id, a->generation, "order-1", kPayment);

  const auto other = store().ApplyLedger(id, a->generation, "order-1", LedgerEntry{"alice", 999});
  EXPECT_EQ(other.status, CommitStatus::kConflict);
  // The original entry is untouched and still returned for matching retries.
  EXPECT_EQ(store().ApplyLedger(id, a->generation, "order-1", kPayment).result, first.result);
}

TEST_P(JobStoreContractTest, ApplyLedgerDeduplicatesAcrossDuplicateJobs) {
  // A client submitted the same logical payment twice.
  const JobId first_job = store().Submit(Spec("pay"));
  const JobId second_job = store().Submit(Spec("pay"));
  const auto a = store().Acquire("w1", kLease);
  const auto b = store().Acquire("w2", kLease);
  ASSERT_EQ(a->job_id, first_job);
  ASSERT_EQ(b->job_id, second_job);

  const auto first = store().ApplyLedger(first_job, a->generation, "order-1", kPayment);
  const auto second = store().ApplyLedger(second_job, b->generation, "order-1", kPayment);
  EXPECT_FALSE(first.deduplicated);
  EXPECT_TRUE(second.deduplicated);
  EXPECT_EQ(second.result, first.result);
}

TEST_P(JobStoreContractTest, ApplyLedgerRejectsMissingIdentifiers) {
  const JobId id = store().Submit(Spec("pay"));
  const auto a = store().Acquire("w1", kLease);
  EXPECT_THROW(store().ApplyLedger(id, a->generation, "", kPayment), std::invalid_argument);
  EXPECT_THROW(store().ApplyLedger(id, a->generation, "order-1", LedgerEntry{"", 1}),
               std::invalid_argument);
}

TEST_P(JobStoreContractTest, ConcurrentApplyOfOneOperationCreatesOneEntry) {
  constexpr int kThreads = 8;
  std::vector<Assignment> assignments;
  for (int i = 0; i < kThreads; ++i) store().Submit(Spec("pay"));
  for (int i = 0; i < kThreads; ++i) assignments.push_back(*store().Acquire("w", kLease));

  std::vector<OperationOutcome> outcomes(kThreads);
  std::atomic<bool> go{false};
  {
    std::vector<std::jthread> threads;
    for (int t = 0; t < kThreads; ++t) {
      threads.emplace_back([&, t] {
        while (!go.load()) std::this_thread::yield();
        outcomes[t] = store().ApplyLedger(assignments[t].job_id, assignments[t].generation,
                                          "order-race", kPayment);
      });
    }
    go.store(true);
  }

  int created = 0;
  for (const auto& o : outcomes) {
    EXPECT_EQ(o.status, CommitStatus::kAccepted);
    EXPECT_EQ(o.result, outcomes[0].result);
    if (!o.deduplicated) ++created;
  }
  EXPECT_EQ(created, 1);
}


// --- External operations (intent before a non-idempotent call) ----------------

struct Reassigned {
  JobId id;
  Assignment first;
  Assignment second;
};

class ExternalHelpers {
 public:
  // Submits a job, acquires it, lets the lease expire, and re-acquires it.
  static Reassigned SubmitAndReassign(JobStore& store, const std::function<void(Duration)>& advance,
                                      const std::function<void(const Assignment&)>& during_first) {
    const JobId id = store.Submit(JobSpec{"charge", 3});
    const Assignment first = *store.Acquire("w1", kLease);
    during_first(first);
    advance(kLease + seconds{1});
    EXPECT_EQ(store.ReclaimExpired(), 1u);
    const Assignment second = *store.Acquire("w2", kLease);
    return {id, first, second};
  }
};

TEST_P(JobStoreContractTest, BeginExternalRecordsIntentForThisAttempt) {
  const JobId id = store().Submit(Spec("charge"));
  const auto a = store().Acquire("w1", kLease);

  const auto begin = store().BeginExternal(id, a->generation, "charge-1", "card=4242;cents=500");
  EXPECT_EQ(begin.status, CommitStatus::kAccepted);
  EXPECT_EQ(begin.state, ExternalState::kStart);
  // The same attempt retrying its request (lost acknowledgement) may proceed.
  EXPECT_EQ(store().BeginExternal(id, a->generation, "charge-1", "card=4242;cents=500").state,
            ExternalState::kStart);
}

TEST_P(JobStoreContractTest, UnconfirmedIntentFromEarlierAttemptIsInDoubt) {
  // The first attempt recorded its intent, maybe called the provider, and died.
  const auto r = ExternalHelpers::SubmitAndReassign(
      store(), [&](Duration d) { AdvanceTime(d); },
      [&](const Assignment& a) {
        ASSERT_EQ(store().BeginExternal(a.job_id, a.generation, "charge-1", "req").state,
                  ExternalState::kStart);
      });

  const auto begin = store().BeginExternal(r.id, r.second.generation, "charge-1", "req");
  EXPECT_EQ(begin.status, CommitStatus::kAccepted);
  EXPECT_EQ(begin.state, ExternalState::kInDoubt);
}

TEST_P(JobStoreContractTest, ConfirmedExternalResultIsReusedByLaterAttempt) {
  const auto r = ExternalHelpers::SubmitAndReassign(
      store(), [&](Duration d) { AdvanceTime(d); },
      [&](const Assignment& a) {
        store().BeginExternal(a.job_id, a.generation, "charge-1", "req");
        ASSERT_EQ(store().ConfirmExternal("charge-1", "req", "ch_123"), CommitStatus::kAccepted);
      });

  const auto begin = store().BeginExternal(r.id, r.second.generation, "charge-1", "req");
  EXPECT_EQ(begin.state, ExternalState::kConfirmed);
  EXPECT_EQ(begin.result, "ch_123");
}

TEST_P(JobStoreContractTest, ConfirmFromStaleAttemptIsStillRecorded) {
  // The provider answered the first attempt after its job was reassigned.
  const auto r = ExternalHelpers::SubmitAndReassign(
      store(), [&](Duration d) { AdvanceTime(d); },
      [&](const Assignment& a) { store().BeginExternal(a.job_id, a.generation, "charge-1", "req"); });

  EXPECT_EQ(store().ConfirmExternal("charge-1", "req", "ch_late"), CommitStatus::kAccepted);
  EXPECT_EQ(store().BeginExternal(r.id, r.second.generation, "charge-1", "req").state,
            ExternalState::kConfirmed);
}

TEST_P(JobStoreContractTest, ConfirmExternalIsIdempotentAndDetectsConflicts) {
  const JobId id = store().Submit(Spec("charge"));
  const auto a = store().Acquire("w1", kLease);
  store().BeginExternal(id, a->generation, "charge-1", "req");

  EXPECT_EQ(store().ConfirmExternal("charge-1", "req", "ch_1"), CommitStatus::kAccepted);
  EXPECT_EQ(store().ConfirmExternal("charge-1", "req", "ch_1"), CommitStatus::kAccepted);
  EXPECT_EQ(store().ConfirmExternal("charge-1", "req", "ch_2"), CommitStatus::kConflict);
  EXPECT_EQ(store().ConfirmExternal("charge-1", "other", "ch_1"), CommitStatus::kConflict);
  EXPECT_EQ(store().ConfirmExternal("missing", "req", "ch_1"), CommitStatus::kNotFound);
}

TEST_P(JobStoreContractTest, BeginExternalIsFencedAndChecksContent) {
  const JobId id = store().Submit(Spec("charge"));
  const auto a = store().Acquire("w1", kLease);
  EXPECT_EQ(store().BeginExternal(id, a->generation + 1, "charge-1", "req").status,
            CommitStatus::kStale);
  EXPECT_EQ(store().BeginExternal(987654, 1, "charge-1", "req").status, CommitStatus::kNotFound);
  EXPECT_THROW(store().BeginExternal(id, a->generation, "", "req"), std::invalid_argument);

  store().BeginExternal(id, a->generation, "charge-1", "req");
  EXPECT_EQ(store().BeginExternal(id, a->generation, "charge-1", "different").status,
            CommitStatus::kConflict);
}

TEST_P(JobStoreContractTest, LedgerAndExternalShareOneOperationNamespace) {
  const JobId id = store().Submit(Spec("mixed"));
  const auto a = store().Acquire("w1", kLease);
  ASSERT_EQ(store().ApplyLedger(id, a->generation, "op-1", LedgerEntry{"bob", 1}).status,
            CommitStatus::kAccepted);
  EXPECT_EQ(store().BeginExternal(id, a->generation, "op-1", "req").status,
            CommitStatus::kConflict);

  store().BeginExternal(id, a->generation, "op-2", "req");
  EXPECT_EQ(store().ApplyLedger(id, a->generation, "op-2", LedgerEntry{"bob", 1}).status,
            CommitStatus::kConflict);
}

// --- UNKNOWN and reconciliation ------------------------------------------------

TEST_P(JobStoreContractTest, MarkUnknownParksJobOutsideRetryAndRecovery) {
  const JobId id = store().Submit(Spec("charge", 3));
  const auto a = store().Acquire("w1", kLease);

  EXPECT_EQ(store().MarkUnknown(id, a->generation, "provider timed out"), CommitStatus::kAccepted);
  auto job = store().Get(id);
  EXPECT_EQ(job->state, JobState::kUnknown);
  EXPECT_EQ(job->error, "provider timed out");
  EXPECT_FALSE(job->worker_id.has_value());
  EXPECT_FALSE(job->lease_until.has_value());

  // Never re-run automatically and never reclaimed.
  AdvanceTime(kLease * 10);
  EXPECT_EQ(store().ReclaimExpired(), 0u);
  EXPECT_FALSE(store().Acquire("w2", kLease).has_value());
  EXPECT_EQ(store().Get(id)->state, JobState::kUnknown);
}

TEST_P(JobStoreContractTest, MarkUnknownIsFencedAndIdempotent) {
  const JobId id = store().Submit(Spec("charge"));
  const auto a = store().Acquire("w1", kLease);
  EXPECT_EQ(store().MarkUnknown(id, a->generation + 1, "x"), CommitStatus::kStale);
  EXPECT_EQ(store().MarkUnknown(987654, 1, "x"), CommitStatus::kNotFound);

  ASSERT_EQ(store().MarkUnknown(id, a->generation, "timeout"), CommitStatus::kAccepted);
  EXPECT_EQ(store().MarkUnknown(id, a->generation, "timeout"), CommitStatus::kAccepted);
  EXPECT_EQ(store().MarkUnknown(id, a->generation, "other"), CommitStatus::kConflict);
  // A late completion from the same attempt cannot override the UNKNOWN.
  EXPECT_EQ(store().Complete(id, a->generation, "done"), CommitStatus::kStale);
}

TEST_P(JobStoreContractTest, ReconcileResolvesUnknownWithEvidence) {
  const JobId paid = store().Submit(Spec("a"));
  const JobId unpaid = store().Submit(Spec("b"));
  const auto p = store().Acquire("w1", kLease);
  const auto u = store().Acquire("w1", kLease);
  store().MarkUnknown(paid, p->generation, "timeout");
  store().MarkUnknown(unpaid, u->generation, "timeout");

  EXPECT_EQ(store().Reconcile(paid, JobState::kCompleted, "provider shows ch_9"),
            CommitStatus::kAccepted);
  EXPECT_EQ(store().Reconcile(unpaid, JobState::kFailed, "provider has no charge"),
            CommitStatus::kAccepted);
  EXPECT_EQ(store().Get(paid)->state, JobState::kCompleted);
  EXPECT_EQ(store().Get(paid)->result, "provider shows ch_9");
  EXPECT_EQ(store().Get(unpaid)->state, JobState::kFailed);
  EXPECT_EQ(store().Get(unpaid)->error, "provider has no charge");
}

TEST_P(JobStoreContractTest, ReconcileIsIdempotentAndOnlyForUnknownJobs) {
  const JobId id = store().Submit(Spec("a"));
  EXPECT_EQ(store().Reconcile(id, JobState::kCompleted, "x"), CommitStatus::kStale);  // PENDING
  const auto a = store().Acquire("w1", kLease);
  EXPECT_EQ(store().Reconcile(id, JobState::kCompleted, "x"), CommitStatus::kStale);  // RUNNING
  EXPECT_EQ(store().Reconcile(987654, JobState::kCompleted, "x"), CommitStatus::kNotFound);

  store().MarkUnknown(id, a->generation, "timeout");
  ASSERT_EQ(store().Reconcile(id, JobState::kCompleted, "ch_1"), CommitStatus::kAccepted);
  EXPECT_EQ(store().Reconcile(id, JobState::kCompleted, "ch_1"), CommitStatus::kAccepted);
  EXPECT_EQ(store().Reconcile(id, JobState::kCompleted, "ch_2"), CommitStatus::kStale);
  EXPECT_EQ(store().Reconcile(id, JobState::kFailed, "ch_1"), CommitStatus::kStale);
  EXPECT_EQ(store().Get(id)->result, "ch_1");
}

TEST_P(JobStoreContractTest, ReconcileRejectsNonTerminalOutcome) {
  const JobId id = store().Submit(Spec("a"));
  EXPECT_THROW(store().Reconcile(id, JobState::kPending, "x"), std::invalid_argument);
  EXPECT_THROW(store().Reconcile(id, JobState::kUnknown, "x"), std::invalid_argument);
}

// --- Operation-level reconciliation (ADR 0006) ---------------------------------

constexpr const char* kOp = "charge-1";
constexpr const char* kReq = "account=alice;amount_cents=500";

// A job whose external call was started and whose outcome is unknown.
JobId UnknownWithIntent(JobStore& store) {
  const JobId id = store.Submit(JobSpec{"charge", 3});
  const auto a = store.Acquire("w1", kLease);
  EXPECT_EQ(a->job_id, id);
  EXPECT_EQ(store.BeginExternal(id, a->generation, kOp, kReq).state, ExternalState::kStart);
  EXPECT_EQ(store.MarkUnknown(id, a->generation, "provider timed out"), CommitStatus::kAccepted);
  return id;
}

// What a later job using the same operation id is told.
ExternalBegin BeginFromNewJob(JobStore& store) {
  const JobId id = store.Submit(JobSpec{"retry", 3});
  const auto a = store.Acquire("w2", kLease);
  EXPECT_EQ(a->job_id, id);
  return store.BeginExternal(id, a->generation, kOp, kReq);
}

TEST_P(JobStoreContractTest, ReconcileExternalConfirmsTheOperationAndCompletesTheJob) {
  const JobId id = UnknownWithIntent(store());
  ASSERT_EQ(store().ReconcileExternal(id, kOp, JobState::kCompleted, "ch_9"),
            CommitStatus::kAccepted);
  EXPECT_EQ(store().Get(id)->state, JobState::kCompleted);
  EXPECT_EQ(store().Get(id)->result, "ch_9");
  // A later attempt reuses the operator's evidence instead of being in doubt.
  const auto later = BeginFromNewJob(store());
  EXPECT_EQ(later.state, ExternalState::kConfirmed);
  EXPECT_EQ(later.result, "ch_9");
}

TEST_P(JobStoreContractTest, ReconcileExternalAsFailedMakesTheOperationTerminal) {
  const JobId id = UnknownWithIntent(store());
  ASSERT_EQ(store().ReconcileExternal(id, kOp, JobState::kFailed, "provider has no charge"),
            CommitStatus::kAccepted);
  EXPECT_EQ(store().Get(id)->state, JobState::kFailed);
  EXPECT_EQ(store().Get(id)->error, "provider has no charge");
  // Reusing the id replays the failure; a provider answer now contradicts it.
  const auto later = BeginFromNewJob(store());
  EXPECT_EQ(later.status, CommitStatus::kAccepted);
  EXPECT_EQ(later.state, ExternalState::kFailed);
  EXPECT_EQ(later.result, "provider has no charge");
  EXPECT_EQ(store().ConfirmExternal(kOp, kReq, "ch_1"), CommitStatus::kConflict);
}

TEST_P(JobStoreContractTest, ReconcileExternalIsIdempotentAndRejectsOtherDecisions) {
  const JobId id = UnknownWithIntent(store());
  ASSERT_EQ(store().ReconcileExternal(id, kOp, JobState::kCompleted, "ch_9"),
            CommitStatus::kAccepted);
  EXPECT_EQ(store().ReconcileExternal(id, kOp, JobState::kCompleted, "ch_9"),
            CommitStatus::kAccepted);
  EXPECT_EQ(store().ReconcileExternal(id, kOp, JobState::kCompleted, "ch_8"),
            CommitStatus::kStale);
  EXPECT_EQ(store().ReconcileExternal(id, kOp, JobState::kFailed, "ch_9"), CommitStatus::kStale);
  EXPECT_EQ(store().Get(id)->result, "ch_9");
  EXPECT_EQ(BeginFromNewJob(store()).result, "ch_9");
}

TEST_P(JobStoreContractTest, ReconcileExternalMustAgreeWithALateConfirmation) {
  const JobId id = UnknownWithIntent(store());
  // The worker's confirmation arrived after it had already reported UNKNOWN.
  ASSERT_EQ(store().ConfirmExternal(kOp, kReq, "ch_1"), CommitStatus::kAccepted);

  EXPECT_EQ(store().ReconcileExternal(id, kOp, JobState::kCompleted, "ch_2"),
            CommitStatus::kConflict);
  EXPECT_EQ(store().ReconcileExternal(id, kOp, JobState::kFailed, "no charge"),
            CommitStatus::kConflict);
  EXPECT_EQ(store().Get(id)->state, JobState::kUnknown);

  EXPECT_EQ(store().ReconcileExternal(id, kOp, JobState::kCompleted, "ch_1"),
            CommitStatus::kAccepted);
  EXPECT_EQ(store().Get(id)->state, JobState::kCompleted);
}

TEST_P(JobStoreContractTest, ReconcileExternalRequiresThisJobsExternalOperation) {
  const JobId id = UnknownWithIntent(store());
  // An unrelated UNKNOWN job may not resolve someone else's operation.
  const JobId other = store().Submit(Spec("other"));
  const auto o = store().Acquire("w3", kLease);
  store().ApplyLedger(other, o->generation, "ledger-1", LedgerEntry{"bob", 1});
  store().MarkUnknown(other, o->generation, "timeout");

  EXPECT_EQ(store().ReconcileExternal(other, kOp, JobState::kCompleted, "ch_9"),
            CommitStatus::kConflict);
  EXPECT_EQ(store().ReconcileExternal(other, "ledger-1", JobState::kCompleted, "ch_9"),
            CommitStatus::kConflict);
  EXPECT_EQ(store().ReconcileExternal(other, "missing", JobState::kCompleted, "ch_9"),
            CommitStatus::kNotFound);
  EXPECT_EQ(store().ReconcileExternal(987654, kOp, JobState::kCompleted, "ch_9"),
            CommitStatus::kNotFound);
  EXPECT_THROW(store().ReconcileExternal(id, "", JobState::kCompleted, "x"),
               std::invalid_argument);
  EXPECT_THROW(store().ReconcileExternal(id, kOp, JobState::kUnknown, "x"),
               std::invalid_argument);
  // Nothing changed: the intent is still unresolved.
  EXPECT_EQ(store().Get(id)->state, JobState::kUnknown);
  EXPECT_EQ(store().Get(other)->state, JobState::kUnknown);
  EXPECT_EQ(BeginFromNewJob(store()).state, ExternalState::kInDoubt);
}

TEST_P(JobStoreContractTest, ReconcileExternalOnlyResolvesUnknownJobs) {
  const JobId id = store().Submit(JobSpec{"charge", 3});
  const auto a = store().Acquire("w1", kLease);
  store().BeginExternal(id, a->generation, kOp, kReq);
  EXPECT_EQ(store().ReconcileExternal(id, kOp, JobState::kCompleted, "ch_9"),
            CommitStatus::kStale);  // RUNNING
  EXPECT_EQ(store().Get(id)->state, JobState::kRunning);
}

TEST_P(JobStoreContractTest, JobOnlyReconcileIsRefusedWhileAnExternalOperationIsUnresolved) {
  const JobId id = UnknownWithIntent(store());
  EXPECT_EQ(store().Reconcile(id, JobState::kCompleted, "ch_9"), CommitStatus::kStale);
  EXPECT_EQ(store().Get(id)->state, JobState::kUnknown);
  // Once the provider's answer is recorded, nothing is left unresolved.
  ASSERT_EQ(store().ConfirmExternal(kOp, kReq, "ch_9"), CommitStatus::kAccepted);
  EXPECT_EQ(store().Reconcile(id, JobState::kCompleted, "ch_9"), CommitStatus::kAccepted);
}

}  // namespace
}  // namespace epochflow::testing
