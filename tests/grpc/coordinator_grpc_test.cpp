// Coordinator RPCs over a real loopback gRPC connection. The store uses a
// ManualClock, so lease expiry is driven by the test, not by sleeping.

#include "client/coordinator_client.h"
#include "coordinator/coordinator.h"
#include "rpc/rpc_error.h"
#include "store/in_memory_job_store.h"
#include "support/manual_clock.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

namespace epochflow::testing {
namespace {

using std::chrono::hours;
using std::chrono::milliseconds;
using std::chrono::seconds;

constexpr Duration kLease = seconds{10};

class CoordinatorGrpcTest : public ::testing::Test {
 protected:
  CoordinatorGrpcTest() {
    CoordinatorOptions options;
    options.listen_address = "127.0.0.1:0";
    options.lease = kLease;
    options.reclaim_interval = hours{1};  // tests reclaim explicitly
    coordinator_ = std::make_unique<Coordinator>(store_, options);
    client_ = std::make_unique<CoordinatorClient>(Target());
  }

  std::string Target() const { return "127.0.0.1:" + std::to_string(coordinator_->port()); }

  ManualClock clock_;
  InMemoryJobStore store_{clock_};
  std::unique_ptr<Coordinator> coordinator_;
  std::unique_ptr<CoordinatorClient> client_;
};

grpc::StatusCode CodeOf(const std::function<void()>& call) {
  try {
    call();
  } catch (const RpcError& e) {
    return e.code();
  }
  return grpc::StatusCode::OK;
}

TEST_F(CoordinatorGrpcTest, SubmitThenGetReturnsPendingJob) {
  const std::string payload("a\0b", 3);  // payload is bytes, not text
  const JobId id = client_->Submit({payload, 4});

  const auto job = client_->Get(id);
  ASSERT_TRUE(job.has_value());
  EXPECT_EQ(job->state, JobState::kPending);
  EXPECT_EQ(job->payload, payload);
  EXPECT_EQ(job->max_attempts, 4);
  EXPECT_EQ(job->generation, 0);
  EXPECT_FALSE(job->worker_id.has_value());
  EXPECT_FALSE(job->lease_until.has_value());
  EXPECT_FALSE(job->result.has_value());
}

TEST_F(CoordinatorGrpcTest, SubmitWithZeroMaxAttemptsUsesDefaultOfThree) {
  const JobId id = client_->Submit({"a", 0});
  EXPECT_EQ(client_->Get(id)->max_attempts, 3);
}

TEST_F(CoordinatorGrpcTest, SubmitRejectsNegativeMaxAttempts) {
  EXPECT_EQ(CodeOf([&] { client_->Submit({"a", -1}); }), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(CoordinatorGrpcTest, GetUnknownJobReturnsNothing) {
  EXPECT_FALSE(client_->Get(424242).has_value());
}

TEST_F(CoordinatorGrpcTest, AcquireReturnsNothingWhenNoJobIsPending) {
  EXPECT_FALSE(client_->Acquire("w1").has_value());
}

TEST_F(CoordinatorGrpcTest, AcquireGrantsTheCoordinatorsLease) {
  const JobId id = client_->Submit({"work", 3});

  const auto a = client_->Acquire("w1");
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(a->job_id, id);
  EXPECT_EQ(a->generation, 1);
  EXPECT_EQ(a->attempt, 1);
  EXPECT_EQ(a->payload, "work");
  EXPECT_EQ(a->lease, kLease);
  EXPECT_EQ(a->lease_until, clock_.Now() + kLease);

  const auto job = client_->Get(id);
  EXPECT_EQ(job->state, JobState::kRunning);
  EXPECT_EQ(job->worker_id, "w1");
  EXPECT_EQ(job->lease_until, a->lease_until);
}

TEST_F(CoordinatorGrpcTest, AcquireRejectsEmptyWorkerId) {
  client_->Submit({"a", 3});
  EXPECT_EQ(CodeOf([&] { client_->Acquire(""); }), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(CoordinatorGrpcTest, RenewAndCompleteWithCurrentGeneration) {
  const JobId id = client_->Submit({"a", 3});
  const auto a = client_->Acquire("w1");

  EXPECT_EQ(client_->Renew(id, a->generation), CommitStatus::kAccepted);
  EXPECT_EQ(client_->Complete(id, a->generation, "done"), CommitStatus::kAccepted);
  const auto job = client_->Get(id);
  EXPECT_EQ(job->state, JobState::kCompleted);
  EXPECT_EQ(job->result, "done");
}

TEST_F(CoordinatorGrpcTest, StatusesSurviveTheWire) {
  const JobId id = client_->Submit({"a", 3});
  const auto a = client_->Acquire("w1");

  EXPECT_EQ(client_->Renew(id, a->generation + 1), CommitStatus::kStale);
  EXPECT_EQ(client_->Complete(424242, 1, "x"), CommitStatus::kNotFound);
  EXPECT_EQ(client_->Complete(id, a->generation + 1, "x"), CommitStatus::kStale);
  ASSERT_EQ(client_->Complete(id, a->generation, "done"), CommitStatus::kAccepted);
  EXPECT_EQ(client_->Complete(id, a->generation, "done"), CommitStatus::kAccepted);
  EXPECT_EQ(client_->Complete(id, a->generation, "other"), CommitStatus::kConflict);
}

TEST_F(CoordinatorGrpcTest, FailRequeuesOrFailsJob) {
  const JobId retry = client_->Submit({"retry", 3});
  const JobId fatal = client_->Submit({"fatal", 3});
  const auto r = client_->Acquire("w1");
  const auto f = client_->Acquire("w1");

  EXPECT_EQ(client_->Fail(retry, r->generation, "transient", true), CommitStatus::kAccepted);
  EXPECT_EQ(client_->Fail(fatal, f->generation, "bad input", false), CommitStatus::kAccepted);
  EXPECT_EQ(client_->Get(retry)->state, JobState::kPending);
  EXPECT_EQ(client_->Get(retry)->error, "transient");
  EXPECT_EQ(client_->Get(fatal)->state, JobState::kFailed);
}

TEST_F(CoordinatorGrpcTest, StaleWorkerIsFencedAfterReassignment) {
  const JobId id = client_->Submit({"a", 3});
  CoordinatorClient slow(Target());
  CoordinatorClient fresh(Target());
  const auto old_assignment = slow.Acquire("slow");

  clock_.Advance(kLease + seconds{1});
  ASSERT_EQ(store_.ReclaimExpired(), 1u);
  const auto new_assignment = fresh.Acquire("fresh");
  ASSERT_EQ(new_assignment->generation, 2);

  EXPECT_EQ(slow.Renew(id, old_assignment->generation), CommitStatus::kStale);
  EXPECT_EQ(slow.Complete(id, old_assignment->generation, "stale"), CommitStatus::kStale);
  EXPECT_EQ(fresh.Complete(id, new_assignment->generation, "fresh"), CommitStatus::kAccepted);
  EXPECT_EQ(client_->Get(id)->result, "fresh");
}

TEST_F(CoordinatorGrpcTest, UnreachableCoordinatorThrowsRetryableError) {
  ClientOptions options;
  options.rpc_timeout = milliseconds{500};
  CoordinatorClient nowhere("127.0.0.1:1", options);

  const auto code = CodeOf([&] { nowhere.Acquire("w1"); });
  EXPECT_TRUE(code == grpc::StatusCode::UNAVAILABLE ||
              code == grpc::StatusCode::DEADLINE_EXCEEDED)
      << static_cast<int>(code);
}

TEST_F(CoordinatorGrpcTest, ShutdownStopsServing) {
  coordinator_->Shutdown();
  ClientOptions options;
  options.rpc_timeout = milliseconds{500};
  CoordinatorClient client(Target(), options);
  EXPECT_NE(CodeOf([&] { client.Acquire("w1"); }), grpc::StatusCode::OK);
}


TEST_F(CoordinatorGrpcTest, LedgerOperationsSurviveTheWire) {
  const JobId id = client_->Submit({"pay", 3});
  const auto a = client_->Acquire("w1");
  const LedgerEntry payment{"alice", 1250};

  const auto first = client_->ApplyLedger(id, a->generation, "order-7", payment);
  EXPECT_EQ(first.status, CommitStatus::kAccepted);
  EXPECT_FALSE(first.deduplicated);
  const auto again = client_->ApplyLedger(id, a->generation, "order-7", payment);
  EXPECT_TRUE(again.deduplicated);
  EXPECT_EQ(again.result, first.result);
  EXPECT_EQ(client_->ApplyLedger(id, a->generation, "order-7", LedgerEntry{"alice", 1}).status,
            CommitStatus::kConflict);
  EXPECT_EQ(client_->ApplyLedger(id, a->generation + 1, "order-8", payment).status,
            CommitStatus::kStale);
  EXPECT_EQ(CodeOf([&] { client_->ApplyLedger(id, a->generation, "", payment); }),
            grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(CoordinatorGrpcTest, ExternalOperationsSurviveTheWire) {
  const JobId id = client_->Submit({"charge", 3});
  const auto first = client_->Acquire("w1");
  const auto begin = client_->BeginExternal(id, first->generation, "charge-1", "req");
  EXPECT_EQ(begin.status, CommitStatus::kAccepted);
  EXPECT_EQ(begin.state, ExternalState::kStart);

  clock_.Advance(kLease + seconds{1});
  ASSERT_EQ(store_.ReclaimExpired(), 1u);
  const auto second = client_->Acquire("w2");
  EXPECT_EQ(client_->BeginExternal(id, second->generation, "charge-1", "req").state,
            ExternalState::kInDoubt);

  EXPECT_EQ(client_->ConfirmExternal("charge-1", "req", "ch_1"), CommitStatus::kAccepted);
  const auto confirmed = client_->BeginExternal(id, second->generation, "charge-1", "req");
  EXPECT_EQ(confirmed.state, ExternalState::kConfirmed);
  EXPECT_EQ(confirmed.result, "ch_1");
  EXPECT_EQ(client_->ConfirmExternal("charge-1", "req", "ch_2"), CommitStatus::kConflict);
  EXPECT_EQ(client_->ConfirmExternal("nope", "req", "ch_1"), CommitStatus::kNotFound);
}

TEST_F(CoordinatorGrpcTest, UnknownAndReconcileSurviveTheWire) {
  const JobId id = client_->Submit({"charge", 3});
  const auto a = client_->Acquire("w1");
  EXPECT_EQ(client_->Reconcile(id, JobState::kCompleted, "x"), CommitStatus::kStale);

  EXPECT_EQ(client_->MarkUnknown(id, a->generation, "timeout"), CommitStatus::kAccepted);
  EXPECT_EQ(client_->Get(id)->state, JobState::kUnknown);
  EXPECT_EQ(CodeOf([&] { client_->Reconcile(id, JobState::kRunning, "x"); }),
            grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(client_->Reconcile(id, JobState::kCompleted, "ch_9"), CommitStatus::kAccepted);
  const auto job = client_->Get(id);
  EXPECT_EQ(job->state, JobState::kCompleted);
  EXPECT_EQ(job->result, "ch_9");
}

}  // namespace
}  // namespace epochflow::testing
