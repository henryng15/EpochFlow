// Demonstration tasks run directly against an in-memory store, so each test
// controls exactly which attempt runs and what the store already contains.

#include "worker/tasks.h"
#include "store/in_memory_job_store.h"
#include "support/forwarding_job_store.h"
#include "support/manual_clock.h"
#include "worker/work_source.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace epochflow::testing {
namespace {

using std::chrono::seconds;

constexpr Duration kLease = seconds{10};

// Records calls; optionally behaves like a provider that times out after
// performing the charge.
class FakeProvider final : public ExternalProvider {
 public:
  bool time_out = false;
  std::vector<std::string> charges;

  std::string Charge(const std::string& reference, const std::string& request) override {
    charges.push_back(reference + " " + request);
    if (time_out) throw ProviderTimeout("no response within deadline");
    return "ch_" + std::to_string(charges.size());
  }
};

// Scripts ConfirmExternal: each call takes the next step; once the script is
// used up, calls go straight to the store.
class ScriptedConfirmStore final : public ForwardingJobStore {
 public:
  enum class Step {
    kThrow,            // transport failure; the store never saw the call
    kApplyThenThrow,   // the store applied it, but the acknowledgement was lost
    kConflict,         // the store answered kConflict
  };
  using ForwardingJobStore::ForwardingJobStore;

  std::vector<Step> script;
  int calls = 0;

  CommitStatus ConfirmExternal(const std::string& op, const std::string& req,
                               const std::string& conf) override {
    const auto i = static_cast<std::size_t>(calls++);
    if (i >= script.size()) return inner_.ConfirmExternal(op, req, conf);
    switch (script[i]) {
      case Step::kThrow:
        throw std::runtime_error("UNAVAILABLE: connection reset");
      case Step::kApplyThenThrow:
        inner_.ConfirmExternal(op, req, conf);
        throw std::runtime_error("DEADLINE_EXCEEDED: acknowledgement lost");
      case Step::kConflict:
        return CommitStatus::kConflict;
    }
    return CommitStatus::kConflict;
  }
};

class TasksTest : public ::testing::Test {
 protected:
  // Acquires the next job and runs the task handler for it.
  HandlerResult RunNext(TaskOptions options = {}) {
    if (options.provider == nullptr) options.provider = &provider_;
    const auto a = store_.Acquire("w", kLease);
    EXPECT_TRUE(a.has_value());
    last_ = *a;
    return MakeTaskHandler(source_, options)(*a, std::stop_token{});
  }

  // Lets the current lease expire and returns the job to PENDING.
  void ExpireAndReclaim() {
    clock_.Advance(kLease + seconds{1});
    ASSERT_EQ(store_.ReclaimExpired(), 1u);
  }

  ManualClock clock_;
  InMemoryJobStore store_{clock_};
  ScriptedConfirmStore confirms_{store_};
  StoreWorkSource source_{confirms_, kLease};
  FakeProvider provider_;
  Assignment last_;
};

// Returns a copy: callers pass temporaries, so a reference would dangle.
template <typename T>
T As(const HandlerResult& r) {
  EXPECT_TRUE(std::holds_alternative<T>(r)) << "unexpected handler result kind";
  return std::holds_alternative<T>(r) ? std::get<T>(r) : T{};
}

TEST(ParsePayload, SplitsKeyValuePairs) {
  const auto p = ParsePayload("task=payment;op=order-1;;account=alice;amount_cents=500");
  EXPECT_EQ(p.at("task"), "payment");
  EXPECT_EQ(p.at("op"), "order-1");
  EXPECT_EQ(p.at("account"), "alice");
  EXPECT_EQ(p.at("amount_cents"), "500");
  EXPECT_EQ(p.size(), 4u);
  EXPECT_TRUE(ParsePayload("hello").empty());
}

TEST_F(TasksTest, PaymentAppliesLedgerEntry) {
  store_.Submit({"task=payment;op=order-1;account=alice;amount_cents=500", 3});
  EXPECT_EQ(As<JobSucceeded>(RunNext()).result, "ledger_entry=1");
}

TEST_F(TasksTest, PaymentDefaultsOperationIdToTheJob) {
  const JobId id = store_.Submit({"task=payment;account=alice;amount_cents=500", 3});
  RunNext();
  // Re-applying with the default id from another attempt deduplicates.
  ExpireAndReclaim();
  EXPECT_EQ(As<JobSucceeded>(RunNext()).result, "ledger_entry=1;deduplicated=true");
  EXPECT_EQ(last_.job_id, id);
}

TEST_F(TasksTest, PaymentRerunAfterCrashDeduplicates) {
  store_.Submit({"task=payment;op=order-1;account=alice;amount_cents=500", 3});
  ASSERT_EQ(As<JobSucceeded>(RunNext()).result, "ledger_entry=1");
  // The first worker crashed before reporting; the job runs again.
  ExpireAndReclaim();
  EXPECT_EQ(As<JobSucceeded>(RunNext()).result, "ledger_entry=1;deduplicated=true");
}

TEST_F(TasksTest, PaymentWithInvalidPayloadFailsPermanently) {
  store_.Submit({"task=payment;op=order-1;amount_cents=500", 3});  // no account
  const auto missing = As<JobFailed>(RunNext());
  EXPECT_FALSE(missing.retryable);

  store_.Submit({"task=payment;op=order-2;account=a;amount_cents=12x", 3});
  EXPECT_FALSE(As<JobFailed>(RunNext()).retryable);
}

TEST_F(TasksTest, PaymentReusingAnOperationIdDifferentlyFailsPermanently) {
  store_.Submit({"task=payment;op=order-1;account=alice;amount_cents=500", 3});
  store_.Submit({"task=payment;op=order-1;account=alice;amount_cents=999", 3});
  RunNext();
  const auto conflict = As<JobFailed>(RunNext());
  EXPECT_FALSE(conflict.retryable);
  EXPECT_NE(conflict.reason.find("conflict"), std::string::npos);
}

TEST_F(TasksTest, ExternalChargeHappensOnceAndIsConfirmed) {
  store_.Submit({"task=external;op=charge-1;account=alice;amount_cents=500", 3});
  EXPECT_EQ(As<JobSucceeded>(RunNext()).result, "charge=ch_1");
  ASSERT_EQ(provider_.charges.size(), 1u);
  EXPECT_EQ(provider_.charges[0], "charge-1 account=alice;amount_cents=500");

  // A later attempt (crash after confirm, before report) reuses the answer.
  ExpireAndReclaim();
  EXPECT_EQ(As<JobSucceeded>(RunNext()).result, "charge=ch_1;reused=true");
  EXPECT_EQ(provider_.charges.size(), 1u);
}

constexpr const char* kExternalJob = "task=external;op=charge-1;account=alice;amount_cents=500";
constexpr const char* kExternalRequest = "account=alice;amount_cents=500";

TEST_F(TasksTest, ExternalSuccessIsReportedOnlyAfterTheConfirmationIsRecorded) {
  const JobId id = store_.Submit({kExternalJob, 3});
  EXPECT_EQ(As<JobSucceeded>(RunNext()).result, "charge=ch_1");
  EXPECT_EQ(confirms_.calls, 1);
  // The durable record now holds the provider's answer.
  const auto again = store_.BeginExternal(id, last_.generation, "charge-1", kExternalRequest);
  EXPECT_EQ(again.state, ExternalState::kConfirmed);
  EXPECT_EQ(again.result, "ch_1");
}

TEST_F(TasksTest, RejectedConfirmationMakesTheOutcomeUnknownWithoutCallingTheProviderAgain) {
  const JobId id = store_.Submit({kExternalJob, 3});
  confirms_.script = {ScriptedConfirmStore::Step::kConflict};
  const auto unknown = As<JobUnknown>(RunNext());
  EXPECT_NE(unknown.reason.find("charge-1"), std::string::npos);
  EXPECT_NE(unknown.reason.find("rejected"), std::string::npos);
  EXPECT_EQ(confirms_.calls, 1);  // a rejection is an answer: no retry
  EXPECT_EQ(provider_.charges.size(), 1u);
  // The intent is still unconfirmed, so a later attempt would see it in doubt.
  EXPECT_EQ(store_.BeginExternal(id, last_.generation, "charge-1", kExternalRequest).state,
            ExternalState::kStart);
}

TEST_F(TasksTest, ConfirmationThatNeverReachesTheStoreMakesTheOutcomeUnknown) {
  const JobId id = store_.Submit({kExternalJob, 3});
  using Step = ScriptedConfirmStore::Step;
  confirms_.script = {Step::kThrow, Step::kThrow, Step::kThrow};
  const auto unknown = As<JobUnknown>(RunNext());
  EXPECT_NE(unknown.reason.find("not recorded"), std::string::npos);
  EXPECT_EQ(confirms_.calls, 3);
  EXPECT_EQ(provider_.charges.size(), 1u);
  EXPECT_EQ(store_.BeginExternal(id, last_.generation, "charge-1", kExternalRequest).state,
            ExternalState::kStart);  // still only an intent
}

TEST_F(TasksTest, LostConfirmationAcknowledgementIsRecognizedOnRetry) {
  store_.Submit({kExternalJob, 3});
  using Step = ScriptedConfirmStore::Step;
  confirms_.script = {Step::kApplyThenThrow};
  EXPECT_EQ(As<JobSucceeded>(RunNext()).result, "charge=ch_1");
  EXPECT_EQ(confirms_.calls, 2);  // the retry finds the identical confirmation
  EXPECT_EQ(provider_.charges.size(), 1u);
}

TEST_F(TasksTest, ConfirmationFromAStaleAttemptIsKeptButItsResultIsFenced) {
  const JobId id = store_.Submit({kExternalJob, 3});
  TaskOptions options;
  Assignment newer;
  // While the provider call is in flight the lease expires and another worker
  // takes over.
  options.hooks.after_effect = [&](const Assignment&) {
    ExpireAndReclaim();
    newer = *store_.Acquire("newer", kLease);
  };
  EXPECT_TRUE(std::holds_alternative<JobSucceeded>(RunNext(options)));
  // The stale worker's report is rejected ...
  EXPECT_EQ(store_.Complete(id, last_.generation, "charge=ch_1"), CommitStatus::kStale);
  EXPECT_EQ(store_.Get(id)->state, JobState::kRunning);
  // ... but the provider's answer it recorded lets the current attempt finish.
  const auto begin = store_.BeginExternal(id, newer.generation, "charge-1", kExternalRequest);
  EXPECT_EQ(begin.state, ExternalState::kConfirmed);
  EXPECT_EQ(provider_.charges.size(), 1u);
}

TEST_F(TasksTest, ExternalTimeoutMakesTheOutcomeUnknown) {
  store_.Submit({"task=external;op=charge-1;account=alice;amount_cents=500", 3});
  provider_.time_out = true;
  const auto unknown = As<JobUnknown>(RunNext());
  EXPECT_NE(unknown.reason.find("timed out"), std::string::npos);
  EXPECT_EQ(provider_.charges.size(), 1u);
}

TEST_F(TasksTest, ExternalIntentLeftByCrashedAttemptIsNeverRetried) {
  const JobId id = store_.Submit({"task=external;op=charge-1;account=alice;amount_cents=500", 3});
  // Attempt 1 recorded its intent and may have charged, then died.
  const auto first = store_.Acquire("crashed", kLease);
  store_.BeginExternal(id, first->generation, "charge-1", "account=alice;amount_cents=500");
  ExpireAndReclaim();

  EXPECT_TRUE(std::holds_alternative<JobUnknown>(RunNext()));
  EXPECT_TRUE(provider_.charges.empty());
}

TEST_F(TasksTest, ExternalWithoutProviderFailsPermanently) {
  store_.Submit({"task=external;op=charge-1;account=alice;amount_cents=500", 3});
  const auto a = store_.Acquire("w", kLease);
  const auto result = MakeTaskHandler(source_, TaskOptions{})(*a, std::stop_token{});
  EXPECT_FALSE(As<JobFailed>(result).retryable);
}

TEST_F(TasksTest, HooksSurroundTheSideEffect) {
  store_.Submit({"task=payment;op=order-1;account=alice;amount_cents=500", 3});
  std::vector<std::string> events;
  TaskOptions options;
  options.hooks.before_effect = [&](const Assignment&) { events.push_back("before"); };
  options.hooks.after_effect = [&](const Assignment&) { events.push_back("after"); };
  RunNext(options);
  EXPECT_EQ(events, (std::vector<std::string>{"before", "after"}));
}

TEST_F(TasksTest, PayloadWithoutTaskIsEchoed) {
  store_.Submit({"hello", 3});
  EXPECT_EQ(As<JobSucceeded>(RunNext()).result, "processed:hello");
}

TEST_F(TasksTest, PaymentWithIdempotencyOffDuplicatesOnRerun) {
  // The naive baseline: a fresh operation id per attempt, as a client without
  // idempotency keys would produce.
  store_.Submit({"task=payment;op=order-1;account=alice;amount_cents=500;idempotency=off", 3});
  EXPECT_EQ(As<JobSucceeded>(RunNext()).result, "ledger_entry=1");
  ExpireAndReclaim();
  EXPECT_EQ(As<JobSucceeded>(RunNext()).result, "ledger_entry=2");  // a second, real entry
}

TEST_F(TasksTest, WorkAfterEffectHappensAfterTheEffect) {
  store_.Submit({"task=payment;op=order-1;account=alice;amount_cents=500", 3});
  TaskOptions options;
  options.work_after_effect = std::chrono::milliseconds{40};
  std::chrono::steady_clock::time_point effect_done;
  options.hooks.after_effect = [&](const Assignment&) {
    effect_done = std::chrono::steady_clock::now();
  };
  const auto result = RunNext(options);
  EXPECT_GE(std::chrono::steady_clock::now() - effect_done, std::chrono::milliseconds{40});
  EXPECT_EQ(As<JobSucceeded>(result).result, "ledger_entry=1");
}

TEST_F(TasksTest, LeaseLostDuringWorkAfterEffectAbandonsTheReport) {
  store_.Submit({"task=payment;op=order-1;account=alice;amount_cents=500", 3});
  TaskOptions options;
  options.work_after_effect = std::chrono::milliseconds{1000};
  std::stop_source lost;
  options.hooks.after_effect = [&](const Assignment&) { lost.request_stop(); };
  const auto a = store_.Acquire("w", kLease);
  const auto result = MakeTaskHandler(source_, options)(*a, lost.get_token());
  // The effect stays applied; a later attempt deduplicates it.
  EXPECT_TRUE(As<JobFailed>(result).retryable);
}

TEST(FileProvider, AppendsOneLinePerChargeAndCanTimeOutAfterCharging) {
  const auto path = std::filesystem::temp_directory_path() /
                    ("epochflow_provider_test_" + std::to_string(::getpid()) + ".log");
  std::filesystem::remove(path);
  {
    FileProvider provider(path.string());
    EXPECT_FALSE(provider.Charge("charge-1", "account=a;amount_cents=1").empty());
    FileProvider timing_out(path.string(), /*time_out_after_charge=*/true);
    EXPECT_THROW(timing_out.Charge("charge-2", "account=b;amount_cents=2"), ProviderTimeout);
  }
  std::ifstream in(path);
  std::vector<std::string> lines;
  for (std::string line; std::getline(in, line);) lines.push_back(line);
  ASSERT_EQ(lines.size(), 2u);
  EXPECT_TRUE(lines[0].starts_with("charge-1\taccount=a;amount_cents=1\t"));
  EXPECT_TRUE(lines[1].starts_with("charge-2\taccount=b;amount_cents=2\t"));
  std::filesystem::remove(path);
}

}  // namespace
}  // namespace epochflow::testing
