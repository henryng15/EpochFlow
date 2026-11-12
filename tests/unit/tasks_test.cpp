// Demonstration tasks run directly against an in-memory store, so each test
// controls exactly which attempt runs and what the store already contains.

#include "worker/tasks.h"
#include "store/in_memory_job_store.h"
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
  StoreWorkSource source_{store_, kLease};
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
