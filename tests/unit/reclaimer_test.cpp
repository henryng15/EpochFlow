#include "coordinator/reclaimer.h"
#include "store/in_memory_job_store.h"
#include "support/forwarding_job_store.h"
#include "support/manual_clock.h"
#include "support/wait.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <stdexcept>

namespace epochflow::testing {
namespace {

using std::chrono::hours;
using std::chrono::milliseconds;
using std::chrono::seconds;

// In-memory store whose first ReclaimExpired call throws.
class FailsFirstReclaim final : public ForwardingJobStore {
 public:
  using ForwardingJobStore::ForwardingJobStore;
  std::size_t ReclaimExpired() override {
    if (!failed_once_.exchange(true)) throw std::runtime_error("database unavailable");
    return inner_.ReclaimExpired();
  }

 private:
  std::atomic<bool> failed_once_{false};
};

class ReclaimerTest : public ::testing::Test {
 protected:
  JobId SubmitAndExpire(JobStore& store) {
    const JobId id = store.Submit({"a", 3});
    store.Acquire("w1", seconds{10});
    clock_.Advance(seconds{11});
    return id;
  }

  ManualClock clock_;
  InMemoryJobStore store_{clock_};
};

TEST_F(ReclaimerTest, RecoversExpiredLeasesPeriodically) {
  const JobId id = SubmitAndExpire(store_);
  Reclaimer reclaimer(store_, milliseconds{5});

  EXPECT_TRUE(WaitUntil([&] { return store_.Get(id)->state == JobState::kPending; }));
}

TEST_F(ReclaimerTest, KeepsRunningAfterAStoreError) {
  FailsFirstReclaim store(store_);
  const JobId id = SubmitAndExpire(store);
  Reclaimer reclaimer(store, milliseconds{5});

  EXPECT_TRUE(WaitUntil([&] { return store.Get(id)->state == JobState::kPending; }));
}

TEST_F(ReclaimerTest, StopIsPromptEvenWithLongInterval) {
  Reclaimer reclaimer(store_, hours{1});
  const auto start = std::chrono::steady_clock::now();
  reclaimer.Stop();
  EXPECT_LT(std::chrono::steady_clock::now() - start, seconds{1});
}

TEST_F(ReclaimerTest, RejectsNonPositiveInterval) {
  EXPECT_THROW(Reclaimer(store_, milliseconds{0}), std::invalid_argument);
}

}  // namespace
}  // namespace epochflow::testing
