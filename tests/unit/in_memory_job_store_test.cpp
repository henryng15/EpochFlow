#include "contract/job_store_contract.h"
#include "store/in_memory_job_store.h"
#include "support/manual_clock.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>

namespace epochflow::testing {
namespace {

using std::chrono::seconds;

class InMemoryHarness final : public StoreHarness {
 public:
  JobStore& store() override { return store_; }
  void AdvanceTime(Duration by) override { clock_.Advance(by); }

 private:
  ManualClock clock_;
  InMemoryJobStore store_{clock_};
};

std::unique_ptr<StoreHarness> MakeInMemoryHarness() {
  return std::make_unique<InMemoryHarness>();
}

INSTANTIATE_TEST_SUITE_P(InMemory, JobStoreContractTest,
                         ::testing::Values(StoreHarnessFactory{&MakeInMemoryHarness}));

// Exact-time checks below rely on the in-process clock and do not belong in the
// shared contract.

TEST(InMemoryJobStore, LeaseDeadlineIsNowPlusLease) {
  ManualClock clock;
  InMemoryJobStore store{clock};
  store.Submit(JobSpec{"a", 3});

  const auto a = store.Acquire("w1", seconds{10});
  EXPECT_EQ(a->lease_until, clock.Now() + seconds{10});

  clock.Advance(seconds{4});
  const auto r = store.Renew(a->job_id, a->generation, seconds{10});
  EXPECT_EQ(r.lease_until, clock.Now() + seconds{10});
}

TEST(InMemoryJobStore, LeaseIsStillValidAtExactDeadline) {
  ManualClock clock;
  InMemoryJobStore store{clock};
  const JobId id = store.Submit(JobSpec{"a", 3});
  store.Acquire("w1", seconds{10});

  clock.Advance(seconds{10});
  EXPECT_EQ(store.ReclaimExpired(), 0u);
  clock.Advance(std::chrono::milliseconds{1});
  EXPECT_EQ(store.ReclaimExpired(), 1u);
  EXPECT_EQ(store.Get(id)->state, JobState::kPending);
}

}  // namespace
}  // namespace epochflow::testing
