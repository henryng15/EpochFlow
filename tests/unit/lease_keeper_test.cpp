// LeaseKeeper runs a real thread, so these tests use short real intervals with
// generous timeouts. They assert ordering and eventual effects, never exact
// timing.

#include "worker/lease_keeper.h"
#include "support/wait.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <thread>

namespace epochflow::testing {
namespace {

using std::chrono::hours;
using std::chrono::milliseconds;

TEST(LeaseKeeper, RenewsRepeatedlyWhileHeld) {
  std::atomic<int> calls{0};
  LeaseKeeper keeper([&] { ++calls; return true; }, milliseconds{5});

  EXPECT_TRUE(WaitUntil([&] { return calls.load() >= 3; }));
  EXPECT_FALSE(keeper.lost());
  EXPECT_FALSE(keeper.lost_token().stop_requested());
}

TEST(LeaseKeeper, SignalsLostLeaseWhenRenewalIsRejected) {
  std::atomic<int> calls{0};
  LeaseKeeper keeper([&] { return ++calls < 2; }, milliseconds{5});

  ASSERT_TRUE(WaitUntil([&] { return keeper.lost_token().stop_requested(); }));
  EXPECT_TRUE(keeper.lost());
  // No renewals after the lease is known to be lost.
  std::this_thread::sleep_for(milliseconds{30});
  EXPECT_EQ(calls.load(), 2);
}

TEST(LeaseKeeper, TreatsRenewalErrorAsLostLease) {
  LeaseKeeper keeper([]() -> bool { throw std::runtime_error("network down"); },
                     milliseconds{5});

  EXPECT_TRUE(WaitUntil([&] { return keeper.lost(); }));
}

TEST(LeaseKeeper, StopInterruptsWaitWithoutRenewing) {
  std::atomic<int> calls{0};
  LeaseKeeper keeper([&] { ++calls; return true; }, hours{1});

  const auto start = std::chrono::steady_clock::now();
  keeper.Stop();
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds{1});
  EXPECT_EQ(calls.load(), 0);
  // Stopping on purpose is not a lost lease.
  EXPECT_FALSE(keeper.lost());
}

TEST(LeaseKeeper, DestructionStopsRenewals) {
  std::atomic<int> calls{0};
  {
    LeaseKeeper keeper([&] { ++calls; return true; }, milliseconds{5});
    ASSERT_TRUE(WaitUntil([&] { return calls.load() >= 1; }));
  }
  const int after_destruction = calls.load();
  std::this_thread::sleep_for(milliseconds{30});
  EXPECT_EQ(calls.load(), after_destruction);
}

}  // namespace
}  // namespace epochflow::testing
