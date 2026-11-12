#pragma once

// Behavioral contract every JobStore implementation must satisfy. A test binary
// runs the suite for an implementation with:
//
//   INSTANTIATE_TEST_SUITE_P(InMemory, JobStoreContractTest,
//                            ::testing::Values(&MakeInMemoryHarness));

#include "core/clock.h"
#include "core/job_store.h"

#include <gtest/gtest.h>

#include <functional>
#include <memory>

namespace epochflow::testing {

// Gives the contract a fresh, empty store and control over the time the store
// uses to judge lease expiry.
class StoreHarness {
 public:
  virtual ~StoreHarness() = default;
  virtual JobStore& store() = 0;
  // Moves the store's notion of "now" forward.
  virtual void AdvanceTime(Duration by) = 0;
};

// Returns a harness with a fresh, empty store, or nullptr to skip.
using StoreHarnessFactory = std::function<std::unique_ptr<StoreHarness>()>;

class JobStoreContractTest : public ::testing::TestWithParam<StoreHarnessFactory> {
 protected:
  // A factory returns nullptr when its backend is unavailable (for example no
  // EPOCHFLOW_DSN); the test is then skipped.
  void SetUp() override {
    harness_ = GetParam()();
    if (!harness_) GTEST_SKIP() << "store backend unavailable";
  }

  JobStore& store() { return harness_->store(); }
  void AdvanceTime(Duration by) { harness_->AdvanceTime(by); }

 private:
  std::unique_ptr<StoreHarness> harness_;
};

}  // namespace epochflow::testing
