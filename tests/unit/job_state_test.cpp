#include "core/job_state.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>

namespace epochflow {
namespace {

constexpr std::array kAllStates{JobState::kPending, JobState::kRunning,
                                JobState::kCompleted, JobState::kFailed,
                                JobState::kUnknown};

TEST(JobState, AllowedTransitionsMatchSpecification) {
  // Rows are the current state, columns the target, both in kAllStates order:
  //                    PENDING RUNNING COMPLETED FAILED UNKNOWN
  constexpr bool kExpected[5][5] = {
      /* PENDING   */ {false, true, false, false, false},
      /* RUNNING   */ {true, false, true, true, true},
      /* COMPLETED */ {false, false, false, false, false},
      /* FAILED    */ {false, false, false, false, false},
      /* UNKNOWN   */ {false, false, true, true, false},
  };
  for (std::size_t from = 0; from < kAllStates.size(); ++from) {
    for (std::size_t to = 0; to < kAllStates.size(); ++to) {
      EXPECT_EQ(IsAllowedTransition(kAllStates[from], kAllStates[to]),
                kExpected[from][to])
          << ToString(kAllStates[from]) << " -> " << ToString(kAllStates[to]);
    }
  }
}

TEST(JobState, OnlyCompletedAndFailedAreTerminal) {
  EXPECT_FALSE(IsTerminal(JobState::kPending));
  EXPECT_FALSE(IsTerminal(JobState::kRunning));
  EXPECT_TRUE(IsTerminal(JobState::kCompleted));
  EXPECT_TRUE(IsTerminal(JobState::kFailed));
  // UNKNOWN waits for reconciliation; it is neither retried nor final.
  EXPECT_FALSE(IsTerminal(JobState::kUnknown));
}

TEST(JobState, TerminalStatesHaveNoOutgoingTransitions) {
  for (JobState from : kAllStates) {
    if (!IsTerminal(from)) continue;
    for (JobState to : kAllStates) {
      EXPECT_FALSE(IsAllowedTransition(from, to)) << ToString(from);
    }
  }
}

TEST(JobState, ToStringUsesProtocolNames) {
  EXPECT_EQ(ToString(JobState::kPending), "PENDING");
  EXPECT_EQ(ToString(JobState::kRunning), "RUNNING");
  EXPECT_EQ(ToString(JobState::kCompleted), "COMPLETED");
  EXPECT_EQ(ToString(JobState::kFailed), "FAILED");
  EXPECT_EQ(ToString(JobState::kUnknown), "UNKNOWN");
}

}  // namespace
}  // namespace epochflow
