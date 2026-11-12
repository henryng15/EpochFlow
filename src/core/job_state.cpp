#include "core/job_state.h"

namespace epochflow {

std::string_view ToString(JobState state) {
  switch (state) {
    case JobState::kPending:
      return "PENDING";
    case JobState::kRunning:
      return "RUNNING";
    case JobState::kCompleted:
      return "COMPLETED";
    case JobState::kFailed:
      return "FAILED";
    case JobState::kUnknown:
      return "UNKNOWN";
  }
  return "INVALID";
}

bool IsTerminal(JobState state) {
  return state == JobState::kCompleted || state == JobState::kFailed;
}

bool IsAllowedTransition(JobState from, JobState to) {
  switch (from) {
    case JobState::kPending:
      return to == JobState::kRunning;
    case JobState::kRunning:
      return to == JobState::kPending || to == JobState::kCompleted ||
             to == JobState::kFailed || to == JobState::kUnknown;
    case JobState::kUnknown:
      return to == JobState::kCompleted || to == JobState::kFailed;
    case JobState::kCompleted:
    case JobState::kFailed:
      return false;
  }
  return false;
}

}  // namespace epochflow
