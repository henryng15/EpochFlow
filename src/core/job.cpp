#include "core/job.h"

namespace epochflow {

std::string_view ToString(CommitStatus status) {
  switch (status) {
    case CommitStatus::kAccepted:
      return "ACCEPTED";
    case CommitStatus::kNotFound:
      return "NOT_FOUND";
    case CommitStatus::kStale:
      return "STALE";
    case CommitStatus::kConflict:
      return "CONFLICT";
  }
  return "INVALID";
}

}  // namespace epochflow
