#pragma once

#include <chrono>
#include <functional>
#include <stop_token>
#include <thread>

namespace epochflow {

// Renews one job lease on a background thread so long task steps cannot delay
// renewal. Owned by the code executing the job, for exactly one assignment.
//
// `renew` is called every `interval` (first call after one interval). It
// returns true if the lease was extended and false if the assignment is no
// longer current. An exception from `renew` is also treated as a lost lease:
// stopping work early costs only a retry, while continuing without a confirmed
// lease risks running past a reassignment.
//
// Once the lease is lost, lost_token() is stop-requested and no further renewal
// happens. Pass that token to the task so it can abandon work, and never report
// a result for a lost lease.
//
// Thread safety: lost() and lost_token() may be called from any thread. Stop()
// and destruction belong to the owner and must not race with each other.
// `renew` runs on the keeper's thread and must be safe to call from it.
class LeaseKeeper {
 public:
  using RenewFn = std::function<bool()>;

  LeaseKeeper(RenewFn renew, std::chrono::milliseconds interval);
  // Stops renewing and joins the thread.
  ~LeaseKeeper();

  LeaseKeeper(const LeaseKeeper&) = delete;
  LeaseKeeper& operator=(const LeaseKeeper&) = delete;

  // Stops renewing (for example after the task finished). Does not mark the
  // lease lost. Blocks until an in-progress renewal returns. Idempotent.
  void Stop();

  bool lost() const { return lost_.stop_requested(); }
  std::stop_token lost_token() const { return lost_.get_token(); }

 private:
  void Run(std::stop_token stop);

  RenewFn renew_;
  std::chrono::milliseconds interval_;
  std::stop_source lost_;
  // Declared last: destroyed (stopped and joined) before the members it uses.
  std::jthread thread_;
};

}  // namespace epochflow
