#pragma once

#include <atomic>
#include <stdexcept>
#include <string>

namespace epochflow {

// The provider did not answer in time: the call may or may not have taken
// effect.
struct ProviderTimeout : std::runtime_error {
  using std::runtime_error::runtime_error;
};

// A third-party system that performs a side effect (for example a card
// charge) and offers no idempotency key: calling it twice charges twice.
// EpochFlow cannot deduplicate such calls; it can only avoid repeating one
// whose outcome is unknown.
class ExternalProvider {
 public:
  virtual ~ExternalProvider() = default;
  // Performs the effect and returns the provider's confirmation. Throws
  // ProviderTimeout when the outcome is unknown.
  virtual std::string Charge(const std::string& reference, const std::string& request) = 0;
};

// Simulated provider backed by an append-only file shared by every worker:
// each charge appends "reference<TAB>request<TAB>confirmation\n" with a single
// write(2) on an O_APPEND descriptor, so lines from concurrent processes never
// interleave. The file is the provider's own record, which an operator (or a
// test) reads to reconcile UNKNOWN jobs. With time_out_after_charge, every call
// records the charge and then throws ProviderTimeout, modeling a response lost
// after the provider acted.
class FileProvider final : public ExternalProvider {
 public:
  explicit FileProvider(std::string path, bool time_out_after_charge = false);

  // Throws std::runtime_error if the file cannot be written.
  std::string Charge(const std::string& reference, const std::string& request) override;

 private:
  std::string path_;
  bool time_out_after_charge_;
  std::atomic<long> counter_{0};
};

}  // namespace epochflow
