#include "worker/tasks.h"

#include <algorithm>
#include <charconv>
#include <format>
#include <optional>
#include <system_error>
#include <thread>

namespace epochflow {
namespace {

struct Charge {
  std::string operation_id;
  LedgerEntry entry;
  // Canonical content sent to the store and the provider.
  std::string request;
};

std::optional<std::int64_t> ParseInt(const std::string& text) {
  std::int64_t value = 0;
  const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (text.empty() || ec != std::errc{} || end != text.data() + text.size()) return std::nullopt;
  return value;
}

// Reads account, amount, and operation id; nullopt with `error` set if invalid.
std::optional<Charge> ParseCharge(const std::map<std::string, std::string>& p, const Assignment& a,
                                  std::string& error) {
  const auto account = p.find("account");
  const auto amount = p.find("amount_cents");
  if (account == p.end() || account->second.empty()) {
    error = "invalid payload: account is required";
    return std::nullopt;
  }
  const auto cents = amount == p.end() ? std::nullopt : ParseInt(amount->second);
  if (!cents) {
    error = "invalid payload: amount_cents must be an integer";
    return std::nullopt;
  }
  const auto op = p.find("op");
  Charge c;
  c.operation_id = op != p.end() && !op->second.empty() ? op->second
                                                        : std::format("job-{}", a.job_id);
  if (const auto idem = p.find("idempotency"); idem != p.end() && idem->second == "off") {
    c.operation_id += std::format("-attempt-{}", a.attempt);
  }
  c.entry = LedgerEntry{account->second, *cents};
  c.request = std::format("account={};amount_cents={}", account->second, *cents);
  return c;
}

HandlerResult NotCurrent(CommitStatus status) {
  return JobFailed{std::format("assignment no longer current ({})", ToString(status)), true};
}

HandlerResult Conflict(const std::string& operation_id) {
  return JobFailed{std::format("operation {} conflicts with an earlier request", operation_id),
                   false};
}

void Run(const std::function<void(const Assignment&)>& hook, const Assignment& a) {
  if (hook) hook(a);
}

// Spends `work`; false if the lease was lost meanwhile.
bool SimulateWork(std::chrono::milliseconds work, std::stop_token lease_lost);

HandlerResult Payment(WorkSource& source, const TaskOptions& options, const Assignment& a,
                      const Charge& c, std::stop_token lease_lost) {
  Run(options.hooks.before_effect, a);
  const OperationOutcome o = source.ApplyLedger(a.job_id, a.generation, c.operation_id, c.entry);
  if (o.status == CommitStatus::kConflict) return Conflict(c.operation_id);
  if (o.status != CommitStatus::kAccepted) return NotCurrent(o.status);
  Run(options.hooks.after_effect, a);
  if (!SimulateWork(options.work_after_effect, lease_lost)) return JobFailed{"lease lost", true};
  return JobSucceeded{std::format("ledger_entry={}{}", o.result,
                                  o.deduplicated ? ";deduplicated=true" : "")};
}

HandlerResult External(WorkSource& source, const TaskOptions& options, const Assignment& a,
                       const Charge& c, std::stop_token lease_lost) {
  if (options.provider == nullptr) {
    return JobFailed{"no external provider configured on this worker", false};
  }
  const ExternalBegin b = source.BeginExternal(a.job_id, a.generation, c.operation_id, c.request);
  if (b.status == CommitStatus::kConflict) return Conflict(c.operation_id);
  if (b.status != CommitStatus::kAccepted) return NotCurrent(b.status);
  if (b.state == ExternalState::kConfirmed) {
    return JobSucceeded{std::format("charge={};reused=true", b.result)};
  }
  if (b.state == ExternalState::kInDoubt) {
    return JobUnknown{std::format(
        "operation {} was started by an earlier attempt and never confirmed; it may have "
        "charged; reconcile against the provider",
        c.operation_id)};
  }

  Run(options.hooks.before_effect, a);
  std::string confirmation;
  try {
    confirmation = options.provider->Charge(c.operation_id, c.request);
  } catch (const ProviderTimeout& e) {
    return JobUnknown{std::format("provider timed out for operation {}: {}", c.operation_id,
                                  e.what())};
  }
  Run(options.hooks.after_effect, a);

  // The provider charged. Success may be reported only once the durable record
  // holds that confirmation (invariant I8): otherwise the job could complete
  // while its operation stays STARTED, and every later attempt would be left
  // in doubt. Confirmation is idempotent for the same answer, so transport
  // failures are retried; a lost acknowledgement comes back as kAccepted. Any
  // other answer, or running out of tries, leaves the outcome UNKNOWN. The
  // provider is never called again here.
  constexpr int kConfirmTries = 3;
  for (int attempt = 1;; ++attempt) {
    try {
      const CommitStatus s = source.ConfirmExternal(c.operation_id, c.request, confirmation);
      if (s == CommitStatus::kAccepted) break;
      return JobUnknown{std::format(
          "provider charged operation {} ({}) but the confirmation was rejected ({}); "
          "reconcile against the provider",
          c.operation_id, confirmation, ToString(s))};
    } catch (const std::exception& e) {
      if (attempt == kConfirmTries) {
        return JobUnknown{std::format(
            "provider charged operation {} ({}) but the confirmation was not recorded after {} "
            "tries: {}; reconcile against the provider",
            c.operation_id, confirmation, kConfirmTries, e.what())};
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{50});
    }
  }
  if (!SimulateWork(options.work_after_effect, lease_lost)) return JobFailed{"lease lost", true};
  return JobSucceeded{std::format("charge={}", confirmation)};
}

bool SimulateWork(std::chrono::milliseconds work, std::stop_token lease_lost) {
  const auto deadline = std::chrono::steady_clock::now() + work;
  while (std::chrono::steady_clock::now() < deadline) {
    if (lease_lost.stop_requested()) return false;
    std::this_thread::sleep_for(std::min<std::chrono::milliseconds>(work, std::chrono::milliseconds{10}));
  }
  return !lease_lost.stop_requested();
}

}  // namespace

std::map<std::string, std::string> ParsePayload(std::string_view payload) {
  std::map<std::string, std::string> out;
  while (!payload.empty()) {
    const auto end = payload.find(';');
    const std::string_view segment = payload.substr(0, end);
    if (const auto eq = segment.find('='); eq != std::string_view::npos) {
      out.emplace(std::string(segment.substr(0, eq)), std::string(segment.substr(eq + 1)));
    }
    if (end == std::string_view::npos) break;
    payload.remove_prefix(end + 1);
  }
  return out;
}

JobHandler MakeTaskHandler(WorkSource& source, TaskOptions options) {
  return [&source, options](const Assignment& a, std::stop_token lease_lost) -> HandlerResult {
    if (!SimulateWork(options.work, lease_lost)) return JobFailed{"lease lost", true};
    const auto payload = ParsePayload(a.payload);
    const auto task = payload.find("task");
    if (task == payload.end() || (task->second != "payment" && task->second != "external")) {
      return JobSucceeded{"processed:" + a.payload};
    }
    std::string error;
    const auto charge = ParseCharge(payload, a, error);
    if (!charge) return JobFailed{error, false};
    return task->second == "payment" ? Payment(source, options, a, *charge, lease_lost)
                                     : External(source, options, a, *charge, lease_lost);
  };
}

}  // namespace epochflow
