#pragma once

#include "core/job.h"
#include "worker/external_provider.h"
#include "worker/work_source.h"
#include "worker/worker.h"

#include <chrono>
#include <functional>
#include <map>
#include <string>
#include <string_view>

namespace epochflow {

// Fault-injection points around a task's side effect (see the worker's
// --fault flag): before the effect is attempted and right after it took
// effect, before anything else is recorded or reported.
struct TaskHooks {
  std::function<void(const Assignment&)> before_effect;
  std::function<void(const Assignment&)> after_effect;
};

struct TaskOptions {
  // Simulated processing time of every task, spent before its side effect
  // (the whole of the echo task). Abandoned early if the lease is lost.
  std::chrono::milliseconds work{0};
  // Simulated processing after a payment or external task's side effect, before
  // the result is returned (for example sending a receipt). Widens the window
  // in which a crash leaves an applied effect unreported. Abandoned, with a
  // retryable failure, if the lease is lost.
  std::chrono::milliseconds work_after_effect{0};
  // Required for external tasks; must outlive the handler.
  ExternalProvider* provider = nullptr;
  TaskHooks hooks;
};

// Parses a payload of "key=value" pairs separated by ';'. Segments without '='
// are ignored, so plain text yields an empty map.
std::map<std::string, std::string> ParsePayload(std::string_view payload);

// The demonstration tasks, selected by the payload's "task" key:
//
// Every task first spends `work` (see TaskOptions), then:
// (payment and external also spend `work_after_effect` after the effect)
//
//   task=payment;account=A;amount_cents=N[;op=ID]
//     Applies a ledger entry through WorkSource::ApplyLedger, exactly once per
//     operation id (default "job-<job_id>"). Result "ledger_entry=<id>", with
//     ";deduplicated=true" when an earlier attempt or job already applied it.
//     With "idempotency=off" the operation id gets an "-attempt-<n>" suffix,
//     so every attempt creates its own entry: the naive baseline used by the
//     benchmark to show what idempotency prevents.
//
//   task=external;account=A;amount_cents=N[;op=ID]
//     Charges the external provider at most once per operation id: records the
//     intent (BeginExternal), calls the provider, confirms. If an earlier
//     attempt left an unconfirmed intent, or the provider times out, returns
//     JobUnknown instead of risking a second charge. Result "charge=<conf>",
//     with ";reused=true" when an earlier attempt's confirmation is reused.
//
//   anything else
//     Echo: returns "processed:<payload>".
//
// Invalid payloads and operation-id conflicts fail without retry. A stale
// assignment returns a retryable failure, which the worker cannot report
// anyway. Transport errors from the coordinator propagate as exceptions
// (retryable failure); re-running is safe because the effect is deduplicated
// or, for external charges, turns into JobUnknown.
JobHandler MakeTaskHandler(WorkSource& source, TaskOptions options);

}  // namespace epochflow
