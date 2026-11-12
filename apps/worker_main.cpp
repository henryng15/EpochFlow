// epochflow-worker: acquires jobs from a coordinator and runs the demonstration
// tasks (src/worker/tasks.h): payment (idempotent ledger entry), external
// (charge through a provider without idempotency keys), or echo. --fault
// injects one failure at a chosen point, for the fault-injection tests.

#include "client/coordinator_client.h"
#include "common/flags.h"
#include "common/log.h"
#include "common/signals.h"
#include "worker/external_provider.h"
#include "worker/tasks.h"
#include "worker/worker.h"

#include <signal.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <format>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

constexpr const char* kUsage = R"(usage: epochflow-worker [flags]

  --coordinator=HOST:PORT     coordinator address (default 127.0.0.1:50051)
  --worker-id=ID              identity shown in job records (default HOST-PID)
  --work-ms=N                 simulated processing time per job, spent before
                              its side effect (default 0)
  --work-after-effect-ms=N    simulated processing after a side effect, before
                              reporting (default 0)
  --provider-log=PATH         file used as the simulated external provider
                              (required for task=external jobs)
  --fault=NAME                inject one failure, once per process:
                                crash-before-work    SIGKILL right after acquire
                                crash-after-effect   SIGKILL right after the side
                                                     effect, before recording or
                                                     reporting anything else
                                pause-before-effect  SIGSTOP before the side effect
                                pause-before-report  SIGSTOP before reporting
                                                     (resume with SIGCONT)
                                drop-report-ack      lose the first report's
                                                     acknowledgement
                                external-timeout     provider charges, then times out
  --rpc-timeout-ms=N          deadline per RPC (default 5000)
  --idle-backoff-ms=N         wait when no job is available (default 200)
  --help

Stops on SIGINT or SIGTERM after the job in progress finishes and is reported.
Logs events to stderr.
)";

constexpr const char* kFaults[] = {"crash-before-work",   "crash-after-effect", "pause-before-effect",
                                   "pause-before-report", "drop-report-ack",    "external-timeout"};

// Raises `signal` the first time Fire is called, after logging the job and
// generation, so tests can see the process reach the fault point. Later calls
// do nothing: each process injects its fault once.
//
// raise() targets the calling thread, so it stops (or dies) before raise()
// returns. kill(getpid(), ...) would not do: the kernel may deliver a
// process-directed signal to another thread, and this thread could run on
// into the side effect before the whole process stops.
class FaultPoint {
 public:
  FaultPoint(std::string worker, std::string name) : worker_(std::move(worker)), name_(std::move(name)) {}

  void Fire(const epochflow::Assignment& a, int signal) {
    if (fired_.exchange(true)) return;
    epochflow::LogToStderr(std::format("worker={} fault={} firing job={} generation={}", worker_,
                                       name_, a.job_id, a.generation));
    ::raise(signal);
  }

 private:
  std::string worker_;
  std::string name_;
  std::atomic<bool> fired_{false};
};

// Loses the acknowledgement of the first successful Complete: the coordinator
// applies it, the worker sees an error and retries.
class DropFirstReportAck final : public epochflow::WorkSource {
 public:
  explicit DropFirstReportAck(epochflow::WorkSource& inner) : inner_(inner) {}
  std::optional<epochflow::Assignment> Acquire(const epochflow::WorkerId& w) override {
    return inner_.Acquire(w);
  }
  epochflow::CommitStatus Renew(epochflow::JobId id, epochflow::Generation g) override {
    return inner_.Renew(id, g);
  }
  epochflow::CommitStatus Complete(epochflow::JobId id, epochflow::Generation g,
                                   const std::string& r) override {
    const auto status = inner_.Complete(id, g, r);
    if (status == epochflow::CommitStatus::kAccepted && !dropped_.exchange(true)) {
      epochflow::LogToStderr(std::format("fault=drop-report-ack firing job={} generation={}", id, g));
      throw std::runtime_error("acknowledgement lost (injected)");
    }
    return status;
  }
  epochflow::CommitStatus Fail(epochflow::JobId id, epochflow::Generation g, const std::string& r,
                               bool retry) override {
    return inner_.Fail(id, g, r, retry);
  }
  epochflow::CommitStatus MarkUnknown(epochflow::JobId id, epochflow::Generation g,
                                      const std::string& r) override {
    return inner_.MarkUnknown(id, g, r);
  }
  epochflow::OperationOutcome ApplyLedger(epochflow::JobId id, epochflow::Generation g,
                                          const std::string& op,
                                          const epochflow::LedgerEntry& e) override {
    return inner_.ApplyLedger(id, g, op, e);
  }
  epochflow::ExternalBegin BeginExternal(epochflow::JobId id, epochflow::Generation g,
                                         const std::string& op, const std::string& req) override {
    return inner_.BeginExternal(id, g, op, req);
  }
  epochflow::CommitStatus ConfirmExternal(const std::string& op, const std::string& req,
                                          const std::string& conf) override {
    return inner_.ConfirmExternal(op, req, conf);
  }

 private:
  epochflow::WorkSource& inner_;
  std::atomic<bool> dropped_{false};
};

std::string DefaultWorkerId() {
  char host[256] = {};
  if (gethostname(host, sizeof(host) - 1) != 0) return std::format("worker-{}", getpid());
  return std::format("{}-{}", host, getpid());
}

}  // namespace

int main(int argc, char** argv) {
  using namespace epochflow;
  BlockTerminationSignals();
  try {
    const Flags flags(argc, argv,
                      {"coordinator", "worker-id", "work-ms", "rpc-timeout-ms", "idle-backoff-ms",
                       "provider-log", "fault", "work-after-effect-ms", "help"});
    if (flags.Has("help") || !flags.positional().empty()) {
      std::cout << kUsage;
      return flags.Has("help") ? 0 : 2;
    }
    const auto work = std::chrono::milliseconds{flags.GetInt("work-ms", 0)};
    if (work.count() < 0) throw std::invalid_argument("--work-ms must be >= 0");

    const std::string fault = flags.GetString("fault", "");
    if (!fault.empty() && std::ranges::find(kFaults, fault) == std::end(kFaults)) {
      throw std::invalid_argument("unknown --fault " + fault);
    }
    const std::string worker_id = flags.GetString("worker-id", DefaultWorkerId());

    ClientOptions client_options;
    client_options.rpc_timeout = Duration{flags.GetInt("rpc-timeout-ms", 5000)};
    CoordinatorClient client(flags.GetString("coordinator", "127.0.0.1:50051"), client_options);
    DropFirstReportAck lossy(client);
    WorkSource& source = fault == "drop-report-ack" ? static_cast<WorkSource&>(lossy) : client;

    std::unique_ptr<FileProvider> provider;
    if (flags.Has("provider-log")) {
      provider = std::make_unique<FileProvider>(flags.GetString("provider-log", ""),
                                                fault == "external-timeout");
    }

    FaultPoint fault_point(worker_id, fault);
    const auto at = [&](const char* name, int signal) -> std::function<void(const Assignment&)> {
      if (fault != name) return {};
      return [&fault_point, signal](const Assignment& a) { fault_point.Fire(a, signal); };
    };

    TaskOptions task_options;
    task_options.work = work;
    task_options.work_after_effect =
        std::chrono::milliseconds{flags.GetInt("work-after-effect-ms", 0)};
    if (task_options.work_after_effect.count() < 0) {
      throw std::invalid_argument("--work-after-effect-ms must be >= 0");
    }
    task_options.provider = provider.get();
    task_options.hooks.before_effect = at("pause-before-effect", SIGSTOP);
    task_options.hooks.after_effect = at("crash-after-effect", SIGKILL);

    WorkerOptions options;
    options.idle_backoff = Duration{flags.GetInt("idle-backoff-ms", 200)};
    options.log = LogToStderr;
    options.after_acquire = at("crash-before-work", SIGKILL);
    options.before_report = at("pause-before-report", SIGSTOP);
    Worker worker(source, MakeTaskHandler(source, task_options), worker_id, options);

    std::jthread thread([&](std::stop_token stop) { worker.Run(stop); });
    LogToStderr(std::format("worker={} started{}", worker.id(),
                            fault.empty() ? "" : " fault=" + fault));
    const int signal = WaitForTerminationSignal();
    LogToStderr(std::format("worker={} signal {} received; finishing current job", worker.id(),
                            signal));
    thread.request_stop();
    thread.join();
    return 0;
  } catch (const std::invalid_argument& e) {
    std::cerr << "error: " << e.what() << "\n\n" << kUsage;
    return 2;
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}
