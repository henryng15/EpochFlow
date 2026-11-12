// epochflowctl: submit jobs and inspect them through the coordinator API.

#include "client/coordinator_client.h"
#include "common/flags.h"
#include "rpc/convert.h"
#include "rpc/rpc_error.h"

#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

using epochflow::CoordinatorClient;
using epochflow::JobId;

constexpr const char* kUsage = R"(usage: epochflowctl [--coordinator=HOST:PORT] COMMAND ...

commands:
  submit --payload=TEXT [--max-attempts=N] [--count=N]
      Submit N jobs (default 1); prints one job id per line. max-attempts 0
      (default) uses the coordinator's default.
  get JOB_ID
      Print the job as key=value lines.
  wait JOB_ID... [--timeout-ms=N] [--allow-unknown]
      Poll until every job is COMPLETED or FAILED (default timeout 30000).
      With --allow-unknown, UNKNOWN also ends the wait and counts as success.
  reconcile JOB_ID --outcome=completed|failed --detail=TEXT [--operation=ID]
      Resolve an UNKNOWN job after checking the external system; TEXT becomes
      the result (completed) or error (failed). With --operation, the job's
      external operation is resolved in the same step: confirmed with TEXT as
      the provider's answer, or failed for good (later jobs reusing the id
      fail too). Repeating the same decision succeeds; a job that is not
      UNKNOWN, or a decision contradicting the record, is an error.

exit status: 0 success (wait: all COMPLETED), 1 failure (job FAILED, unknown
job, timeout, RPC error), 2 usage error. Default coordinator 127.0.0.1:50051.
)";

JobId ParseJobId(const std::string& text) {
  std::size_t used = 0;
  const long long id = std::stoll(text, &used);
  if (used != text.size()) throw std::invalid_argument("invalid job id '" + text + "'");
  return id;
}

int Submit(CoordinatorClient& client, const epochflow::Flags& flags) {
  if (!flags.Has("payload")) throw std::invalid_argument("submit requires --payload");
  const std::int64_t count = flags.GetInt("count", 1);
  if (count < 1) throw std::invalid_argument("--count must be >= 1");
  const epochflow::JobSpec spec{flags.GetString("payload", ""),
                                static_cast<int>(flags.GetInt("max-attempts", 0))};
  for (std::int64_t i = 0; i < count; ++i) std::cout << client.Submit(spec) << '\n';
  return 0;
}

int Get(CoordinatorClient& client, const std::vector<std::string>& args) {
  if (args.size() != 2) throw std::invalid_argument("get takes exactly one JOB_ID");
  const auto job = client.Get(ParseJobId(args[1]));
  if (!job) {
    std::cerr << "job " << args[1] << " not found\n";
    return 1;
  }
  std::cout << "job_id=" << job->id << '\n'
            << "state=" << epochflow::ToString(job->state) << '\n'
            << "generation=" << job->generation << '\n'
            << "attempts=" << job->attempts << '\n'
            << "max_attempts=" << job->max_attempts << '\n'
            << "worker_id=" << job->worker_id.value_or("") << '\n'
            << "lease_until_unix_ms="
            << (job->lease_until ? std::to_string(epochflow::rpc::ToUnixMs(*job->lease_until)) : "")
            << '\n'
            << "payload=" << job->payload << '\n'
            << "result=" << job->result.value_or("") << '\n'
            << "error=" << job->error.value_or("") << '\n';
  return 0;
}

int Wait(CoordinatorClient& client, const std::vector<std::string>& args,
         const epochflow::Flags& flags) {
  if (args.size() < 2) throw std::invalid_argument("wait takes at least one JOB_ID");
  std::vector<JobId> ids;
  for (std::size_t i = 1; i < args.size(); ++i) ids.push_back(ParseJobId(args[i]));
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds{flags.GetInt("timeout-ms", 30000)};
  while (true) {
    const bool allow_unknown = flags.Has("allow-unknown");
    bool all_terminal = true;
    bool any_failed = false;
    for (JobId id : ids) {
      const auto job = client.Get(id);
      if (!job) {
        std::cerr << "job " << id << " not found\n";
        return 1;
      }
      all_terminal = all_terminal && (epochflow::IsTerminal(job->state) ||
                                      (allow_unknown && job->state == epochflow::JobState::kUnknown));
      any_failed = any_failed || job->state == epochflow::JobState::kFailed;
    }
    if (all_terminal) return any_failed ? 1 : 0;
    if (std::chrono::steady_clock::now() >= deadline) {
      std::cerr << "timed out waiting for jobs\n";
      return 1;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
  }
}

int Reconcile(CoordinatorClient& client, const std::vector<std::string>& args,
              const epochflow::Flags& flags) {
  if (args.size() != 2) throw std::invalid_argument("reconcile takes exactly one JOB_ID");
  const std::string outcome = flags.GetString("outcome", "");
  if (outcome != "completed" && outcome != "failed") {
    throw std::invalid_argument("reconcile requires --outcome=completed or --outcome=failed");
  }
  if (!flags.Has("detail")) throw std::invalid_argument("reconcile requires --detail");
  const JobId id = ParseJobId(args[1]);
  const std::string op = flags.GetString("operation", "");
  if (flags.Has("operation") && op.empty()) throw std::invalid_argument("--operation is empty");
  const auto status = client.Reconcile(
      id, outcome == "completed" ? epochflow::JobState::kCompleted : epochflow::JobState::kFailed,
      flags.GetString("detail", ""), op);
  using epochflow::CommitStatus;
  switch (status) {
    case CommitStatus::kAccepted:
      return 0;
    case CommitStatus::kNotFound:
      std::cerr << "job " << id << (op.empty() ? "" : " or operation " + op) << " not found\n";
      return 1;
    case CommitStatus::kConflict:
      std::cerr << "operation " << op << " is not job " << id
                << "'s external operation, or the decision contradicts its recorded "
                   "confirmation\n";
      return 1;
    default:
      std::cerr << "job " << id << " is not UNKNOWN\n";
      return 1;
  }
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const epochflow::Flags flags(
        argc, argv,
        {"coordinator", "payload", "max-attempts", "count", "timeout-ms", "allow-unknown", "outcome",
         "detail", "operation", "help"});
    const auto& args = flags.positional();
    if (flags.Has("help")) {
      std::cout << kUsage;
      return 0;
    }
    if (args.empty()) throw std::invalid_argument("missing COMMAND");
    CoordinatorClient client(flags.GetString("coordinator", "127.0.0.1:50051"));
    if (args[0] == "submit" && args.size() == 1) return Submit(client, flags);
    if (args[0] == "get") return Get(client, args);
    if (args[0] == "wait") return Wait(client, args, flags);
    if (args[0] == "reconcile") return Reconcile(client, args, flags);
    throw std::invalid_argument("unknown command '" + args[0] + "'");
  } catch (const std::invalid_argument& e) {
    std::cerr << "error: " << e.what() << "\n\n" << kUsage;
    return 2;
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}
