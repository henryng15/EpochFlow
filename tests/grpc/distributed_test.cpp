// Coordinator plus Worker instances talking over loopback gRPC with the system
// clock: the milestone-2 equivalent of separate processes, in one test binary.

#include "client/coordinator_client.h"
#include "coordinator/coordinator.h"
#include "store/in_memory_job_store.h"
#include "support/wait.h"
#include "worker/worker.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace epochflow::testing {
namespace {

using std::chrono::milliseconds;

HandlerResult Echo(const Assignment& a, std::stop_token) {
  return JobSucceeded{"done:" + a.payload};
}

class DistributedTest : public ::testing::Test {
 protected:
  DistributedTest() {
    CoordinatorOptions options;
    options.listen_address = "127.0.0.1:0";
    options.lease = milliseconds{300};
    options.reclaim_interval = milliseconds{20};
    coordinator_ = std::make_unique<Coordinator>(store_, options);
  }

  std::string Target() const { return "127.0.0.1:" + std::to_string(coordinator_->port()); }

  bool AllTerminal(const std::vector<JobId>& ids) {
    for (JobId id : ids) {
      if (!IsTerminal(store_.Get(id)->state)) return false;
    }
    return true;
  }

  SystemClock clock_;
  InMemoryJobStore store_{clock_};
  std::unique_ptr<Coordinator> coordinator_;
};

WorkerOptions FastWorker() {
  WorkerOptions options;
  options.idle_backoff = milliseconds{5};
  options.report_backoff = milliseconds{5};
  return options;
}

TEST_F(DistributedTest, WorkersCompleteEveryJobThroughTheCoordinator) {
  CoordinatorClient submitter(Target());
  std::vector<JobId> ids;
  for (int i = 0; i < 30; ++i) ids.push_back(submitter.Submit({"p" + std::to_string(i), 3}));

  std::vector<std::unique_ptr<CoordinatorClient>> clients;
  std::vector<std::unique_ptr<Worker>> workers;
  std::vector<std::jthread> threads;
  for (int i = 0; i < 3; ++i) {
    clients.push_back(std::make_unique<CoordinatorClient>(Target()));
    workers.push_back(std::make_unique<Worker>(*clients.back(), Echo, "w" + std::to_string(i),
                                               FastWorker()));
    threads.emplace_back([w = workers.back().get()](std::stop_token s) { w->Run(s); });
  }

  ASSERT_TRUE(WaitUntil([&] { return AllTerminal(ids); }, std::chrono::seconds{10}));
  threads.clear();  // stop and join
  for (JobId id : ids) {
    const auto job = store_.Get(id);
    EXPECT_EQ(job->state, JobState::kCompleted);
    EXPECT_EQ(job->result, "done:" + job->payload);
    EXPECT_EQ(job->attempts, 1);
  }
}

TEST_F(DistributedTest, CrashedWorkersJobIsRecoveredByAnotherWorker) {
  CoordinatorClient crashed(Target());
  const JobId id = crashed.Submit({"a", 3});
  // Acquires, then never renews or reports: indistinguishable from a crash.
  const auto lost = crashed.Acquire("crashed");
  ASSERT_TRUE(lost.has_value());

  CoordinatorClient client(Target());
  Worker survivor(client, Echo, "survivor", FastWorker());
  std::jthread thread([&](std::stop_token s) { survivor.Run(s); });

  ASSERT_TRUE(WaitUntil([&] { return store_.Get(id)->state == JobState::kCompleted; }));
  thread = {};
  const auto job = store_.Get(id);
  EXPECT_EQ(job->attempts, 2);
  EXPECT_EQ(job->generation, 2);
  // The "crashed" worker comes back and tries to report: fenced.
  EXPECT_EQ(crashed.Complete(id, lost->generation, "late"), CommitStatus::kStale);
  EXPECT_EQ(store_.Get(id)->result, "done:a");
}

}  // namespace
}  // namespace epochflow::testing
