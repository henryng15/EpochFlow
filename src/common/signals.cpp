#include "common/signals.h"

#include <pthread.h>
#include <signal.h>

namespace epochflow {
namespace {

sigset_t TerminationSignals() {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  return set;
}

}  // namespace

void BlockTerminationSignals() {
  const sigset_t set = TerminationSignals();
  pthread_sigmask(SIG_BLOCK, &set, nullptr);
}

int WaitForTerminationSignal() {
  const sigset_t set = TerminationSignals();
  int signal = 0;
  sigwait(&set, &signal);
  return signal;
}

}  // namespace epochflow
