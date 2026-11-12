#pragma once

namespace epochflow {

// Blocks SIGINT and SIGTERM in the calling thread. Call first in main(),
// before any thread starts, so every thread inherits the mask and the signals
// are only ever received by WaitForTerminationSignal.
void BlockTerminationSignals();

// Blocks until SIGINT or SIGTERM arrives; returns the signal number.
int WaitForTerminationSignal();

}  // namespace epochflow
