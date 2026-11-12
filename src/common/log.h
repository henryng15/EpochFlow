#pragma once

#include <functional>
#include <string_view>

namespace epochflow {

// Receives one complete, human-readable event line. Components accept an
// optional LogFn so tests stay quiet and binaries choose the destination.
using LogFn = std::function<void(std::string_view line)>;

// Writes "<UTC timestamp> <line>\n" to stderr in one call, so concurrent lines
// do not interleave.
void LogToStderr(std::string_view line);

}  // namespace epochflow
