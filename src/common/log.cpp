#include "common/log.h"

#include <chrono>
#include <cstdio>
#include <format>
#include <string>

namespace epochflow {

void LogToStderr(std::string_view line) {
  const auto now = std::chrono::floor<std::chrono::milliseconds>(std::chrono::system_clock::now());
  const std::string out = std::format("{:%FT%TZ} {}\n", now, line);
  std::fwrite(out.data(), 1, out.size(), stderr);
}

}  // namespace epochflow
