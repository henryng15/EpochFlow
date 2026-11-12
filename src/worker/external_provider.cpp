#include "worker/external_provider.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <format>
#include <utility>

namespace epochflow {

FileProvider::FileProvider(std::string path, bool time_out_after_charge)
    : path_(std::move(path)), time_out_after_charge_(time_out_after_charge) {}

std::string FileProvider::Charge(const std::string& reference, const std::string& request) {
  const std::string confirmation = std::format("ch_{}_{}", ::getpid(), ++counter_);
  const std::string line = std::format("{}\t{}\t{}\n", reference, request, confirmation);
  const int fd = ::open(path_.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
  if (fd < 0) throw std::runtime_error("cannot open provider log " + path_ + ": " + std::strerror(errno));
  const ssize_t written = ::write(fd, line.data(), line.size());
  ::close(fd);
  if (written != static_cast<ssize_t>(line.size())) {
    throw std::runtime_error("short write to provider log " + path_);
  }
  if (time_out_after_charge_) throw ProviderTimeout("provider did not respond in time");
  return confirmation;
}

}  // namespace epochflow
