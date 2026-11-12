#include "common/flags.h"

#include <charconv>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>

namespace epochflow {

Flags::Flags(int argc, char** argv, std::set<std::string> allowed) {
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    if (!arg.starts_with("--")) {
      positional_.emplace_back(arg);
      continue;
    }
    const std::string_view body = arg.substr(2);
    const auto eq = body.find('=');
    const std::string name(body.substr(0, eq));
    if (name.empty()) throw std::invalid_argument("empty flag name in '" + std::string(arg) + "'");
    if (!allowed.contains(name)) throw std::invalid_argument("unknown flag --" + name);
    values_[name] = eq == std::string_view::npos ? "true" : std::string(body.substr(eq + 1));
  }
}

std::string Flags::GetString(const std::string& name, const std::string& fallback) const {
  const auto it = values_.find(name);
  return it == values_.end() ? fallback : it->second;
}

std::int64_t Flags::GetInt(const std::string& name, std::int64_t fallback) const {
  const auto it = values_.find(name);
  if (it == values_.end()) return fallback;
  const std::string& text = it->second;
  std::int64_t value = 0;
  const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (text.empty() || ec != std::errc{} || end != text.data() + text.size()) {
    throw std::invalid_argument("--" + name + " expects an integer, got '" + text + "'");
  }
  return value;
}

}  // namespace epochflow
