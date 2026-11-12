#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace epochflow {

// Minimal command-line parser for the EpochFlow binaries. Accepts
// "--name=value" and bare "--name" (value "true"); every other argument is
// positional. The space-separated "--name value" form is deliberately not
// supported, because it is ambiguous with positional arguments.
class Flags {
 public:
  // Skips argv[0]. Throws std::invalid_argument for a flag not in `allowed`
  // or with an empty name.
  Flags(int argc, char** argv, std::set<std::string> allowed);

  bool Has(const std::string& name) const { return values_.contains(name); }
  std::string GetString(const std::string& name, const std::string& fallback) const;
  // Throws std::invalid_argument if the value is not a base-10 int64.
  std::int64_t GetInt(const std::string& name, std::int64_t fallback) const;
  const std::vector<std::string>& positional() const { return positional_; }

 private:
  std::map<std::string, std::string> values_;
  std::vector<std::string> positional_;
};

}  // namespace epochflow
