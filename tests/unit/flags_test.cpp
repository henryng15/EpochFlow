#include "common/flags.h"

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <vector>

namespace epochflow::testing {
namespace {

Flags Parse(std::vector<std::string> args, std::set<std::string> allowed) {
  std::vector<char*> argv{const_cast<char*>("prog")};
  for (auto& a : args) argv.push_back(a.data());
  return Flags(static_cast<int>(argv.size()), argv.data(), std::move(allowed));
}

TEST(Flags, ParsesNameEqualsValue) {
  const Flags f = Parse({"--listen=127.0.0.1:0", "--lease-ms=500"}, {"listen", "lease-ms"});
  EXPECT_EQ(f.GetString("listen", ""), "127.0.0.1:0");
  EXPECT_EQ(f.GetInt("lease-ms", 0), 500);
}

TEST(Flags, ValueMayContainEqualsSign) {
  const Flags f = Parse({"--payload=a=b"}, {"payload"});
  EXPECT_EQ(f.GetString("payload", ""), "a=b");
}

TEST(Flags, BareFlagIsPresentWithValueTrue) {
  const Flags f = Parse({"--help"}, {"help"});
  EXPECT_TRUE(f.Has("help"));
  EXPECT_EQ(f.GetString("help", ""), "true");
}

TEST(Flags, ReturnsDefaultsWhenAbsent) {
  const Flags f = Parse({}, {"listen", "lease-ms"});
  EXPECT_FALSE(f.Has("listen"));
  EXPECT_EQ(f.GetString("listen", "fallback"), "fallback");
  EXPECT_EQ(f.GetInt("lease-ms", 42), 42);
}

TEST(Flags, KeepsPositionalArgumentsInOrder) {
  const Flags f = Parse({"wait", "--timeout-ms=5", "1", "2"}, {"timeout-ms"});
  EXPECT_EQ(f.positional(), (std::vector<std::string>{"wait", "1", "2"}));
}

TEST(Flags, RejectsUnknownFlag) {
  EXPECT_THROW(Parse({"--lease=5"}, {"lease-ms"}), std::invalid_argument);
}

TEST(Flags, RejectsEmptyFlagName) {
  EXPECT_THROW(Parse({"--=5"}, {"lease-ms"}), std::invalid_argument);
}

TEST(Flags, RejectsNonIntegerValues) {
  const Flags f = Parse({"--lease-ms=12x", "--count=", "--big=99999999999999999999"},
                        {"lease-ms", "count", "big"});
  EXPECT_THROW(f.GetInt("lease-ms", 0), std::invalid_argument);
  EXPECT_THROW(f.GetInt("count", 0), std::invalid_argument);
  EXPECT_THROW(f.GetInt("big", 0), std::invalid_argument);
}

TEST(Flags, ParsesNegativeIntegers) {
  const Flags f = Parse({"--max-attempts=-1"}, {"max-attempts"});
  EXPECT_EQ(f.GetInt("max-attempts", 0), -1);
}

}  // namespace
}  // namespace epochflow::testing
