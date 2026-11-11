# 0001. C++ toolchain and dependency sourcing

## Status

Accepted (2026-11-11).

## Context

EpochFlow is a C++20 coordinator and worker system using gRPC/Protobuf for
transport, PostgreSQL for durable state, and GoogleTest for tests. The project
needs a build that is reproducible on a developer WSL2/Ubuntu machine and in CI,
without spending effort on dependency management.

Constraints:

- Development machines have limited memory (about 10 GB shared with the editor).
  Building gRPC from source is slow and memory-heavy.
- Developer machines may have other toolchains on `PATH`, such as conda, that
  ship their own GTest and c-ares builds.

## Decision

- **Language and compiler:** C++20, GCC 13 (Ubuntu 24.04 default).
- **Build:** CMake 3.25+ with Ninja, driven by `CMakePresets.json`. The `dev`
  build preset caps parallelism at 2 jobs to bound peak memory.
- **Dependencies come from apt, not from source or a package manager:**
  - gRPC 1.51 and Protobuf 3.21 from Ubuntu 24.04 (`libgrpc++-dev`,
    `protobuf-compiler-grpc`), found with `find_package(gRPC CONFIG)` and
    `find_package(Protobuf)`.
  - libpqxx 8.x from the PostgreSQL (PGDG) apt repository, found through
    `pkg-config` because the package has no CMake config. CI adds the same
    repository so local and CI builds use the same major version.
  - GoogleTest 1.14 from Ubuntu (`libgtest-dev`, `libgmock-dev`).
- **Test database:** `postgres:16` in Docker (`docker-compose.test.yml`), bound
  to `127.0.0.1:15432`. CI uses an identical service container.
- **Package lookup ignores `PATH`-derived prefixes**
  (`CMAKE_FIND_USE_SYSTEM_ENVIRONMENT_PATH OFF`, set after `project()`), so a
  conda install on `PATH` cannot shadow the system packages.

## Consequences

- Setup is one `apt install` command; no dependency builds.
- Versions are pinned by the distribution, not by the project. gRPC 1.51 is old
  but provides everything v1 needs (unary RPCs, deadlines, status codes).
- Upgrading Ubuntu or the PGDG repository can change dependency versions. CI on
  `ubuntu-24.04` detects breakage.
- Port 15432 sits below the Windows dynamic port range. Hyper-V reserves blocks
  inside that range (49152+), which change on reboot and made 55432 unbindable.
- Non-Ubuntu platforms are not supported in v1.

**Upgrade trigger:** if a needed gRPC/Protobuf feature is missing from the
distribution version, or a second platform must be supported, move to a pinned
package manager (vcpkg manifest mode).

## Alternatives considered

- **vcpkg or Conan:** reproducible, pinned versions, but builds gRPC from source
  (long, memory-heavy) and adds tooling the project does not otherwise need.
- **CMake `FetchContent` for gRPC:** same build cost, worse caching.
- **Raw libpq instead of libpqxx:** one fewer dependency, but every query would
  need hand-written RAII and parameter handling that libpqxx already provides.
- **Catch2 instead of GoogleTest:** comparable; GoogleTest has first-class CMake
  test discovery and gMock for later fault-injection seams.

## References

- [`CMakeLists.txt`](../../CMakeLists.txt), [`CMakePresets.json`](../../CMakePresets.json)
- [`docker-compose.test.yml`](../../docker-compose.test.yml)
- [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)
- [Testing](../testing.md)
