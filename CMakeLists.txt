cmake_minimum_required(VERSION 3.25)
project(EpochFlow LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)

# Dependencies come from Ubuntu 24.04 / PGDG apt packages; see
# docs/decisions/0001-cpp-toolchain-and-dependencies.md.
# Do not derive package prefixes from PATH: a toolchain on PATH (for example
# conda) would otherwise shadow the apt GTest/c-ares and mix ABIs. Set after
# project() so the compiler and generator are still found through PATH.
set(CMAKE_FIND_USE_SYSTEM_ENVIRONMENT_PATH OFF)
find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(GTest REQUIRED)
find_package(PkgConfig REQUIRED)
# libpqxx ships no CMake package on Ubuntu, only a pkg-config file.
pkg_check_modules(PQXX REQUIRED IMPORTED_TARGET libpqxx)

add_subdirectory(src)

enable_testing()
add_subdirectory(tests)
