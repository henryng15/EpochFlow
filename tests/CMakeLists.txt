include(GoogleTest)

# Discover tests when CTest runs, not at build time, so wrappers such as
# `setarch -R` (needed by ThreadSanitizer, see docs/testing.md) also apply to
# discovery.
set(CMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE PRE_TEST)

# Unit tests: no external services. CTest label "unit".
add_executable(epochflow_unit_tests
  unit/dependency_smoke_test.cpp
  unit/flags_test.cpp
  unit/job_state_test.cpp
  unit/in_memory_job_store_test.cpp
  unit/lease_keeper_test.cpp
  unit/local_runner_test.cpp
  unit/reclaimer_test.cpp
  unit/worker_test.cpp
  contract/job_store_contract.cpp
)
target_include_directories(epochflow_unit_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(epochflow_unit_tests PRIVATE
  epochflow gRPC::grpc++ protobuf::libprotobuf PkgConfig::PQXX GTest::gtest_main)
gtest_discover_tests(epochflow_unit_tests PROPERTIES LABELS unit)

# Integration tests: need PostgreSQL via EPOCHFLOW_DSN, skipped otherwise.
# CTest label "db".
add_executable(epochflow_integration_tests
  integration/postgres_smoke_test.cpp
)
target_link_libraries(epochflow_integration_tests PRIVATE
  PkgConfig::PQXX GTest::gtest_main)
gtest_discover_tests(epochflow_integration_tests PROPERTIES LABELS db)

# gRPC tests: coordinator, client, and workers over loopback. No external
# services. CTest label "grpc".
add_executable(epochflow_grpc_tests
  grpc/coordinator_grpc_test.cpp
  grpc/distributed_test.cpp
)
target_include_directories(epochflow_grpc_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(epochflow_grpc_tests PRIVATE epochflow_rpc GTest::gtest_main)
gtest_discover_tests(epochflow_grpc_tests PROPERTIES LABELS grpc)

# End-to-end: real coordinator and worker processes, including SIGKILL of a
# worker mid-job. CTest label "e2e".
add_test(NAME e2e.smoke
  COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/e2e/smoke_test.sh ${PROJECT_BINARY_DIR}/bin)
set_tests_properties(e2e.smoke PROPERTIES LABELS e2e TIMEOUT 120)
