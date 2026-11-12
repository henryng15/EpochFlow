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
  unit/tasks_test.cpp
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
  integration/pg_job_store_test.cpp
  contract/job_store_contract.cpp
)
target_include_directories(epochflow_integration_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_definitions(epochflow_integration_tests PRIVATE
  EPOCHFLOW_SOURCE_DIR="${PROJECT_SOURCE_DIR}")
target_link_libraries(epochflow_integration_tests PRIVATE
  epochflow_pg GTest::gtest_main)
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

# Coordinator restart against PostgreSQL. Needs EPOCHFLOW_DSN and psql; the
# script exits 77 (reported as skipped) without them. CTest labels "e2e", "db".
add_test(NAME e2e.restart
  COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/e2e/restart_test.sh ${PROJECT_BINARY_DIR}/bin)
set_tests_properties(e2e.restart PROPERTIES LABELS "e2e;db" TIMEOUT 120 SKIP_RETURN_CODE 77)

# Fault injection with real processes and PostgreSQL (Python standard library
# unittest). Exits 77 (skipped) without EPOCHFLOW_DSN or psql. Labels "faults",
# "db".
add_test(NAME faults COMMAND python3 ${CMAKE_CURRENT_SOURCE_DIR}/faults/test_faults.py)
set_tests_properties(faults PROPERTIES
  LABELS "faults;db" TIMEOUT 300 SKIP_RETURN_CODE 77
  ENVIRONMENT "EPOCHFLOW_BIN=${PROJECT_BINARY_DIR}/bin")

# Small chaos benchmark in both modes; fails unless the EpochFlow run completes
# every job with no duplicate effect and no invariant violation. Skipped
# without EPOCHFLOW_DSN. Labels "bench", "db".
add_test(NAME bench.smoke
  COMMAND sh -c "[ -n \"$EPOCHFLOW_DSN\" ] || exit 77; exec python3 '${PROJECT_SOURCE_DIR}/scripts/bench.py' --bin '${PROJECT_BINARY_DIR}/bin' --dsn \"$EPOCHFLOW_DSN\" --mode compare --jobs 60 --chaos-interval-ms 300 --check")
set_tests_properties(bench.smoke PROPERTIES LABELS "bench;db" TIMEOUT 300 SKIP_RETURN_CODE 77)

# Benchmark matrix: plan, resume, and aggregation logic (no database), then a
# one-seed matrix end to end. Labels "bench" (and "db" for the end-to-end run).
add_test(NAME bench.matrix_logic
  COMMAND python3 "${CMAKE_CURRENT_SOURCE_DIR}/bench/test_bench_matrix.py")
set_tests_properties(bench.matrix_logic PROPERTIES LABELS "bench")
add_test(NAME bench.matrix_smoke
  COMMAND sh -c "[ -n \"$EPOCHFLOW_DSN\" ] || exit 77; out='${CMAKE_CURRENT_BINARY_DIR}/bench-matrix-smoke'; rm -rf \"$out\" && python3 '${PROJECT_SOURCE_DIR}/scripts/bench_matrix.py' run --bin '${PROJECT_BINARY_DIR}/bin' --dsn \"$EPOCHFLOW_DSN\" --out \"$out\" --seeds 1 --workers 2 --jobs 30 && python3 '${PROJECT_SOURCE_DIR}/scripts/bench_matrix.py' aggregate --out \"$out\" && grep -q '| 2 | 30 | epochflow | 1 | 0 | 0 |' \"$out/summary.md\"")
set_tests_properties(bench.matrix_smoke PROPERTIES LABELS "bench;db" TIMEOUT 300 SKIP_RETURN_CODE 77)
