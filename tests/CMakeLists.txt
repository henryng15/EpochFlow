include(GoogleTest)

# Unit tests: no external services. CTest label "unit".
add_executable(epochflow_unit_tests
  unit/dependency_smoke_test.cpp
)
target_link_libraries(epochflow_unit_tests PRIVATE
  gRPC::grpc++ protobuf::libprotobuf PkgConfig::PQXX GTest::gtest_main)
gtest_discover_tests(epochflow_unit_tests PROPERTIES LABELS unit)

# Integration tests: need PostgreSQL via EPOCHFLOW_DSN, skipped otherwise.
# CTest label "db".
add_executable(epochflow_integration_tests
  integration/postgres_smoke_test.cpp
)
target_link_libraries(epochflow_integration_tests PRIVATE
  PkgConfig::PQXX GTest::gtest_main)
gtest_discover_tests(epochflow_integration_tests PROPERTIES LABELS db)
