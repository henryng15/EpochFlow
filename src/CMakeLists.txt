# Core library shared by the runner, coordinator, worker, and tests.
add_library(epochflow STATIC
  common/flags.cpp
  common/log.cpp
  common/signals.cpp
  coordinator/reclaimer.cpp
  core/job.cpp
  core/job_state.cpp
  store/in_memory_job_store.cpp
  worker/lease_keeper.cpp
  worker/worker.cpp
  runner/local_runner.cpp
)
target_include_directories(epochflow PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(epochflow PUBLIC Threads::Threads)

# gRPC layer: coordinator server, client, and protocol conversion. Kept apart
# from the core library so core code and its unit tests do not depend on gRPC.
add_library(epochflow_rpc STATIC
  client/coordinator_client.cpp
  coordinator/coordinator.cpp
  coordinator/coordinator_service.cpp
  rpc/convert.cpp
)
target_link_libraries(epochflow_rpc PUBLIC epochflow epochflow_proto)

# PostgreSQL store. Migrations in sql/migrations/NNNN_name.sql are embedded into
# the library at configure time; editing or adding one re-runs CMake.
file(GLOB EPOCHFLOW_MIGRATION_FILES CONFIGURE_DEPENDS
  ${PROJECT_SOURCE_DIR}/sql/migrations/*.sql)
list(SORT EPOCHFLOW_MIGRATION_FILES)
set(EPOCHFLOW_MIGRATION_ENTRIES "")
foreach(migration IN LISTS EPOCHFLOW_MIGRATION_FILES)
  get_filename_component(migration_name ${migration} NAME_WE)
  if(NOT migration_name MATCHES "^([0-9]+)_[a-z0-9_]+$")
    message(FATAL_ERROR "migration ${migration} must be named NNNN_name.sql")
  endif()
  math(EXPR migration_version "${CMAKE_MATCH_1}")
  file(READ ${migration} migration_sql)
  if(migration_sql MATCHES "\\)__sql__")
    message(FATAL_ERROR "${migration} contains the raw-string delimiter )__sql__")
  endif()
  string(APPEND EPOCHFLOW_MIGRATION_ENTRIES
    "      {${migration_version}, \"${migration_name}\", R\"__sql__(${migration_sql})__sql__\"},\n")
endforeach()
configure_file(store/embedded_migrations.cpp.in
  ${CMAKE_CURRENT_BINARY_DIR}/embedded_migrations.cpp @ONLY)

add_library(epochflow_pg STATIC
  store/migrations.cpp
  store/pg_connection_pool.cpp
  store/pg_job_store.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/embedded_migrations.cpp
)
target_link_libraries(epochflow_pg PUBLIC epochflow PkgConfig::PQXX)
