# Core library shared by the runner, coordinator, worker, and tests.
add_library(epochflow STATIC
  common/log.cpp
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
