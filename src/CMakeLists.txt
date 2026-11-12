# Core library shared by the runner, coordinator, worker, and tests.
add_library(epochflow STATIC
  core/job.cpp
  core/job_state.cpp
  store/in_memory_job_store.cpp
  worker/lease_keeper.cpp
  runner/local_runner.cpp
)
target_include_directories(epochflow PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(epochflow PUBLIC Threads::Threads)
