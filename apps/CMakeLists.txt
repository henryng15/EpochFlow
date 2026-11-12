# Executables. Output to build/bin so scripts find all of them in one place.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)

add_executable(epochflow-coordinator coordinator_main.cpp)
target_link_libraries(epochflow-coordinator PRIVATE epochflow_rpc epochflow_pg)

add_executable(epochflow-worker worker_main.cpp)
target_link_libraries(epochflow-worker PRIVATE epochflow_rpc)

add_executable(epochflowctl epochflowctl.cpp)
target_link_libraries(epochflowctl PRIVATE epochflow_rpc)
