# Generated protobuf and gRPC code for the coordinator API.
# Include as "epochflow/v1/epochflow.pb.h" and "epochflow/v1/epochflow.grpc.pb.h".
add_library(epochflow_proto STATIC epochflow/v1/epochflow.proto)
target_link_libraries(epochflow_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)
target_include_directories(epochflow_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

protobuf_generate(TARGET epochflow_proto LANGUAGE cpp
  IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
  PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
protobuf_generate(TARGET epochflow_proto LANGUAGE grpc
  GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
  PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
  IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
  PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
