cmake_minimum_required(VERSION 3.5)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

# set(CMAKE_CXX_FLAGS "-fPIC -std=c++1z -Wall -DPROFILING -DMSHADOW_STAND_ALONE=1")
set(CMAKE_CXX_FLAGS "-fPIC -std=c++1z -Wall -DPROFILING")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -ggdb")

# The default, new approach to config the protobuf is using cmake's "config" mode, where
# a set of function (like 'generate_function') can be used to generate the protobuf stubs
# easily. However, those functions are not stable and sometimes generate stubs in unexpected
# location. For now, it's better to use the lecagy "module" mode to fully control over
# protobuf stub generation.
#
# The protobuf installation may disabled the legacy "module" mode. In such a case even
# you specified set(protobuf_MODULE_COMPATIBLE ON), in "config" mode, the compatibility
# will be overriden. See:
# protobuf-options.cmake:6:option(protobuf_MODULE_COMPATIBLE "CMake build-in FindProtobuf.cmake module compatible" OFF)"
# 
# Therefore, we force the cmake system to use the "MODULE" mode.
#
# find_package(Protobuf CONFIG REQUIRED)
find_package(Protobuf MODULE REQUIRED)
message(STATUS "Using protobuf ${protobuf_VERSION}")

# TODO: cmake system in grpc is incomplete we don't check it for now.
# find_package(gRPC CONFIG REQUIRED)
# message(STATUS "Using gRPC ${gRPC_VERSION}")

set(PROTO_PATH "${CMAKE_CURRENT_SOURCE_DIR}/protos")
set(FRONT_END_PROTO "${PROTO_PATH}/frontend.proto")
set(GENERATED_PROTOBUF_PATH "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GENERATED_PROTOBUF_PATH})

set(FRONT_END_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/frontend.pb.cc")
set(FRONT_END_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/frontend.pb.h")
set(FRONT_END_GRPC_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/frontend.grpc.pb.cc")
set(FRONT_END_GRPC_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/frontend.grpc.pb.h")
add_custom_command(
    OUTPUT "${FRONT_END_PB_H_FILE}"
           "${FRONT_END_PB_CPP_FILE}"
           "${FRONT_END_GRPC_PB_H_FILE}"
           "${FRONT_END_GRPC_PB_CPP_FILE}"
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS "--proto_path=${PROTO_PATH}"
         "--cpp_out=${GENERATED_PROTOBUF_PATH}"
         "${FRONT_END_PROTO}"
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS "--proto_path=${PROTO_PATH}"
         "--grpc_out=${GENERATED_PROTOBUF_PATH}"
         "--plugin=protoc-gen-grpc=/usr/local/bin/grpc_cpp_plugin"
         "${FRONT_END_PROTO}"
    DEPENDS "${FRONT_END_PROTO}"
    )
set(FRONT_END_PROTO_HDRS ${FRONT_END_PB_H_FILE} ${FRONT_END_GRPC_PB_H_FILE})
set(FRONT_END_PROTO_SRCS ${FRONT_END_PB_CPP_FILE} ${FRONT_END_GRPC_PB_CPP_FILE})


add_executable(sospdemo main.cpp frontend.cpp backend.cpp utils.cpp blob.cpp ${FRONT_END_PROTO_SRCS} ${FRONT_END_PROTO_HDRS})
target_include_directories(sospdemo PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${GENERATED_PROTOBUF_PATH}>
)
target_link_libraries(sospdemo derecho mutils mxnet fabric pthread protobuf grpc++)
