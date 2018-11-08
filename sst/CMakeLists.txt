cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
PROJECT(sst CXX)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++1z -O0 -Wall -ggdb -gdwarf-3 -pg")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -O3 -Wall")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -O3 -Wall -ggdb -gdwarf-3")

include_directories(${derecho_SOURCE_DIR})
include_directories(${derecho_SOURCE_DIR}/third_party/spdlog/include)
include_directories(${derecho_SOURCE_DIR}/third_party/libfabric/include)
link_directories(${derecho_SOURCE_DIR}/third_party/libfabric/src/.libs)


# ADD_LIBRARY(sst SHARED verbs.cpp lf.cpp poll_utils.cpp ../derecho/connection_manager.cpp)
ADD_LIBRARY(sst SHARED lf.cpp poll_utils.cpp ../derecho/connection_manager.cpp)
TARGET_LINK_LIBRARIES(sst conf tcp rdmacm fabric ibverbs pthread rt) 
add_dependencies(sst libfabric_target)

add_custom_target(format_sst clang-format-3.8 -i *.cpp *.h)
