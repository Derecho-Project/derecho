cmake_minimum_required(VERSION 2.8.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z -Wall -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -O0 -ggdb -gdwarf-3 -pg")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS}-O3 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

include_directories(${derecho_SOURCE_DIR})
include_directories(${derecho_SOURCE_DIR}/third_party)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)
include_directories(${derecho_SOURCE_DIR}/third_party/spdlog/include)
# include_directories(${derecho_SOURCE_DIR}/third_party/libfabric/build/include)
include_directories(${derecho_SOURCE_DIR}/third_party/libfabric/include)

link_directories(${derecho_SOURCE_DIR}/third_party/mutils)
link_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)
# link_directories(${derecho_SOURCE_DIR}/third_party/libfabric/build/lib)
link_directories(${derecho_SOURCE_DIR}/third_party/libfabric/src/.libs)

add_library(core OBJECT derecho_sst.cpp view.cpp view_manager.cpp rpc_manager.cpp p2p_connections.cpp multicast_group.cpp subgroup_functions.cpp connection_manager.cpp restart_state.cpp persistence_manager.cpp)
target_include_directories(core PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include/private>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_custom_target(format_derecho 
	COMMAND clang-format-6.0 -i *.cpp *.h
	WORKING_DIRECTORY ${derecho_SOURCE_DIR}/derecho
)
