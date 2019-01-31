cmake_minimum_required(VERSION 2.8.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++1z -Wall -ggdb -gdwarf-3 -pg")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -Wall -O3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -Wall -O3 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

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

add_library(derecho SHARED derecho_sst.cpp view.cpp view_manager.cpp rpc_manager.cpp p2p_connections.cpp multicast_group.cpp subgroup_functions.cpp connection_manager.cpp restart_state.cpp persistence_manager.cpp)
target_link_libraries(derecho rdmacm ibverbs rt pthread atomic rdmc sst mutils mutils-serialization persistent conf utils)
add_dependencies(derecho mutils_serialization_target mutils_target libfabric_target)

add_custom_target(format_derecho 
	COMMAND clang-format-6.0 -i *.cpp *.h
	WORKING_DIRECTORY ${derecho_SOURCE_DIR}/derecho
)
