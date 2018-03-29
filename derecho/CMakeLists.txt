cmake_minimum_required(VERSION 2.8.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_CXX_FLAGS_DEBUG "-std=c++14 -Wall -ggdb -gdwarf-3 -DSPDLOG_DEBUG_ON -DSPDLOG_TRACE_ON")
set(CMAKE_CXX_FLAGS_RELEASE "-std=c++14 -Wall -O3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-std=c++14 -Wall -O3 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

add_subdirectory(experiments)

include_directories(${derecho_SOURCE_DIR})
include_directories(${derecho_SOURCE_DIR}/third_party)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)
include_directories(${derecho_SROUCE_DIR}/third_party/persistent)
include_directories(${derecho_SOURCE_DIR}/third_party/spdlog/include)

link_directories(${derecho_SOURCE_DIR}/third_party/mutils)
link_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)
link_directories(${derecho_SOURCE_DIR}/third_party/persistence)

add_library(derecho SHARED derecho_sst.cpp view.cpp view_manager.cpp rpc_manager.cpp multicast_group.cpp raw_subgroup.cpp subgroup_functions.cpp connection_manager.cpp)
target_link_libraries(derecho rdmacm ibverbs rt pthread atomic rdmc sst mutils mutils-serialization persistent)
add_dependencies(derecho mutils_serialization_target mutils_target)

add_executable(subgroup_function_tester subgroup_function_tester.cpp)
target_link_libraries(subgroup_function_tester derecho)

add_custom_target(format_derecho 
	COMMAND clang-format-3.8 -i *.cpp *.h
	WORKING_DIRECTORY ${derecho_SOURCE_DIR}/derecho
)
