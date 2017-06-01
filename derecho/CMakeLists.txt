cmake_minimum_required(VERSION 2.8.1)

set(CMAKE_CXX_FLAGS "-std=c++14 -O3 -Wall -ggdb -gdwarf-3")

add_subdirectory(experiments)

include_directories(${derecho_SOURCE_DIR})
include_directories(${derecho_SOURCE_DIR}/third_party)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)
include_directories(${derecho_SOURCE_DIR}/third_party/spdlog/include)

link_directories(${derecho_SOURCE_DIR}/third_party/mutils)
link_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)

add_library(derecho SHARED derecho_sst.cpp view.cpp view_manager.cpp rpc_manager.cpp multicast_group.cpp raw_subgroup.cpp subgroup_functions.cpp filewriter.cpp connection_manager.cpp)
target_link_libraries(derecho rdmacm ibverbs rt pthread atomic rdmc sst mutils mutils-serialization)
add_dependencies(derecho mutils_serialization_target mutils_target)

add_executable(subgroup_function_tester subgroup_function_tester.cpp)
target_link_libraries(subgroup_function_tester derecho)

add_executable(parse_state_file parse_state_file.cpp)
target_link_libraries(parse_state_file derecho)

add_executable(create_state_file create_state_file.cpp)
target_link_libraries(create_state_file derecho)

add_executable(latest_logged_message latest_logged_message.cpp)
target_link_libraries(latest_logged_message derecho)

add_executable(log_tail_length log_tail_length.cpp)
target_link_libraries(log_tail_length derecho)

add_custom_target(format_derecho clang-format-3.8 -i *.cpp *.h)
