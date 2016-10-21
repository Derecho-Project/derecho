cmake_minimum_required(VERSION 2.8.1)

set(CMAKE_CXX_FLAGS "-std=c++14 -Wall -ggdb -gdwarf-3")

add_subdirectory(experiments)

include_directories(${derecho_SOURCE_DIR})
include_directories(${derecho_SOURCE_DIR}/third_party)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)

link_directories(${derecho_SOURCE_DIR}/third_party/mutils)
link_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)

add_library(derecho SHARED derecho_row.cpp logger.cpp filewriter.cpp connection_manager.cpp)
target_link_libraries(derecho rdmacm ibverbs rt pthread atomic rdmc sst mutils mutils-serialization)
add_dependencies(derecho mutils_serialization_target mutils_target)

add_executable(main main.cpp)
target_link_libraries(main derecho)

add_executable(dynamic_connection_setup dynamic_connection_setup.cpp)
target_link_libraries(dynamic_connection_setup derecho rdmc)

add_executable(parse_state_file parse_state_file.cpp)
target_link_libraries(parse_state_file derecho)

add_executable(create_state_file create_state_file.cpp)
target_link_libraries(create_state_file derecho)

add_executable(latest_logged_message latest_logged_message.cpp)
target_link_libraries(latest_logged_message derecho)

add_executable(log_tail_length log_tail_length.cpp)
target_link_libraries(log_tail_length derecho)

add_executable(conntest connection_test.cpp)
target_link_libraries(conntest rdmc sst)

add_executable(gms_test gms_test.cpp experiments/block_size.cpp)
target_link_libraries(gms_test derecho)

add_executable(gms_test2 gms_test2.cpp experiments/block_size.cpp)
target_link_libraries(gms_test2 derecho)

add_executable(test_inheritance_plan test_inheritance_plan.cpp)
target_link_libraries(test_inheritance_plan mutils-serialization)

add_custom_target(format_derecho clang-format-3.6 -i *.cpp *.h)
