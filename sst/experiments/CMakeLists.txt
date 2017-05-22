cmake_minimum_required(VERSION 2.8)

PROJECT(sst CXX)
set(CMAKE_CXX_FLAGS "-std=c++14 -O3 -Wall -ggdb -gdwarf-3")

include_directories(${derecho_SOURCE_DIR})

# count_write
add_executable(count_write count_write.cpp)
target_link_libraries(count_write sst rdmc)

# sst_test
add_executable(sst_test sst_test.cpp)
target_link_libraries(sst_test sst rdmc)

# simple_predicate
add_executable(simple_predicate simple_predicate.cpp)
target_link_libraries(simple_predicate sst rdmc)

# test_write
add_executable(test_write test_write.cpp)
target_link_libraries(test_write sst rdmc)

# multicast_latency
add_executable(multicast_latency multicast_latency.cpp)
target_link_libraries(multicast_latency sst rdmc)

# multicast_throughput
add_executable(multicast_throughput multicast_throughput.cpp ${derecho_SOURCE_DIR}/derecho/experiments/aggregate_bandwidth.cpp)
target_link_libraries(multicast_throughput sst rdmc)

# cpu_load_experiment
add_executable(cpu_load_experiment cpu_load_experiment.cpp ${derecho_SOURCE_DIR}/derecho/experiments/aggregate_bandwidth.cpp)
target_link_libraries(cpu_load_experiment sst rdmc)

# format experiments
add_custom_target(format_sst_experiments clang-format-3.8 -i *.cpp *.h)
