cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
PROJECT(sst CXX)
set(CMAKE_CXX_FLAGS_DEBUG "-std=c++14 -Wall -ggdb -gdwarf-3 -pg")
set(CMAKE_CXX_FLAGS_RELEASE "-std=c++14 -Wall -O3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-std=c++14 -Wall -O3 -ggdb -gdwarf-3")

include_directories(${derecho_SOURCE_DIR})

# count_write
add_executable(count_write count_write.cpp)
target_link_libraries(count_write sst)

# sst_test
add_executable(sst_test sst_test.cpp)
target_link_libraries(sst_test sst)

# simple_predicate
add_executable(simple_predicate simple_predicate.cpp)
target_link_libraries(simple_predicate sst)

# test_write
add_executable(test_write test_write.cpp)
target_link_libraries(test_write sst)

# # multicast_latency
# add_executable(multicast_latency multicast_latency.cpp)
# target_link_libraries(multicast_latency sst)

# multicast_throughput
add_executable(multicast_throughput multicast_throughput.cpp ${derecho_SOURCE_DIR}/derecho/experiments/aggregate_bandwidth.cpp)
target_link_libraries(multicast_throughput sst)

# # cpu_load_experiment
# add_executable(cpu_load_experiment cpu_load_experiment.cpp ${derecho_SOURCE_DIR}/derecho/experiments/aggregate_bandwidth.cpp)
# target_link_libraries(cpu_load_experiment sst)

# write_avg_time
add_executable(write_avg_time write_avg_time.cpp compute_nodes_list.cpp)
target_link_libraries(write_avg_time sst)

# rdma_two_sided
add_executable(rdma_two_sided rdma_two_sided.cpp)
target_link_libraries(rdma_two_sided sst)

# format experiments
add_custom_target(format_sst_experiments clang-format-3.8 -i *.cpp *.h)
