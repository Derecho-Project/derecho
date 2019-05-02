cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_CXX_FLAGS "-std=c++1z -Wall -fPIC")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -ggdb -gdwarf-3 -D_DEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# subgroup_scaling_test
add_executable(subgroup_scaling_test subgroup_scaling_test.cpp aggregate_bandwidth.cpp)
target_link_libraries(subgroup_scaling_test derecho)

# derecho_caller_test
add_executable(derecho_caller_test derecho_caller_test.cpp)
target_link_libraries(derecho_caller_test derecho)

# p2p_latency_test
add_executable(p2p_latency_test p2p_latency_test.cpp)
target_link_libraries(p2p_latency_test derecho)

# subgroup_test
add_executable(subgroup_test subgroup_test.cpp)
target_link_libraries(subgroup_test derecho)

# p2p_send_test
add_executable(p2p_send_test p2p_send_test.cpp)
target_link_libraries(p2p_send_test derecho)

# typed_subgroup_test
add_executable(typed_subgroup_test typed_subgroup_test.cpp)
target_link_libraries(typed_subgroup_test derecho)

# long_subgroup_test
add_executable(long_subgroup_test long_subgroup_test.cpp)
target_link_libraries(long_subgroup_test derecho)

# typed_subgroup_bw_test
add_executable(typed_subgroup_bw_test typed_subgroup_bw_test.cpp)
target_link_libraries(typed_subgroup_bw_test derecho)

# persistent_typed_subgroup_bw_test
add_executable(persistent_typed_subgroup_bw_test persistent_typed_subgroup_bw_test.cpp)
target_link_libraries(persistent_typed_subgroup_bw_test derecho)

# objstore
add_executable(objstore objstore.cpp)
target_link_libraries(objstore derecho)

# volatile_typed_subgroup_bw_test
add_executable(volatile_typed_subgroup_bw_test volatile_typed_subgroup_bw_test.cpp)
target_link_libraries(volatile_typed_subgroup_bw_test derecho)

# volatile_temporal_send_test
add_executable(volatile_temporal_send_test volatile_temporal_send_test.cpp)
target_link_libraries(volatile_temporal_send_test derecho)

# persistent_temporal_send_test
add_executable(persistent_temporal_send_test persistent_temporal_send_test.cpp)
target_link_libraries(persistent_temporal_send_test derecho)

# volatile_temporal_stability_test
add_executable(volatile_temporal_stability_test volatile_temporal_stability_test.cpp)
target_link_libraries(volatile_temporal_stability_test derecho)

# persistent_temporal_stability_test
add_executable(persistent_temporal_stability_test persistent_temporal_stability_test.cpp)
target_link_libraries(persistent_temporal_stability_test derecho)

# persistent_latency_test
add_executable(persistent_latency_test persistent_latency_test.cpp)
target_link_libraries(persistent_latency_test derecho)

# smart_membership_function
add_executable(smart_membership_function_test smart_membership_function_test.cpp)
target_link_libraries(smart_membership_function_test derecho)

# persistent_typed_subgroup_test
add_executable(persistent_typed_subgroup_test persistent_typed_subgroup_test.cpp)
target_link_libraries(persistent_typed_subgroup_test derecho)

# persistent_vector
add_executable(persistent_vector persistent_vector.cpp)
target_link_libraries(persistent_vector derecho)

# shard_iterator_p2p_test
add_executable(shard_iterator_p2p_test shard_iterator_p2p_test.cpp)
target_link_libraries(shard_iterator_p2p_test derecho)

# persistent_rejoin_test
add_executable(persistent_rejoin_test persistent_rejoin_test.cpp)
target_link_libraries(persistent_rejoin_test derecho)

# total_restart_test
add_executable(total_restart_test total_restart_test.cpp)
target_link_libraries(total_restart_test derecho)

# scaling_subgroup_test
add_executable(scaling_subgroup_test scaling_subgroup_test.cpp)
target_link_libraries(scaling_subgroup_test derecho)

# server_client_p2p
add_executable(server_client_p2p server_client_p2p.cpp)
target_link_libraries(server_client_p2p derecho)

# asynchronous_dht
add_executable(asynchronous_dht asynchronous_dht.cpp)
target_link_libraries(asynchronous_dht derecho)

# simple_send
add_executable(simple_send simple_send.cpp)
target_link_libraries(simple_send derecho)

# debug_failure_test
add_executable(debug_failure_test debug_failure_test.cpp)
target_link_libraries(debug_failure_test derecho)

# count_write
add_executable(count_write count_write.cpp)
target_link_libraries(count_write derecho)

# rdma_for_ml.cpp
add_executable(rdma_for_ml rdma_for_ml.cpp)
target_link_libraries(rdma_for_ml derecho)

# sst_test
add_executable(sst_test sst_test.cpp)
target_link_libraries(sst_test derecho)

# simple_predicate
add_executable(simple_predicate simple_predicate.cpp)
target_link_libraries(simple_predicate derecho)

# test_write
add_executable(test_write test_write.cpp)
target_link_libraries(test_write derecho)

# write_avg_time
add_executable(write_avg_time write_avg_time.cpp compute_nodes_list.cpp)
target_link_libraries(write_avg_time derecho)

# rdma_two_sided
add_executable(rdma_two_sided rdma_two_sided.cpp)
target_link_libraries(rdma_two_sided derecho)

# membership_tests
add_executable(membership_tests membership_tests.cpp)
target_link_libraries(membership_tests derecho)

add_executable(custom_subgroup_profiles_test custom_subgroup_profiles_test.cpp)
target_link_libraries(custom_subgroup_profiles_test derecho)
