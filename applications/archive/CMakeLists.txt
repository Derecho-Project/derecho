cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++1z -Wall -ggdb -gdwarf-3 -D_DEBUG -pg")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -Wall -O3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -Wall -O3 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

include_directories(${derecho_SOURCE_DIR})
include_directories(${derecho_SOURCE_DIR}/third_party)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)
include_directories(${derecho_SOURCE_DIR}/third_party/spdlog/include)
include_directories(${derecho_SOURCE_DIR}/third_party/libfabric/include)

link_directories(${derecho_SOURCE_DIR}/third_party/mutils)
link_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)
link_directories(${derecho_SOURCE_DIR}/third_party/libfabric/src/.libs)

# test_group_interface
add_executable(test_group_interface test_group_interface.cpp initialize.cpp )
target_link_libraries(test_group_interface derecho)

# single_sender_one_message
add_executable(single_sender_one_message single_sender_one_message.cpp initialize.cpp )
target_link_libraries(single_sender_one_message derecho)

# single_sender_multiple_messages
add_executable(single_sender_multiple_messages single_sender_multiple_messages.cpp initialize.cpp )
target_link_libraries(single_sender_multiple_messages derecho)

# multiple_senders_multiple_messages
add_executable(multiple_senders_multiple_messages multiple_senders_multiple_messages.cpp initialize.cpp )
target_link_libraries(multiple_senders_multiple_messages derecho)

# rdmc_test
add_executable(rdmc_test rdmc_test.cpp block_size.cpp)
target_link_libraries(rdmc_test rdmc derecho)

# derecho_bw_test
add_executable(derecho_bw_test derecho_bw_test.cpp block_size.cpp aggregate_bandwidth.cpp initialize.cpp )
target_link_libraries(derecho_bw_test derecho)

# subgroup_scaling
add_executable(subgroup_scaling_test subgroup_scaling_test.cpp block_size.cpp aggregate_bandwidth.cpp initialize.cpp)
target_link_libraries(subgroup_scaling_test derecho)

# latency_test
add_executable(latency_test latency_test.cpp block_size.cpp)
target_link_libraries(latency_test derecho)

# derecho_caller_test
add_executable(derecho_caller_test derecho_caller_test.cpp block_size.cpp)
target_link_libraries(derecho_caller_test derecho mutils mutils-serialization)

# p2p_latency_test
add_executable(p2p_latency_test p2p_latency_test.cpp block_size.cpp)
target_link_libraries(p2p_latency_test derecho mutils mutils-serialization)

# subgroup_test
add_executable(subgroup_test subgroup_test.cpp initialize.cpp)
target_link_libraries(subgroup_test derecho)

# raw_send_test
add_executable(raw_send_test raw_send_test.cpp block_size.cpp initialize.cpp)
target_link_libraries(raw_send_test derecho mutils mutils-serialization)

# ssd_bw_test
add_executable(ssd_bw_test ssd_bw_test.cpp block_size.cpp aggregate_bandwidth.cpp initialize.cpp )
target_link_libraries(ssd_bw_test derecho)

# gms_test
add_executable(gms_test gms_test.cpp block_size.cpp initialize.cpp)
target_link_libraries(gms_test derecho)

# gms_test
add_executable(gms_test2 gms_test2.cpp block_size.cpp initialize.cpp)
target_link_libraries(gms_test2 derecho)

# gms_bw_test
add_executable(gms_bw_test gms_bw_test.cpp initialize.cpp)
target_link_libraries(gms_bw_test derecho)

# window_size_test
add_executable(window_size_test window_size_test.cpp block_size.cpp aggregate_bandwidth.cpp initialize.cpp )
target_link_libraries(window_size_test derecho)

# write_to_ssd
add_executable(write_to_ssd write_to_ssd.cpp block_size.cpp aggregate_bandwidth.cpp initialize.cpp )
target_link_libraries(write_to_ssd derecho)

# timeline_test
add_executable(timeline_test timeline_test.cpp initialize.cpp)
target_link_libraries(timeline_test derecho)

add_executable(viewchange_timeline viewchange_timeline.cpp initialize.cpp)
target_link_libraries(viewchange_timeline derecho)

# p2p_query_test
add_executable(p2p_query_test p2p_query_test.cpp initialize.cpp)
target_link_libraries(p2p_query_test derecho)

add_executable(repeated_rpc_test repeated_rpc_test.cpp initialize.cpp)
target_link_libraries(repeated_rpc_test derecho)

# typed_subgroup_test
add_executable(typed_subgroup_test typed_subgroup_test.cpp initialize.cpp)
target_link_libraries(typed_subgroup_test derecho)

# long_subgroup_test
add_executable(long_subgroup_test long_subgroup_test.cpp initialize.cpp)
target_link_libraries(long_subgroup_test derecho)

# typed_subgroup_bw_test
add_executable(typed_subgroup_bw_test typed_subgroup_bw_test.cpp block_size.cpp initialize.cpp)
target_link_libraries(typed_subgroup_bw_test derecho)

# persistent_typed_subgroup_bw_test
add_executable(persistent_typed_subgroup_bw_test persistent_typed_subgroup_bw_test.cpp block_size.cpp initialize.cpp)
target_link_libraries(persistent_typed_subgroup_bw_test derecho)

# objstore
add_executable(objstore objstore.cpp block_size.cpp initialize.cpp)
target_link_libraries(objstore derecho)

# volatile_typed_subgroup_bw_test
add_executable(volatile_typed_subgroup_bw_test volatile_typed_subgroup_bw_test.cpp block_size.cpp initialize.cpp)
target_link_libraries(volatile_typed_subgroup_bw_test derecho)

# volatile_temporal_query_test
add_executable(volatile_temporal_query_test volatile_temporal_query_test.cpp block_size.cpp initialize.cpp)
target_link_libraries(volatile_temporal_query_test derecho)

# persistent_temporal_query_test
add_executable(persistent_temporal_query_test persistent_temporal_query_test.cpp block_size.cpp initialize.cpp)
target_link_libraries(persistent_temporal_query_test derecho)

# volatile_temporal_stability_test.cpp
add_executable(volatile_temporal_stability_test volatile_temporal_stability_test.cpp block_size.cpp initialize.cpp)
target_link_libraries(volatile_temporal_stability_test derecho)

# persistent_temporal_stability_test
add_executable(persistent_temporal_stability_test persistent_temporal_stability_test.cpp block_size.cpp initialize.cpp)
target_link_libraries(persistent_temporal_stability_test derecho)

# persistent_latency_test
add_executable(persistent_latency_test persistent_latency_test.cpp block_size.cpp initialize.cpp)
target_link_libraries(persistent_latency_test derecho)

# smart membership function
add_executable(smart_membership_function_test smart_membership_function_test.cpp initialize.cpp)
target_link_libraries(smart_membership_function_test derecho)

# persistent_typed_subgroup_test
add_executable(persistent_typed_subgroup_test persistent_typed_subgroup_test.cpp initialize.cpp)
target_link_libraries(persistent_typed_subgroup_test derecho)

# persistent_vector
add_executable(persistent_vector persistent_vector.cpp initialize.cpp)
target_link_libraries(persistent_vector derecho)

#shard_iterator_p2p_test
add_executable(shard_iterator_p2p_test shard_iterator_p2p_test.cpp initialize.cpp)
target_link_libraries(shard_iterator_p2p_test derecho)

#persistent_rejoin_test
add_executable(persistent_rejoin_test persistent_rejoin_test.cpp initialize.cpp)
target_link_libraries(persistent_rejoin_test derecho)

add_executable(total_restart_test total_restart_test.cpp initialize.cpp)
target_link_libraries(total_restart_test derecho)

# scaling_subgroup_test
add_executable(scaling_subgroup_test scaling_subgroup_test.cpp initialize.cpp)
target_link_libraries(scaling_subgroup_test derecho)

# server_client_p2p
add_executable(server_client_p2p server_client_p2p.cpp initialize.cpp)
target_link_libraries(server_client_p2p derecho)

# asynchronous_dht
add_executable(asynchronous_dht asynchronous_dht.cpp initialize.cpp)
target_link_libraries(asynchronous_dht derecho)

# simple_send
add_executable(simple_send simple_send.cpp)
target_link_libraries(simple_send derecho)

# random_messages
add_executable(random_messages random_messages.cpp)
target_link_libraries(random_messages derecho)

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
#add_executable(multicast_throughput multicast_throughput.cpp ${derecho_SOURCE_DIR}/derecho/experiments/aggregate_bandwidth.cpp)
#target_link_libraries(multicast_throughput sst)

# # cpu_load_experiment
# add_executable(cpu_load_experiment cpu_load_experiment.cpp ${derecho_SOURCE_DIR}/derecho/experiments/aggregate_bandwidth.cpp)
# target_link_libraries(cpu_load_experiment sst)

# write_avg_time
add_executable(write_avg_time write_avg_time.cpp compute_nodes_list.cpp)
target_link_libraries(write_avg_time sst)

# rdma_two_sided
add_executable(rdma_two_sided rdma_two_sided.cpp)
target_link_libraries(rdma_two_sided sst)

