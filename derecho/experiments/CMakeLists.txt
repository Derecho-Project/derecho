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

# rdmc_test
add_executable(rdmc_test rdmc_test.cpp)
target_link_libraries(rdmc_test rdmc derecho)

# derecho_bw_test
add_executable(derecho_bw_test derecho_bw_test.cpp aggregate_bandwidth.cpp)
target_link_libraries(derecho_bw_test derecho)

# subgroup_scaling
add_executable(subgroup_scaling_test subgroup_scaling_test.cpp aggregate_bandwidth.cpp )
target_link_libraries(subgroup_scaling_test derecho)

# latency_test
add_executable(latency_test latency_test.cpp)
target_link_libraries(latency_test derecho)

# derecho_caller_test
add_executable(derecho_caller_test derecho_caller_test.cpp)
target_link_libraries(derecho_caller_test derecho mutils mutils-serialization)

# p2p_latency_test
add_executable(p2p_latency_test p2p_latency_test.cpp)
target_link_libraries(p2p_latency_test derecho mutils mutils-serialization)

# subgroup_test
add_executable(subgroup_test subgroup_test.cpp )
target_link_libraries(subgroup_test derecho)

# p2p_query_test
add_executable(p2p_query_test p2p_query_test.cpp )
target_link_libraries(p2p_query_test derecho)

# typed_subgroup_test
add_executable(typed_subgroup_test typed_subgroup_test.cpp )
target_link_libraries(typed_subgroup_test derecho)

# long_subgroup_test
add_executable(long_subgroup_test long_subgroup_test.cpp )
target_link_libraries(long_subgroup_test derecho)

# typed_subgroup_bw_test
add_executable(typed_subgroup_bw_test typed_subgroup_bw_test.cpp)
target_link_libraries(typed_subgroup_bw_test derecho)

# persistent_typed_subgroup_bw_test
add_executable(persistent_typed_subgroup_bw_test persistent_typed_subgroup_bw_test.cpp )
target_link_libraries(persistent_typed_subgroup_bw_test derecho)

# objstore
add_executable(objstore objstore.cpp )
target_link_libraries(objstore derecho)

# volatile_typed_subgroup_bw_test
add_executable(volatile_typed_subgroup_bw_test volatile_typed_subgroup_bw_test.cpp )
target_link_libraries(volatile_typed_subgroup_bw_test derecho)

# volatile_temporal_query_test
add_executable(volatile_temporal_query_test volatile_temporal_query_test.cpp )
target_link_libraries(volatile_temporal_query_test derecho)

# persistent_temporal_query_test
add_executable(persistent_temporal_query_test persistent_temporal_query_test.cpp )
target_link_libraries(persistent_temporal_query_test derecho)

# volatile_temporal_stability_test.cpp
add_executable(volatile_temporal_stability_test volatile_temporal_stability_test.cpp )
target_link_libraries(volatile_temporal_stability_test derecho)

# persistent_temporal_stability_test
add_executable(persistent_temporal_stability_test persistent_temporal_stability_test.cpp )
target_link_libraries(persistent_temporal_stability_test derecho)

# persistent_latency_test
add_executable(persistent_latency_test persistent_latency_test.cpp )
target_link_libraries(persistent_latency_test derecho)

# smart membership function
add_executable(smart_membership_function_test smart_membership_function_test.cpp )
target_link_libraries(smart_membership_function_test derecho)

# persistent_typed_subgroup_test
add_executable(persistent_typed_subgroup_test persistent_typed_subgroup_test.cpp )
target_link_libraries(persistent_typed_subgroup_test derecho)

# persistent_vector
add_executable(persistent_vector persistent_vector.cpp )
target_link_libraries(persistent_vector derecho)

# shard_iterator_p2p_test
add_executable(shard_iterator_p2p_test shard_iterator_p2p_test.cpp )
target_link_libraries(shard_iterator_p2p_test derecho)

# persistent_rejoin_test
add_executable(persistent_rejoin_test persistent_rejoin_test.cpp )
target_link_libraries(persistent_rejoin_test derecho)

# scaling_subgroup_test
add_executable(scaling_subgroup_test scaling_subgroup_test.cpp )
target_link_libraries(scaling_subgroup_test derecho)

# server_client_p2p
add_executable(server_client_p2p server_client_p2p.cpp )
target_link_libraries(server_client_p2p derecho)

# asynchronous_dht
add_executable(asynchronous_dht asynchronous_dht.cpp )
target_link_libraries(asynchronous_dht derecho)

# simple_send
add_executable(simple_send simple_send.cpp)
target_link_libraries(simple_send derecho)

# random_messages
add_executable(random_messages random_messages.cpp)
target_link_libraries(random_messages derecho)

add_custom_target(format_experiments clang-format-3.8 -i *.cpp *.h)
