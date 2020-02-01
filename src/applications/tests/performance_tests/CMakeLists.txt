cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_CXX_FLAGS "-std=c++1z -Wall -fPIC")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -gdwarf-3 -D_DEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_CIRRENT_SOURCE_DIR})

# bandwidth_test
add_executable(bandwidth_test bandwidth_test.cpp aggregate_bandwidth.cpp)
target_link_libraries(bandwidth_test derecho)

# latency_test
add_executable(latency_test latency_test.cpp aggregate_latency.cpp)
target_link_libraries(latency_test derecho)

# single_active_subgroup_test
add_executable(single_active_subgroup_test single_active_subgroup_test.cpp aggregate_bandwidth.cpp)
target_link_libraries(single_active_subgroup_test derecho)

# multiple_active_subgroups_test
add_executable(multiple_active_subgroups_test multiple_active_subgroups_test.cpp aggregate_bandwidth.cpp)
target_link_libraries(multiple_active_subgroups_test derecho)

# sender_delay_test
add_executable(sender_delay_test sender_delay_test.cpp aggregate_bandwidth.cpp)
target_link_libraries(sender_delay_test derecho)

# sender_delay_test
add_executable(deliver_predicate_delay deliver_predicate_delay.cpp aggregate_bandwidth.cpp)
target_link_libraries(deliver_predicate_delay derecho)