cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_CIRRENT_SOURCE_DIR})

# bandwidth_test
add_executable(bandwidth_test bandwidth_test.cpp aggregate_bandwidth.cpp)
target_link_libraries(bandwidth_test derecho)

# latency_test
add_executable(latency_test latency_test.cpp aggregate_latency.cpp)
target_link_libraries(latency_test derecho)
