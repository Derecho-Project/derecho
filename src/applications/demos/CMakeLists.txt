cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_CXX_FLAGS "-fPIC -std=c++1z -Wall")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -gdwarf-3 -D_DEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/include)

# random_messages
add_executable(random_messages random_messages.cpp)
target_link_libraries(random_messages derecho)

# simple_replicated_objects
add_executable(simple_replicated_objects simple_replicated_objects.cpp)
target_link_libraries(simple_replicated_objects derecho)

# overlapping_replicated_objects
add_executable(overlapping_replicated_objects overlapping_replicated_objects.cpp)
target_link_libraries(overlapping_replicated_objects derecho)

# simple_data_distribution_service (dds)
add_executable(dds_demo simple_data_distribution_service.cpp dds_demo.cpp)
target_link_libraries(dds_demo dpods)
