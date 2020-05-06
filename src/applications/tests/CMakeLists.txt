cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_subdirectory(unit_tests)
add_subdirectory(performance_tests)
add_subdirectory(scalability_tests)
