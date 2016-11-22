cmake_minimum_required(VERSION 2.8)

PROJECT(sst CXX)
set(CMAKE_CXX_FLAGS "-std=c++14 -Wall -ggdb -gdwarf-3")

include_directories(${derecho_SOURCE_DIR})

# count_read
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
