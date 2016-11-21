cmake_minimum_required(VERSION 2.8)

PROJECT(sst CXX)
set(CMAKE_CXX_FLAGS "-std=c++14 -Wall -ggdb -gdwarf-3")

include_directories(${derecho_SOURCE_DIR})

# count_read
add_executable(count_write count_write.cpp)
target_link_libraries(count_write sst rdmc)
