cmake_minimum_required(VERSION 3.5)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(sospdemo CXX)

set(CMAKE_CXX_FLAGS "-fPIC -std=c++1z -Wall -DPROFILING")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -ggdb")
add_subdirectory(src)
