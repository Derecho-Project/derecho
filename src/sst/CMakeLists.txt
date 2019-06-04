cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
PROJECT(sst CXX)

set(CMAKE_CXX_FLAGS "-std=c++1z -fPIC -Wall")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -Wall -ggdb -gdwarf-3")

# ADD_LIBRARY(sst SHARED verbs.cpp lf.cpp poll_utils.cpp ../derecho/connection_manager.cpp)
ADD_LIBRARY(sst OBJECT lf.cpp poll_utils.cpp)
target_include_directories(sst PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)

add_custom_target(format_sst clang-format-3.8 -i *.cpp *.hpp)
