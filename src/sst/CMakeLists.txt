cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
PROJECT(sst CXX)

# ADD_LIBRARY(sst SHARED verbs.cpp lf.cpp poll_utils.cpp ../derecho/connection_manager.cpp)
if (${USE_VERBS_API})
    ADD_LIBRARY(sst OBJECT verbs.cpp poll_utils.cpp)
else()
    ADD_LIBRARY(sst OBJECT lf.cpp poll_utils.cpp)
endif()
target_include_directories(sst PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)

add_custom_target(format_sst clang-format-3.8 -i *.cpp *.hpp)
