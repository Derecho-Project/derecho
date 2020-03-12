cmake_minimum_required (VERSION 3.1)
project (utils)

add_library(utils OBJECT logger.cpp)
target_include_directories(utils PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_custom_target(format_utils clang-format-3.8 -i *.cpp *.hpp)
