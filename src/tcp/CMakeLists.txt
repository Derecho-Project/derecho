cmake_minimum_required(VERSION 2.8)

ADD_LIBRARY(tcp OBJECT tcp.cpp)
target_include_directories(tcp PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_custom_target(format_tcp clang-format-3.8 -i *.cpp *.hpp)
