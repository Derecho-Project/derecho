cmake_minimum_required(VERSION 2.8)

SET(CMAKE_CXX_FLAGS "-std=c++1z -Wall -fPIC -D_REENTRANT")
SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -O0 -ggdb -gdwarf-3 -pg")
SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS} -O3 -ggdb -gdwarf-3")

ADD_LIBRARY(tcp OBJECT tcp.cpp)
target_include_directories(tcp PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include/private>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_custom_target(format_tcp clang-format-3.8 -i *.cpp *.hpp)
