cmake_minimum_required(VERSION 2.8)

SET(CMAKE_CXX_FLAGS "-std=c++14 -O3 -Wall -ggdb")

include_directories(${derecho_SOURCE_DIR})

ADD_LIBRARY(tcp SHARED tcp.cpp)
TARGET_LINK_LIBRARIES(tcp rt pthread)
