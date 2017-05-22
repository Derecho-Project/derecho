cmake_minimum_required(VERSION 2.8)

PROJECT(sst CXX)
SET(CMAKE_CXX_FLAGS "-std=c++14 -O3 -g -Wall -Wno-unused-function -Wno-unused-variable -fno-omit-frame-pointer -Wno-unused-but-set-variable -Wno-unused-result")

add_subdirectory(experiments)

include_directories(${derecho_SOURCE_DIR})

ADD_LIBRARY(sst SHARED verbs.cpp poll_utils.cpp ../derecho/connection_manager.cpp)
TARGET_LINK_LIBRARIES(sst rdmacm ibverbs pthread rt) 

add_custom_target(format_sst clang-format-3.8 -i *.cpp *.h)
