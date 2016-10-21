cmake_minimum_required(VERSION 2.8)

PROJECT(sst CXX)
SET(CMAKE_CXX_FLAGS "-std=c++14 -O1 -g -Wall -Wno-unused-function -Wno-unused-variable -fno-omit-frame-pointer -Wno-unused-but-set-variable -Wno-unused-result")

include_directories(${derecho_SOURCE_DIR})

ADD_LIBRARY(sst SHARED verbs.cpp)
TARGET_LINK_LIBRARIES(sst rdmacm ibverbs pthread rt) 

add_custom_target(format_sst clang-format-3.6 -i *.cpp *.h)
