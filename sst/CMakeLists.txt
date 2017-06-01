cmake_minimum_required(VERSION 2.8)

PROJECT(sst CXX)
set(CMAKE_CXX_FLAGS_DEBUG "-std=c++14 -Wall -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELEASE "-std=c++14 -Wall -O3")

include_directories(${derecho_SOURCE_DIR})

add_subdirectory(experiments)

ADD_LIBRARY(sst SHARED verbs.cpp poll_utils.cpp ../derecho/connection_manager.cpp ../rdmc/util.cpp)
TARGET_LINK_LIBRARIES(sst tcp rdmacm ibverbs pthread rt) 

add_custom_target(format_sst clang-format-3.8 -i *.cpp *.h)
