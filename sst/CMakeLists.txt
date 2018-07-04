cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
PROJECT(sst CXX)
set(CMAKE_CXX_FLAGS_DEBUG "-std=c++14 -Wall -ggdb -gdwarf-3 -pg")
set(CMAKE_CXX_FLAGS_RELEASE "-std=c++14 -Wall -O3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-std=c++14 -Wall -O3 -ggdb -gdwarf-3")

include_directories(${derecho_SOURCE_DIR})
include_directories(${derecho_SOURCE_DIR}/third_party/spdlog/include)

add_subdirectory(experiments)

ADD_LIBRARY(sst SHARED verbs.cpp poll_utils.cpp ../derecho/connection_manager.cpp)
TARGET_LINK_LIBRARIES(sst tcp rdmacm ibverbs pthread rt) 

add_custom_target(format_sst clang-format-3.8 -i *.cpp *.h)
