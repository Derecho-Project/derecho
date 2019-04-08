cmake_minimum_required (VERSION 3.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project (mutils-serialization)

# C FLAGS
set(CMAKE_C_FLAGS "-std=c++1z -Wall -fPIC -D_REENTRANT -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS} -O3")
set(CMAKE_C_FLAGS_DEBUG   "${CMAKE_C_FLAGS} -O0 -ggdb -gdwarf-3 -D_DEBUG -DSPDLOG_TRACE_ON")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS} -O3 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

# CXX FLAGS
set(CMAKE_CXX_FLAGS "-std=c++1z -Wall -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS} -O0 -ggdb -gdwarf-3 -D_DEBUG -DSPDLOG_TRACE_ON")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS} -O3 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

add_library(mutils-serialization OBJECT SerializationSupport.cpp)
target_include_directories(mutils-serialization PRIVATE 
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include/private>
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)

add_custom_target(format_mutils-serialization clang-format-3.8 -i *.cpp *.hpp)
