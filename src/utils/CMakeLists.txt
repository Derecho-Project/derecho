cmake_minimum_required (VERSION 3.1)
project (utils)

# C FLAGS
set(CMAKE_C_FLAGS "-Wall -fPIC -D_REENTRANT -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_C_FLAGS_RELEASE "-O3")
set(CMAKE_C_FLAGS_DEBUG   "-O0 -ggdb -gdwarf-3 -D_DEBUG -DSPDLOG_TRACE_ON")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "-O3 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

# CXX FLAGS
set(CMAKE_CXX_FLAGS "-std=c++1z -Wall -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_DEBUG   "-O0 -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

add_library(utils OBJECT logger.cpp)
target_include_directories(utils PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_custom_target(format_utils clang-format-3.8 -i *.cpp *.hpp)
