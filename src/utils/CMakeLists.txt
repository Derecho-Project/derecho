cmake_minimum_required (VERSION 3.1)
project (utils)

# C FLAGS
set(CMAKE_C_FLAGS_RELEASE "-std=c++1z ${CMAKE_C_FLAGS} -Wall -O3 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_C_FLAGS_DEBUG   "-std=c++1z ${CMAKE_C_FLAGS} -Wall -O0 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -ggdb -gdwarf-3 -D_DEBUG -DSPDLOG_TRACE_ON")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "-std=c++1z ${CMAKE_C_FLAGS} -Wall -O3 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

# CXX FLAGS
set(CMAKE_CXX_FLAGS_RELEASE "-std=c++1z ${CMAKE_CXX_FLAGS} -Wall -O3 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_DEBUG   "-std=c++1z ${CMAKE_CXX_FLAGS} -Wall -O0 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_DEBUG -DSPDLOG_TRACE_ON -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-std=c++1z ${CMAKE_CXX_FLAGS} -Wall -O3 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

include_directories(${derecho_SOURCE_DIR})
include_directories(${derecho_SOURCE_DIR}/third_party/spdlog/include)

add_library(utils SHARED logger.cpp)
target_link_libraries(utils conf)
add_dependencies(utils conf)

add_custom_target(format_utils
    COMMAND clang-format-3.8 -i *.cpp *.hpp
    WORKING_DIRECTORY ${derecho_SOURCE_DIR}/utils
)
