cmake_minimum_required (VERSION 3.1)
project (Persistent)

# C FLAGS
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -O2 -fPIC -D_DEBUG")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_REENTRANT -D_GNU_SOURCE -DSPDLOG_TRACE_ON")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")

# CXX FLAGS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall -O3 -fPIC -D_DEBUG")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_REENTRANT -D_GNU_SOURCE -DSPDLOG_TRACE_ON")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")

# set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD 14)

# HELPERS
function(output_directory TGT DIR)
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
endfunction(output_directory TGT DIR)

include_directories(../mutils ../mutils-serialization ../spdlog/include)
link_directories(../mutils ../mutils-serialization)

# add_library(persistent Persistent.hpp PersistLog.cpp PersistLog.hpp FilePersistLog.cpp FilePersistLog.hpp MemLog.cpp MemLog.hpp)
add_library(persistent SHARED Persistent.hpp PersistLog.cpp PersistLog.hpp FilePersistLog.cpp FilePersistLog.hpp HLC.cpp HLC.hpp)
output_directory(persistent target/usr/local/lib)

add_executable(ptst test.cpp)
target_link_libraries(ptst persistent pthread mutils mutils-serialization)
