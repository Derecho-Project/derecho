cmake_minimum_required (VERSION 3.1)
project (Persistent)

# C FLAGS
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS} -Wall -O3 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_C_FLAGS_DEBUG   "${CMAKE_C_FLAGS} -Wall -O2 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -ggdb -gdwarf-3 -D_DEBUG -DSPDLOG_TRACE_ON")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS} -Wall -O3 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

# CXX FLAGS
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -Wall -O3 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS} -Wall -O2 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_DEBUG -DSPDLOG_TRACE_ON -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS} -Wall -O3 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

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

include_directories(
  ${derecho_SOURCE_DIR}/third_party/mutils
  ${derecho_SOURCE_DIR}/third_party/mutils-serialization
  ${derecho_SOURCE_DIR}/third_party/libfabric/build/include
  ${derecho_SOURCE_DIR}/third_party/spdlog/include)
link_directories(
  ${derecho_SOURCE_DIR}/third_party/mutils 
  ${derecho_SOURCE_DIR}/third_party/mutils-serialization)

# add_library(persistent Persistent.hpp PersistLog.cpp PersistLog.hpp FilePersistLog.cpp FilePersistLog.hpp MemLog.cpp MemLog.hpp)
add_library(persistent SHARED Persistent.hpp PersistLog.cpp PersistLog.hpp FilePersistLog.cpp FilePersistLog.hpp HLC.cpp HLC.hpp)
output_directory(persistent target/usr/local/lib)
add_dependencies(persistent libfabric_target)

add_executable(ptst test.cpp)
target_link_libraries(ptst persistent pthread mutils mutils-serialization)
