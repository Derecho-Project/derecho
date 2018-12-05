cmake_minimum_required (VERSION 3.1)
project (Persistent)

# C FLAGS
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -Wall -O3 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_C_FLAGS_DEBUG   "${CMAKE_C_FLAGS_DEBUG} -Wall -O0 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -ggdb -gdwarf-3 -D_DEBUG -DSPDLOG_TRACE_ON")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELEASE} -Wall -O3 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

# CXX FLAGS
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -Wall -O3 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG} -std=c++1z -Wall -O0 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_DEBUG -DSPDLOG_TRACE_ON -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -Wall -O3 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

# HELPERS
function(output_directory TGT DIR)
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
endfunction(output_directory TGT DIR)

# Warning: This is different from the way all other Derecho components include 
# the third_party directory, and makes include statements work differently
include_directories(
  ${derecho_SOURCE_DIR}
  ${derecho_SOURCE_DIR}/third_party/mutils
  ${derecho_SOURCE_DIR}/third_party/mutils-serialization
  ${derecho_SOURCE_DIR}/third_party/libfabric/include
  ${derecho_SOURCE_DIR}/third_party/spdlog/include)
link_directories(
  ${derecho_SOURCE_DIR}/third_party/mutils 
  ${derecho_SOURCE_DIR}/third_party/mutils-serialization)

add_library(persistent SHARED Persistent.hpp Persistent.cpp PersistLog.cpp PersistLog.hpp FilePersistLog.cpp FilePersistLog.hpp HLC.cpp HLC.hpp PersistNoLog.hpp)
target_link_libraries(persistent stdc++fs)
output_directory(persistent target/usr/local/lib)
add_dependencies(persistent libfabric_target)

add_executable(ptst test.cpp)
target_link_libraries(ptst conf persistent pthread mutils mutils-serialization)

add_custom_target(format_persistent
    COMMAND clang-format-3.8 -i *.cpp *.hpp
    WORKING_DIRECTORY ${derecho_SOURCE_DIR}/persistent
)
