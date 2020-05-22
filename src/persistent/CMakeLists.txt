cmake_minimum_required (VERSION 3.1)
project (persistent)

# CXX FLAGS
set(CMAKE_CXX_FLAGS "-std=c++1z -Wall -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG}  -O0 -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

add_library(persistent OBJECT Persistent.cpp PersistLog.cpp PersistThreads.cpp FilePersistLog.cpp SPDKPersistLog.cpp SPDKPersistTool.cpp HLC.cpp)
target_include_directories(persistent PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${OPENSSL_INCLUDE_DIR}>
)

add_executable(persistent_test test_spdk.cpp ${CMAKE_SOURCE_DIR}/src/core/git_version.cpp
    $<TARGET_OBJECTS:persistent>
    $<TARGET_OBJECTS:conf>
    $<TARGET_OBJECTS:utils>
    $<TARGET_OBJECTS:mutils-serialization>
)
target_include_directories(persistent_test PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${OPENSSL_INCLUDE_DIR}>
)
target_link_libraries(persistent_test pthread mutils stdc++fs spdk_nvme spdk_vmd spdk_env_dpdk spdk_log spdk_util spdk_sock dpdk uuid numa dl ${OPENSSL_LIBRARIES})

add_custom_target(format_persistent clang-format-3.8 -i *.cpp *.hpp)
