cmake_minimum_required (VERSION 3.1)
project (persistent)

# CXX FLAGS
set(CMAKE_CXX_FLAGS "-std=c++1z -Wall -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG}  -O0 -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

# TODO: we should have a better solution to this. git_version should be available across all modules other than only to "core"
git_describe(GIT_VERSION_STRING --tags --long)
string(REGEX MATCH "v([0-9]+)\\.([0-9]+)\\.?([0-9]*)-([0-9]+)-g([0-9|a-z]+)" MATCH_OUTPUT ${GIT_VERSION_STRING})
set(DERECHO_MAJOR_VERSION ${CMAKE_MATCH_1})
set(DERECHO_MINOR_VERSION ${CMAKE_MATCH_2})
set(DERECHO_PATCH_VERSION ${CMAKE_MATCH_3})
set(DERECHO_COMMITS_AHEAD ${CMAKE_MATCH_4})
set(DERECHO_COMMIT_HASH ${CMAKE_MATCH_5})
configure_file(../core/git_version.cpp.in git_version.cpp)

add_library(persistent OBJECT Persistent.cpp PersistLog.cpp PersistThreads.cpp FilePersistLog.cpp SPDKPersistLog.cpp SPDKPersistTool.cpp HLC.cpp)
target_include_directories(persistent PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)

add_executable(persistent_test test.cpp git_version.cpp
    $<TARGET_OBJECTS:persistent>
    $<TARGET_OBJECTS:conf>
    $<TARGET_OBJECTS:utils>
    $<TARGET_OBJECTS:mutils-serialization>
)
target_include_directories(persistent_test PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)
target_link_libraries(persistent_test pthread mutils stdc++fs spdk_nvme spdk_vmd spdk_env_dpdk spdk_log spdk_util spdk_sock dpdk uuid numa dl)

add_custom_target(format_persistent clang-format-3.8 -i *.cpp *.hpp)
