cmake_minimum_required(VERSION 2.8.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

set(CMAKE_CXX_FLAGS "-std=c++1z -Wall -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

git_describe(GIT_VERSION_STRING --tags --long)
string(REGEX MATCH "v([0-9]+)\\.([0-9]+)\\.?([0-9]*)-([0-9]+)-g([0-9|a-z]+)" MATCH_OUTPUT ${GIT_VERSION_STRING})
set(DERECHO_MAJOR_VERSION ${CMAKE_MATCH_1})
set(DERECHO_MINOR_VERSION ${CMAKE_MATCH_2})
set(DERECHO_PATCH_VERSION ${CMAKE_MATCH_3})
set(DERECHO_COMMITS_AHEAD ${CMAKE_MATCH_4})
set(DERECHO_COMMIT_HASH ${CMAKE_MATCH_5})
configure_file(git_version.cpp.in git_version.cpp) 

add_library(core OBJECT derecho_sst.cpp view.cpp view_manager.cpp rpc_manager.cpp p2p_connections.cpp multicast_group.cpp subgroup_functions.cpp connection_manager.cpp restart_state.cpp persistence_manager.cpp version_code.cpp ${CMAKE_CURRENT_BINARY_DIR}/git_version.cpp)
target_include_directories(core PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_custom_target(format_derecho clang-format-6.0 -i *.cpp *.h)
