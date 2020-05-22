cmake_minimum_required(VERSION 2.8.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_library(core OBJECT derecho_sst.cpp view.cpp view_manager.cpp rpc_manager.cpp p2p_connection.cpp p2p_connection_manager.cpp multicast_group.cpp subgroup_functions.cpp connection_manager.cpp restart_state.cpp persistence_manager.cpp version_code.cpp git_version.cpp)
target_include_directories(core PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_custom_target(format_derecho clang-format-6.0 -i *.cpp *.h)
