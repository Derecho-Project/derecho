add_library(core OBJECT
    bytes_object.cpp
    connection_manager.cpp
    derecho_sst.cpp
    git_version.cpp
    multicast_group.cpp
    notification.cpp
    p2p_connection.cpp
    p2p_connection_manager.cpp
    persistence_manager.cpp
    restart_state.cpp
    rpc_manager.cpp
    rpc_utils.cpp
    subgroup_functions.cpp
    version_code.cpp
    view.cpp
    view_manager.cpp
)
target_include_directories(core PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)
# OBJECT libraries are not linked, but this command can be used to declare library dependencies
target_link_libraries(core mutils::mutils mutils::mutils-containers spdlog::spdlog)