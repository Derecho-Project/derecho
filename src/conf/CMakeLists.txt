add_library(conf OBJECT conf.cpp)
target_include_directories(conf PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
)
target_link_libraries(conf nlohmann_json::nlohmann_json)


if (${ENABLE_LEADER_REGISTRY})
    add_executable(leader_registry leader_registry.cpp)
    target_include_directories(leader_registry PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    )
    target_link_libraries(leader_registry rpclib::rpc pthread)

    add_executable(leader_registry_client leader_registry_client.cpp)
    target_include_directories(leader_registry_client PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    )
    target_link_libraries(leader_registry_client rpclib::rpc pthread)
endif()

add_executable(conftst test.cpp conf.cpp)
target_include_directories(conftst PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
)
target_link_libraries(conftst pthread nlohmann_json::nlohmann_json)

# installation
install(FILES derecho-sample.cfg
    DESTINATION share/derecho/)
install(TARGETS leader_registry leader_registry_client
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
