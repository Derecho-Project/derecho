add_library(conf OBJECT conf.cpp)
target_include_directories(conf PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
)
target_link_libraries(conf PUBLIC nlohmann_json::nlohmann_json)
if (${ENABLE_MEMBER_REGISTRY})
    target_link_libraries(conf PUBLIC rpclib::rpc)
endif()


if (${ENABLE_MEMBER_REGISTRY})
    add_executable(member_registry member_registry.cpp)
    target_include_directories(member_registry PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    )
    target_link_libraries(member_registry rpclib::rpc derecho pthread)
    add_dependencies(member_registry derecho)

    add_executable(member_registry_client member_registry_client.cpp)
    target_include_directories(member_registry_client PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    )
    target_link_libraries(member_registry_client rpclib::rpc derecho pthread)
    add_dependencies(member_registry_client derecho)
endif()

add_executable(conftst test.cpp conf.cpp)
target_include_directories(conftst PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
)
target_link_libraries(conftst pthread nlohmann_json::nlohmann_json)
if (${ENABLE_MEMBER_REGISTRY})
target_link_libraries(conftst rpclib::rpc)
endif()

# installation
install(FILES derecho-sample.cfg
    DESTINATION share/derecho/)
if (${ENABLE_MEMBER_REGISTRY})
install(TARGETS member_registry member_registry_client
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()
