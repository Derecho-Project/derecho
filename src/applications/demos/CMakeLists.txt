cmake_minimum_required(VERSION 3.15.4)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_BINARY_DIR}/include)

# random_messages
add_executable(random_messages random_messages.cpp)
target_link_libraries(random_messages derecho)

# simple_replicated_objects
add_executable(simple_replicated_objects simple_replicated_objects.cpp)
target_link_libraries(simple_replicated_objects derecho)

add_executable(simple_replicated_objects_json simple_replicated_objects_json.cpp)
target_link_libraries(simple_replicated_objects_json derecho)

# overlapping replicated objects
add_executable(simple_replicated_objects_overlap simple_replicated_objects_overlap.cpp)
target_link_libraries(simple_replicated_objects_overlap derecho)

add_executable(signed_store_mockup signed_store_mockup.cpp)
target_link_libraries(signed_store_mockup derecho)

add_executable(oob_rdma oob_rdma.cpp)
target_link_libraries(oob_rdma derecho)
add_custom_command(TARGET oob_rdma POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/oob_rdma_cfgs
        ${CMAKE_CURRENT_BINARY_DIR}/oob_rdma_cfgs
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/oob_rdma_cfgs/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/oob_rdma_cfgs/n0/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/oob_rdma_cfgs/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/oob_rdma_cfgs/n1/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/oob_rdma_cfgs/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/oob_rdma_cfgs/n2/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/oob_rdma_cfgs/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/oob_rdma_cfgs/n3/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/oob_rdma_cfgs/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/oob_rdma_cfgs/n4/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/oob_rdma_cfgs/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/oob_rdma_cfgs/n5/layout.json
)
