cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++1z -Wall -ggdb -gdwarf-3 -D_DEBUG -pg")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -Wall -O3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -Wall -O3 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

include_directories(${derecho_SOURCE_DIR})
include_directories(${derecho_SOURCE_DIR}/third_party)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)
include_directories(${derecho_SOURCE_DIR}/third_party/spdlog/include)
include_directories(${derecho_SOURCE_DIR}/third_party/libfabric/include)

link_directories(${derecho_SOURCE_DIR}/third_party/mutils)
link_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)
link_directories(${derecho_SOURCE_DIR}/third_party/libfabric/src/.libs)

add_executable(rpc_function_types rpc_function_types.cpp)
target_link_libraries(rpc_function_types derecho)

add_executable(rpc_reply_maps rpc_reply_maps.cpp)
target_link_libraries(rpc_reply_maps derecho)

# cooked_send_test
add_executable(cooked_send_test cooked_send_test.cpp)
target_link_libraries(cooked_send_test derecho)

# delivery_order_test
add_executable(delivery_order_test delivery_order_test.cpp)
target_link_libraries(delivery_order_test derecho)

add_executable(subgroup_function_tester subgroup_function_tester.cpp)
target_link_libraries(subgroup_function_tester derecho conf)
