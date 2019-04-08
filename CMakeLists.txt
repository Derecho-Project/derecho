cmake_minimum_required(VERSION 2.8.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(derecho CXX)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -O0 -Wall -ggdb -gdwarf-3 -pg")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -O3 -Wall -DNOLOG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS} -O3 -Wall -ggdb -gdwarf-3 -DNOLOG")

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules")

# mutils_FOUND 
# mutils_INCLUDE_DIRS
# mutils_LIBRARIES
find_package(mutils)
if (NOT ${mutils_FOUND})
    message(FATAL_ERROR "mutils not found. Please install from https://github.com/mpmilano/mutils.git")
endif()

# mutils-tasks_FOUND
# mutils-tasks_INCLUDE_DIRS
# mutils-tasks_LIBRARIES
find_package(mutils-tasks)
if (NOT ${mutils-tasks_FOUND})
    message(FATAL_ERROR "mutils not found. Please install from https://github.com/mpmilano/mutils-tasks.git")
endif()

# mutils-containers_FOUND
# mutils-containers_INCLUDE_DIRS
# mutils-containers_LIBRARIES
find_package(mutils-containers)
if (NOT ${mutils-containers_FOUND})
    message(FATAL_ERROR "mutils-containers not found. Please install from https://github.com/mpmilano/mutils-containers.git")
endif()

# spdlog_FOUND
find_package(spdlog)
if (NOT ${spdlog_FOUND})
    message(FATAL_ERROR "spdlog not found. Please install from https://github.com/gabime/spdlog [commit:10e809cf644d55e5bd7d66d02e2604e2ddd7fb48]")
endif()

# libfabric_FOUND
# libfabric_INCLUDE_DIRS
# libfabric_LIBRARIES
find_package(libfabric)
if (NOT ${libfabric_FOUND})
    message(FATAL_ERROR "libfabric not found. Please install from https://github.com/ofiwg/libfabric.git. [commit:fcf0f2ec3c7109e06e09d3650564df8d2dfa12b6]")
endif()

add_subdirectory(src/mutils-serialization)
add_subdirectory(src/conf)
add_subdirectory(src/utils)
add_subdirectory(src/core)
add_subdirectory(src/rdmc)
add_subdirectory(src/sst)
add_subdirectory(src/tcp)
add_subdirectory(src/persistent)

# make libderecho.so
add_library(derecho SHARED
    $<TARGET_OBJECTS:mutils-serialization> 
    $<TARGET_OBJECTS:conf>
    $<TARGET_OBJECTS:utils>
    $<TARGET_OBJECTS:core>
    $<TARGET_OBJECTS:rdmc>
    $<TARGET_OBJECTS:sst>
    $<TARGET_OBJECTS:tcp>
    $<TARGET_OBJECTS:persistent>)
target_link_libraries(derecho
    rdmacm ibverbs rt pthread atomic
    stdc++fs ${libfabric_LIBRARIES} 
    ${mutils_LIBRARIES} 
    ${mutils-containers_LIBRARIES}
    ${mutils-tasks_LIBRARIES})
add_dependencies(derecho
    mutils-serialization conf utils core
    rdmc sst tcp persistent)

# add_subdirectory(src/objectstore)
# add_subdirectory(src/applications)

