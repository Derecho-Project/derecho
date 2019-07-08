cmake_minimum_required(VERSION 2.8.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(derecho CXX)

# Version
set(derecho_VERSION 0.9)
set(derecho_build_VERSION 0.9.1)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -Wall -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall -DNOLOG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -Wall -ggdb -gdwarf-3 -DNOLOG")

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules")

if ( NOT DEFINED CMAKE_INSTALL_LIBDIR )
    set( CMAKE_INSTALL_LIBDIR lib )
endif ( )

include(GetGitRevisionDescription)

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
set_target_properties(derecho PROPERTIES
    SOVERSION ${derecho_VERSION}
    VERSION ${derecho_build_VERSION}
)
add_dependencies(derecho
    mutils-serialization conf utils core
    rdmc sst tcp persistent)

# make install
install(TARGETS derecho EXPORT derecho
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY
        include/derecho/conf
        include/derecho/core
        include/derecho/mutils-serialization
        include/derecho/persistent
        include/derecho/rdmc
        include/derecho/sst
        include/derecho/tcp
        include/derecho/utils
        DESTINATION include/derecho)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/derecho/derechoConfigVersion.cmake"
    VERSION ${derecho_VERSION}
    COMPATIBILITY AnyNewerVersion
)

export (EXPORT derecho
    FILE "${CMAKE_CURRENT_BINARY_DIR}/derecho/derechoTargets.cmake"
)

configure_file (derechoConfig.cmake
    "${CMAKE_CURRENT_BINARY_DIR}/derecho/derechoConfig.cmake"
    COPYONLY
)

set(ConfigPackageLocation lib/cmake/derecho)
install(EXPORT derecho
    FILE derechoTargets.cmake
    DESTINATION ${ConfigPackageLocation}
)

install(FILES
    derechoConfig.cmake
    "${CMAKE_CURRENT_BINARY_DIR}/derecho/derechoConfigVersion.cmake"
    DESTINATION ${ConfigPackageLocation}
)

# objectstore aka dPods
add_subdirectory(src/objectstore)

# applications
add_subdirectory(src/applications)
