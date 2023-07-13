cmake_minimum_required(VERSION 3.15.4)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(derecho CXX)

# Version
set(derecho_VERSION 2.3)
set(derecho_build_VERSION 2.3.0)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if (${USE_VERBS_API})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_VERBS_API")
endif()
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDERECHO_DEBUG -O0 -Wall -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall")
set(CMAKE_CXX_FLAGS_BENCHMARK "${CMAKE_CXX_FLAGS_RELEASE} -Wall -DNOLOG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -Wall -ggdb -gdwarf-3")

set(ignoreMe "${CMAKE_WARN_DEPRECATED}")

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules")

include(GNUInstallDirs)

# Prevent "make install" from automatically calling "make all", since the "all" target includes
# all the applications and they are not part of the library we want to install with "make install"
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

include(GetGitRevisionDescription)

# These packages export their location information in the "old" way,
# with INCLUDE_DIRS and LIBRARIES variables

# mutils_FOUND
# mutils_INCLUDE_DIRS
# mutils_LIBRARIES
find_package(mutils)
if (NOT ${mutils_FOUND})
    message(FATAL_ERROR "mutils not found. Please install from https://github.com/mpmilano/mutils.git")
else()
    message(STATUS "Found mutils in: ${mutils_INCLUDE_DIRS}")
    include_directories(${mutils_INCLUDE_DIRS})
endif()

# mutils-tasks_FOUND
# mutils-tasks_INCLUDE_DIRS
# mutils-tasks_LIBRARIES
find_package(mutils-tasks)
if (NOT ${mutils-tasks_FOUND})
    message(FATAL_ERROR "mutils not found. Please install from https://github.com/mpmilano/mutils-tasks.git")
else()
    message(STATUS "Found mutils-tasks in: ${mutils-tasks_INCLUDE_DIRS}")
    include_directories(${mutils-tasks_INCLUDE_DIRS})
endif()

# mutils-containers_FOUND
# mutils-containers_INCLUDE_DIRS
# mutils-containers_LIBRARIES
find_package(mutils-containers)
if (NOT ${mutils-containers_FOUND})
    message(FATAL_ERROR "mutils-containers not found. Please install from https://github.com/mpmilano/mutils-containers.git")
else()
    message(STATUS "Found mutils-containers in: ${mutils-containers_INCLUDE_DIRS}")
    include_directories(${mutils-containers_INCLUDE_DIRS})
endif()

# libfabric_FOUND
# libfabric_INCLUDE_DIRS
# libfabric_LIBRARIES
find_package(libfabric 1.12.1 REQUIRED)

# These packages export their location information in the "new" way,
# by providing an IMPORT-type CMake target that you can use as a
# dependency. Placing this target in target_link_libraries will
# automatically ensure its headers are included.

# Target: spdlog::spdlog
find_package(spdlog 1.3.1 REQUIRED)

# Target: OpenSSL::Crypto and OpenSSL::ssl
find_package(OpenSSL 1.1.1 REQUIRED)

# Target: nlohmann_json::nlohmann_json
find_package(nlohmann_json 3.9.0 REQUIRED)

# Target: Doxygen
find_package(Doxygen)

add_subdirectory(src/mutils-serialization)
add_subdirectory(src/conf)
add_subdirectory(src/utils)
add_subdirectory(src/core)
add_subdirectory(src/rdmc)
add_subdirectory(src/sst)
add_subdirectory(src/tcp)
add_subdirectory(src/persistent)
add_subdirectory(src/openssl)

# make libderecho.so
add_library(derecho SHARED
    $<TARGET_OBJECTS:mutils-serialization>
    $<TARGET_OBJECTS:conf>
    $<TARGET_OBJECTS:utils>
    $<TARGET_OBJECTS:core>
    $<TARGET_OBJECTS:rdmc>
    $<TARGET_OBJECTS:sst>
    $<TARGET_OBJECTS:tcp>
    $<TARGET_OBJECTS:persistent>
    $<TARGET_OBJECTS:openssl_wrapper>)
target_link_libraries(derecho
    rdmacm ibverbs rt pthread atomic stdc++fs
    spdlog::spdlog
    ${libfabric_LIBRARIES}
    ${mutils_LIBRARIES}
    ${mutils-containers_LIBRARIES}
    ${mutils-tasks_LIBRARIES}
    OpenSSL::Crypto
    nlohmann_json::nlohmann_json)
set_target_properties(derecho PROPERTIES
    SOVERSION ${derecho_VERSION}
    VERSION ${derecho_build_VERSION}
)
add_dependencies(derecho
    mutils-serialization conf utils core
    rdmc sst tcp persistent openssl_wrapper)

# Setup for make install
# Declare that we will install the targets built by "derecho"
install(TARGETS derecho EXPORT derechoTargets
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
# Declare that we will install the "include/" directory as a standard include directory
install(DIRECTORY
        include/
        TYPE INCLUDE)

# Use CMakePackageConfigHelpers to create a package version file and config file
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/derecho/derechoConfigVersion.cmake"
    VERSION ${derecho_VERSION}
    COMPATIBILITY AnyNewerVersion
)

set(ConfigPackageLocation lib/cmake/derecho)

configure_package_config_file(derechoConfig.cmake
    "${CMAKE_CURRENT_BINARY_DIR}/derecho/derechoConfig.cmake"
    INSTALL_DESTINATION ${ConfigPackageLocation}
    PATH_VARS CMAKE_INSTALL_LIBDIR CMAKE_INSTALL_INCLUDEDIR ConfigPackageLocation
)

# Create a CMake targets file for Derecho so it can be imported as a CMake package
install(EXPORT derechoTargets
    FILE derechoTargets.cmake
    NAMESPACE derecho::
    DESTINATION ${ConfigPackageLocation}
)

# Declare that we will install the cmake files that CmakePackageConfigHelpers just created
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/derecho/derechoConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/derecho/derechoConfigVersion.cmake"
    DESTINATION ${ConfigPackageLocation}
)

# Build applications
add_subdirectory(src/applications)

if (DOXYGEN_FOUND)
    # create docs
    doxygen_add_docs(docs
        ${CMAKE_CURRENT_SOURCE_DIR}
        ALL
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        CONFIG_FILE ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile
    )
endif()
