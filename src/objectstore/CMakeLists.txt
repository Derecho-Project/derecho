cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

# version
set(dpods_VERSION "0.9")
set(dpods_build_VERSION "0.9.1")

# compiler flags
set(CMAKE_CXX_FLAGS "-fPIC -std=c++1z -Wall")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -gdwarf-3 -D_DEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -ggdb -gdwarf-3 -D_DEBUG -D_PERFORMANCE_DEBUG")


if ( NOT DEFINED CMAKE_INSTALL_LIBDIR )
    set( CMAKE_INSTALL_LIBDIR lib )
endif ( )


# objectstore library
add_library(dpods SHARED ObjectStore.cpp Object.cpp)
target_include_directories(dpods PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_DIR}>
)
set_target_properties(dpods PROPERTIES
    SOVERSION ${dpods_VERSION}
    VERSION ${dpods_build_VERSION}
)
target_link_libraries(dpods derecho)
add_dependencies(dpods derecho)

# objectstore tester
add_executable(dpods_test test.cpp)
target_include_directories(dpods_test PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_DIR}>
)
target_link_libraries(dpods_test dpods)
add_dependencies(dpods_test dpods)

# objectstore performance tester
add_executable(dpods_perf performance.cpp)
target_include_directories(dpods_perf PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_DIR}>
)
target_link_libraries(dpods_perf dpods)
add_dependencies(dpods_perf dpods)

# make install
install(TARGETS dpods EXPORT dpods
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY
        ${CMAKE_SOURCE_DIR}/include/derecho/objectstore
        DESTINATION include/derecho)
install(FILES dpods-sample.cfg
        DESTINATION share/derecho/)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/dpods/dpodsConfigVersion.cmake"
    VERSION ${dpods_VERSION}
    COMPATIBILITY AnyNewerVersion
)

export (EXPORT dpods
    FILE "${CMAKE_CURRENT_BINARY_DIR}/dpods/dpodsTargets.cmake"
)

configure_file(dpodsConfig.cmake
    "${CMAKE_CURRENT_BINARY_DIR}/dpods/dpodsConfig.cmake"
    COPYONLY
)

set(ConfigPackageLocation lib/cmake/dpods)
install(EXPORT derecho
    FILE dpodsTargets.cmake
    DESTINATION ${ConfigPackageLocation}
)

install(FILES
    dpodsConfig.cmake
    "${CMAKE_CURRENT_BINARY_DIR}/dpods/dpodsConfigVersion.cmake"
    DESTINATION ${ConfigPackageLocation}
)

# Java support
find_package(Java QUIET)
find_package(JNI QUIET)
if(Java_FOUND AND JNI_FOUND)
    include_directories(${JAVA_INCLUDE_PATH})
    include_directories(${JAVA_INCLUDE_PATH2})
    include_directories(${CMAKE_SOURCE_DIR}/include)
    add_library(oss-jni SHARED java/com/derecho/objectstore/ObjectStoreService.cpp)
    target_link_libraries(oss-jni dpods)
    add_dependencies(oss-jni dpods)
    set_property(TARGET oss-jni PROPERTY POSITION_INDEPENDENT_CODE ON)

    include(UseJava)
    add_jar(oss java/com/derecho/objectstore/ObjectStoreService.java)
    add_jar(OSSTest java/com/derecho/objectstore/ObjectStoreService.java java/com/derecho/objectstore/OSSTest.java MANIFEST java/META-INF/Manifest.txt)
endif()
