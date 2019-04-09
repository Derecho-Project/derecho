cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

set(CMAKE_CXX_FLAGS "-fPIC -std=c++1z -Wall")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -ggdb -gdwarf-3 -D_DEBUG -pg")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -ggdb -gdwarf-3 -D_DEBUG -D_PERFORMANCE_DEBUG")

# objectstore library
add_library(dpods SHARED ObjectStore.cpp)
target_include_directories(dpods PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_DIR}>
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

# Java support
find_package(Java QUIET)
find_package(JNI QUIET)
if(Java_FOUND AND JNI_FOUND)
    include_directories(${JAVA_INCLUDE_PATH})
    include_directories(${JAVA_INCLUDE_PATH2})
    add_library(oss-jni SHARED java/com/derecho/objectstore/ObjectStoreService.cpp)
    target_link_libraries(oss-jni objectstore)
    add_dependencies(oss-jni objectstore)
    set_property(TARGET oss-jni PROPERTY POSITION_INDEPENDENT_CODE ON)

    include(UseJava)
    add_jar(oss java/com/derecho/objectstore/ObjectStoreService.java)
    add_jar(OSSTest java/com/derecho/objectstore/ObjectStoreService.java java/com/derecho/objectstore/OSSTest.java MANIFEST java/META-INF/Manifest.txt)
endif()
