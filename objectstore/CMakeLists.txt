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

# objectstore library
add_library(objectstore SHARED ObjectStore.cpp)
target_link_libraries(objectstore derecho)
add_dependencies(objectstore derecho)

# objectstore tester
add_executable(objectstore_test test.cpp)
target_link_libraries(objectstore_test objectstore)
add_dependencies(objectstore_test objectstore)

# objectstore performance tester
add_executable(objectstore_performance performance.cpp)
target_link_libraries(objectstore_performance objectstore)
add_dependencies(objectstore_performance objectstore)

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
