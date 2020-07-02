cmake_minimum_required (VERSION 3.0)
project(derecho_jni)

find_package(Java REQUIRED)
find_package(JNI REQUIRED)
include(UseJava)

set(CMAKE_JAVA_COMPILE_FLAGS "-source" "11" "-target" "11" "-h" "${CMAKE_CURRENT_SOURCE_DIR}/cpp")
set(GOOGLE_JAVA_FORMAT_JAR "${CMAKE_CURRENT_SOURCE_DIR}/external/google-java-format-1.7-all-deps.jar")

set(CMAKE_CXX_FLAGS "-std=c++1z -Wall -g -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -ggdb")
set (CMAKE_SHARED_LINKER_FLAGS "${derecho_LIBRARIES}")

add_jar(derechojar
        java/io/derecho/Group.java
        java/io/derecho/Replicated.java
        java/io/derecho/View.java
        java/io/derecho/IShardViewGenerator.java
        java/io/derecho/QueryResults.java
        java/io/derecho/SubgroupObjectRegistry.java
        java/io/derecho/IMessageGenerator.java
    OUTPUT_NAME
        derecho)

add_jar(derechoBWjar
        java/io/derecho/test/BandwidthTest.java
    ENTRY_POINT
	io/derecho/test/BandwidthTest
    OUTPUT_NAME
        bandwidth_test
    MANIFEST
    	Manifest.txt
    INCLUDE_JARS
        derechojar
)

add_jar(derechoLjar
        java/io/derecho/test/LatencyTest.java
    ENTRY_POINT
	io/derecho/test/LatencyTest
    OUTPUT_NAME
        latency_test
    MANIFEST
    	Manifest.txt
    INCLUDE_JARS
        derechojar
)

add_jar(derechoTSjar
	    java/io/derecho/test/TypedSubgroupTest.java
    ENTRY_POINT
	io/derecho/test/TypedSubgroupTest
    OUTPUT_NAME
        typed_subgroup_test
    MANIFEST
    	Manifest.txt
    INCLUDE_JARS
        derechojar
)

add_jar(derechoTSBWjar
	    java/io/derecho/test/TypedSubgroupBWTest.java
    ENTRY_POINT
	io/derecho/test/TypedSubgroupBWTest
    OUTPUT_NAME
        typed_subgroup_bw_test
    MANIFEST
    	Manifest.txt 
    INCLUDE_JARS
        derechojar
)

include_directories(${JAVA_INCLUDE_PATH}
                    ${JAVA_INCLUDE_PATH2}
                    ${CMAKE_SOURCE_DIR}/include)
add_library(derechojni SHARED ${CMAKE_CURRENT_SOURCE_DIR}/cpp/derecho_jni.cpp)
target_link_libraries(derechojni derecho)
add_dependencies(derechojni derecho)
