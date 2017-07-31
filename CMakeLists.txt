cmake_minimum_required(VERSION 2.8.1)

project(derecho CXX)
set(CMAKE_CXX_FLAGS "-std=c++14 -Wall -ggdb -gdwarf-3")

add_subdirectory(derecho)
add_subdirectory(rdmc)
add_subdirectory(sst)
add_subdirectory(tcp)
add_subdirectory(third_party/persistent)

add_custom_target(
	mutils_target
	COMMAND make
	WORKING_DIRECTORY ${derecho_SOURCE_DIR}/third_party/mutils
)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils)
link_directories(${derecho_SOURCE_DIR}/third_party/mutils)
add_custom_target(
	mutils_serialization_target
	COMMAND CPLUS_INCLUDE_PATH=${derecho_SOURCE_DIR}/third_party/mutils make
	WORKING_DIRECTORY ${derecho_SOURCE_DIR}/third_party/mutils-serialization
)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)
link_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)

include_directories(${derecho_SOURCE_DIR}/third_party/mutils-containers)

include_directories(${derecho_SOURCE_DIR}/third_party/spdlog/include)

include_directories(${derecho_SOURCE_DIR}/third_party)
