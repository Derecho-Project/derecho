cmake_minimum_required(VERSION 2.8.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(derecho CXX)
set(CMAKE_CXX_FLAGS_DEBUG "-std=c++14 -Wall -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELEASE "-std=c++14 -Wall -O3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-std=c++14 -Wall -O3 -ggdb -gdwarf-3")

add_subdirectory(derecho)
add_subdirectory(rdmc)
add_subdirectory(sst)
add_subdirectory(tcp)
add_subdirectory(persistent)

add_custom_target(
	mutils_target
	COMMAND $(MAKE)
	WORKING_DIRECTORY ${derecho_SOURCE_DIR}/third_party/mutils
)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils)
link_directories(${derecho_SOURCE_DIR}/third_party/mutils)
add_custom_target(
	mutils_serialization_target
	COMMAND CPLUS_INCLUDE_PATH=${derecho_SOURCE_DIR}/third_party/mutils $(MAKE)
	WORKING_DIRECTORY ${derecho_SOURCE_DIR}/third_party/mutils-serialization
)
include_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)
link_directories(${derecho_SOURCE_DIR}/third_party/mutils-serialization)

include_directories(${derecho_SOURCE_DIR}/third_party/mutils-containers)

include_directories(${derecho_SOURCE_DIR}/third_party/spdlog/include)

include_directories(${derecho_SOURCE_DIR}/third_party)
