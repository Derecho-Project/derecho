cmake_minimum_required(VERSION 2.8.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(derecho CXX)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++1z -O0 -Wall -ggdb -gdwarf-3 -pg")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -O3 -Wall -DNOLOG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -O3 -Wall -ggdb -gdwarf-3 -DNOLOG")

add_subdirectory(conf)
add_subdirectory(utils)
add_subdirectory(derecho)
add_subdirectory(rdmc)
add_subdirectory(sst)
add_subdirectory(tcp)
add_subdirectory(persistent)
add_subdirectory(objectstore)
add_subdirectory(applications)

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

add_custom_target(
	libfabric_target
#	COMMAND { if [ ! -e Makefile ]\; then libtoolize && ./autogen.sh && ./configure CFLAGS='-g' --disable-mlx --enable-debug --prefix=/ \; fi } && make && make DESTDIR=${derecho_SOURCE_DIR}/third_party/libfabric/build install
	COMMAND { if [ ! -e src/.libs/libfabric.so ]\; then libtoolize && ./autogen.sh && ./configure CFLAGS='-g' --disable-mlx --enable-debug --prefix=/ \; fi } && make
	WORKING_DIRECTORY ${derecho_SOURCE_DIR}/third_party/libfabric
)
# include_directories(${derecho_SOURCE_DIR}/third_party/libfabric/build/include)
include_directories(${derecho_SOURCE_DIR}/third_party/libfabric/include)
# link_directories(${derecho_SOURCE_DIR}/third_party/libfabric/build/lib)
link_directories(${derecho_SOURCE_DIR}/third_party/libfabric/src/.libs)

include_directories(${derecho_SOURCE_DIR}/third_party)
