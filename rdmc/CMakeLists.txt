cmake_minimum_required(VERSION 2.8)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
PROJECT(verbs-mcast CXX)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++1z -O0 -Wall -ggdb -gdwarf-3 -pg")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -O3 -Wall")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z -O3 -Wall -ggdb -gdwarf-3")

include_directories(${derecho_SOURCE_DIR})
include_directories(${derecho_SOURCE_DIR}/third_party/libfabric/build/include)
include_directories(${derecho_SOURCE_DIR}/third_party/spdlog/include)
include_directories(${derecho_SOURCE_DIR}/third_party/getpot)
link_directories(${derecho_SOURCE_DIR}/third_party/libfabric/build/lib)

ADD_LIBRARY(rdmc SHARED rdmc.cpp util.cpp group_send.cpp schedule.cpp lf_helper.cpp)
TARGET_LINK_LIBRARIES(rdmc tcp rdmacm ibverbs rt pthread fabric)

find_library(SLURM_FOUND slurm)
if (SLURM_FOUND)
  target_link_libraries(rdmc slurm)
  add_definitions(-DUSE_SLURM)
endif (SLURM_FOUND)

#ADD_EXECUTABLE(verbs-mcast experiment.cpp)
#TARGET_LINK_LIBRARIES(verbs-mcast rdmc)
add_dependencies(rdmc libfabric_target)

add_custom_target(format_rdmc clang-format-3.8 -i *.cpp *.h)
