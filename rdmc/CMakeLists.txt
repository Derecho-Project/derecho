cmake_minimum_required(VERSION 2.8)

PROJECT(verbs-mcast CXX)
SET(CMAKE_CXX_FLAGS "-std=c++14 -O3 -Wall -ggdb -gdwarf-3")

ADD_LIBRARY(rdmc SHARED rdmc.cpp util.cpp group_send.cpp verbs_helper.cpp connection.cpp schedule.cpp)
TARGET_LINK_LIBRARIES(rdmc rdmacm ibverbs rt pthread)

find_library(SLURM_FOUND slurm)
if (SLURM_FOUND)
  target_link_libraries(rdmc slurm)
  add_definitions(-DUSE_SLURM)
endif (SLURM_FOUND)

ADD_EXECUTABLE(verbs-mcast experiment.cpp)
TARGET_LINK_LIBRARIES(verbs-mcast rdmc)

add_custom_target(format_rdmc clang-format-3.6 -i *.cpp *.h)
