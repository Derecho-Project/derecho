cmake_minimum_required(VERSION 2.8)

SET(CMAKE_CXX_FLAGS "-std=c++1z -O3 -Wall -ggdb")
SET(CMAKE_CXX_FLAGS_DEBUG "-std=c++1z -Wall -ggdb -gdwarf-3")
SET(CMAKE_CXX_FLAGS_RELEASE "-std=c++1z -O3 -Wall")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-std=c++1z -Wall -O3 -ggdb -gdwarf-3")
include_directories(${derecho_SOURCE_DIR})

ADD_LIBRARY(tcp SHARED tcp.cpp)
TARGET_LINK_LIBRARIES(tcp rt pthread)
