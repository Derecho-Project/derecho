cmake_minimum_required (VERSION 3.1)
project (conf)

# CXX FLAGS
set(CMAKE_CXX_FLAGS "-std=c++1z -Wall -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG} -O0 -D_DEBUG -DSPDLOG_TRACE_ON -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3 -ggdb -gdwarf-3 -D_PERFORMANCE_DEBUG")

add_library(conf OBJECT conf.cpp)
target_include_directories(conf PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)

add_executable(conftst test.cpp conf.cpp)
target_include_directories(conftst PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)
target_link_libraries(conftst pthread)

install(FILES derecho-sample.cfg
    DESTINATION share/derecho/)

add_custom_target(format_conf clang-format-3.8 -i *.cpp *.hpp)
