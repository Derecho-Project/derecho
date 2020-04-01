cmake_minimum_required (VERSION 3.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project (mutils-serialization)

add_library(mutils-serialization OBJECT SerializationSupport.cpp)
target_include_directories(mutils-serialization PRIVATE 
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)

add_custom_target(format_mutils-serialization clang-format-3.8 -i *.cpp *.hpp)
