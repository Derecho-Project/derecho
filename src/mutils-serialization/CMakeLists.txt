cmake_minimum_required (VERSION 3.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project (mutils-serialization)

add_library(mutils-serialization OBJECT SerializationSupport.cpp)
target_include_directories(mutils-serialization PRIVATE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(mutils-serialization mutils::mutils)

add_executable(test serialization-test.cpp SerializationSupport.cpp)
target_include_directories(test PRIVATE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(test mutils::mutils)

add_executable(deserialization_test test_new_deserialization.cpp SerializationSupport.cpp)
target_include_directories(deserialization_test PRIVATE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(deserialization_test mutils::mutils)
