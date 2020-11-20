cmake_minimum_required(VERSION 2.8.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
# I'm worried that naming this project and library "openssl" will conflict with the
# system C library named OpenSSL, so I'll name it "openssl_wrapper" to be safe
PROJECT(openssl_wrapper CXX)

ADD_LIBRARY(openssl_wrapper OBJECT hash.cpp openssl_exception.cpp signature.cpp)
target_include_directories(openssl_wrapper PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${OPENSSL_INCLUDE_DIR}>
)