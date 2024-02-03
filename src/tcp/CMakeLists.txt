ADD_LIBRARY(tcp OBJECT tcp.cpp)
target_include_directories(tcp PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
)
