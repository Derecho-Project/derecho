add_library(utils OBJECT logger.cpp timestamp_logger.cpp)
target_include_directories(utils PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)
# OBJECT libraries are not linked, but this command can be used to declare library dependencies
target_link_libraries(utils spdlog::spdlog)
