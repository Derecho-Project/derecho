if (${USE_VERBS_API})
    ADD_LIBRARY(sst OBJECT verbs.cpp poll_utils.cpp)
else()
    ADD_LIBRARY(sst OBJECT lf.cpp poll_utils.cpp)
endif()
target_include_directories(sst PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
)
target_link_libraries(sst spdlog::spdlog)
