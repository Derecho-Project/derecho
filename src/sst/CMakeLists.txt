if (${USE_VERBS_API})
    ADD_LIBRARY(sst OBJECT verbs.cpp poll_utils.cpp)
else()
    ADD_LIBRARY(sst OBJECT lf.cpp poll_utils.cpp)
endif()
target_include_directories(sst PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)

add_custom_target(format_sst clang-format -i *.cpp *.hpp)
