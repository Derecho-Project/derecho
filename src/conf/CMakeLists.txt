add_library(conf OBJECT conf.cpp)
target_include_directories(conf PRIVATE
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
