if (${USE_VERBS_API})
    ADD_LIBRARY(rdmc OBJECT rdmc.cpp util.cpp group_send.cpp schedule.cpp verbs_helper.cpp)
else ()
    ADD_LIBRARY(rdmc OBJECT rdmc.cpp util.cpp group_send.cpp schedule.cpp lf_helper.cpp)
endif()
target_include_directories(rdmc PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)

#find_library(SLURM_FOUND slurm)
#if (SLURM_FOUND)
#  target_link_libraries(rdmc slurm)
#  add_definitions(-DUSE_SLURM)
#endif (SLURM_FOUND)

#ADD_EXECUTABLE(verbs-mcast experiment.cpp)
#TARGET_LINK_LIBRARIES(verbs-mcast rdmc)
