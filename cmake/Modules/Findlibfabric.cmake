# This module defines
# libfabric_LIBRARY, the name of the library to link against
# libfabric_FOUND, if false, do not try to link to SDL2
# libfabric_INCLUDE_DIR, where to find SDL.h

find_path(libfabric_INCLUDE_DIR NAMES rdma/fabric.h)
find_library(libfabric_LIBRARY NAMES fabric)

if (libfabric_INCLUDE_DIR AND EXISTS "${libfabric_INCLUDE_DIR}/rdma/fabric.h")
    file(STRINGS "${libfabric_INCLUDE_DIR}/rdma/fabric.h" libfabric_VERSION_STR
         REGEX "^#[\t ]*define[\t ]+FI_(MAJOR|MINOR)_VERSION[\t ]+([0-9]+)$")
    unset(libfabric_VERSION_STRING)
    foreach(VPART MAJOR MINOR)
        foreach(VLINE ${libfabric_VERSION_STR})
            if(VLINE MATCHES "^#[\t ]*define[\t ]+FI_$(VPART)_VERSION[\t ]+([0-9]+)$")
                set(libfabric_VERSION_PART "${CMAKE_MATCH_1}")
                if(libfabric_VERSION_STRING)
                    set(libfabric_VERSION_STRING "${libfabric_VERSION_STRING}.${libfabric_VERSION_PART}")
                else()
                    set(libfabric_VERSION_STRING "${libfabric_VERSION_PART}")
                endif()
            endif()
        endforeach()
    endforeach()
endif()

# include(${CMAKE_CURRENT_LIST_DIR}/FindPackageHandleStandardArgs.cmake)
include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(libfabric
                                  REQUIRED_VARS libfabric_LIBRARY libfabric_INCLUDE_DIR
                                  VERSION_VAR libfabric_VERSION_STRING)

if(libfabric_FOUND)
    set(libfabric_INCLUDE_DIRS ${libfabric_INCLUDE_DIR})
    set(libfabric_LIBRARIES ${libfabric_LIBRARY})
endif()
