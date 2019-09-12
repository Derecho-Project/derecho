#----------------------------------------------------------------
# Generated CMake target import file for configuration "Debug".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "derecho" for configuration "Debug"
set_property(TARGET derecho APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(derecho PROPERTIES
  IMPORTED_LINK_INTERFACE_LIBRARIES_DEBUG "rdmacm;ibverbs;rt;pthread;atomic;stdc++fs;/usr/local/lib/libfabric.so;-L/usr/local/lib -lmutils;-L/usr/local/lib -lmutils-tasks"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib/libderecho.so.0.9.1"
  IMPORTED_SONAME_DEBUG "libderecho.so.0.9"
  )

list(APPEND _IMPORT_CHECK_TARGETS derecho )
list(APPEND _IMPORT_CHECK_FILES_FOR_derecho "${_IMPORT_PREFIX}/lib/libderecho.so.0.9.1" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
