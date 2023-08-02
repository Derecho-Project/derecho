@PACKAGE_INIT@

include(CMakeFindDependencyMacro)
# For dependencies that are properly packaged the "modern" way (exporting targets),
# we need to ensure these targets are forwarded to downstream projects that depend on Derecho
find_dependency(mutils)
find_dependency(mutils-containers)
find_dependency(spdlog 1.3.1)
find_dependency(OpenSSL 1.1.1)
find_dependency(nlohmann_json 3.9.0)

set_and_check(derecho_INCLUDE_DIRS "@PACKAGE_CMAKE_INSTALL_INCLUDEDIR@")
set(derecho_LIBRARIES "-L@PACKAGE_CMAKE_INSTALL_LIBDIR@ -lderecho -pthread")
include("${CMAKE_CURRENT_LIST_DIR}/derechoTargets.cmake")

check_required_components(derecho)
