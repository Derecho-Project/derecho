# I'm worried that naming this library "openssl" will conflict with the
# system C library named OpenSSL, so I'll name it "openssl_wrapper" to be safe

ADD_LIBRARY(openssl_wrapper OBJECT hash.cpp openssl_exception.cpp signature.cpp)
target_include_directories(openssl_wrapper PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)
# OBJECT libraries are not linked, but this command can be used to declare library dependencies
target_link_libraries(openssl_wrapper OpenSSL::Crypto spdlog::spdlog)
