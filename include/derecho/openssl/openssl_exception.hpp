/**
 * @file openssl_exception.hpp
 *
 * Defines some exception types and an error-printing function for the OpenSSL
 * wrapper library.
 */
#pragma once

#include <derecho/config.h>
#include <exception>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include <openssl/err.h>

namespace openssl {

/**
 * Gets all of the current OpenSSL error strings (as returned by the
 * ERR_print_errors function) and returns them in a single std::string,
 * separated by newlines. Note that this will clear the current OpenSSL "error
 * stack," so it is not idempotent.
 * @return A string containing the current OpenSSL errors.
 */
std::string openssl_errors_to_string();

/**
 * Gets the OpenSSL error string associated with an error code and appends it
 * to the provided message. Helper function for the constructor of openssl_error.
 */
std::string get_error_string(unsigned long error_code, const std::string& extra_message);

/**
 * An exception that represents an OpenSSL error, used for converting "error
 * returns" into exceptions. Contains the error code returned by OpenSSL at the
 * time an error occurred, split into its three component parts, plus a what()
 * string that includes the OpenSSL error string.
 */
struct openssl_error : public std::runtime_error {
    const int library_code;
    const int reason_code;
    openssl_error(unsigned long error_code_packed, const std::string& operation)
            : runtime_error(get_error_string(error_code_packed, operation)),
              library_code(ERR_GET_LIB(error_code_packed)),
              reason_code(ERR_GET_REASON(error_code_packed)) {}
};

/**
 * An exception that indicates that a function that operates on a caller-provided
 * byte buffer failed because the buffer was too small. This is needed because
 * of OpenSSL's C interface, which operates on raw byte buffers rather than
 * expandable C++ containers.
 */
struct buffer_overflow : public std::runtime_error {
    const std::size_t required_size;
    buffer_overflow(const std::string& function_name, std::size_t required_size, std::size_t actual_size)
            : runtime_error(function_name + ": needed a buffer of size " + std::to_string(required_size)
                            + " but received one of size " + std::to_string(actual_size)),
              required_size(required_size) {}
};

//The standard library doesn't contain any exception types to represent file-related errors,
//but OpenSSL requires reading and writing to files, so we have to create some

/**
 * An exception that reports that a file operation failed because of an I/O
 * error reported by the standard library's file interface. Contains the
 * "errno" value reported by the file operation.
 */
struct file_error : public std::runtime_error {
    const int error_code;
    file_error(const int errno_value, const std::string& message = "")
            : runtime_error(message), error_code(errno_value) {}
};

/**
 * An exception that reports that a file operation failed because the file
 * could not be found by the OS. A specialization of file_error for the error
 * code ENOENT.
 */
struct file_not_found : public file_error {
    file_not_found(const int errno_value, const std::string& filename)
            : file_error(errno_value, "File not found: " + filename) {}
};

/**
 * An exception that reports that a file operation faled because the OS denied
 * permission to this process to open the file. A specialization of file_error
 * for the error codes EACCES and EPERM.
 */
struct permission_denied : public file_error {
    permission_denied(const int errno_value, const std::string& filename)
            : file_error(errno_value, "Error opening file " + filename + ": permission denied.") {}
};

}  // namespace openssl
