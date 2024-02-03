#ifndef PERSISTENT_EXCEPTION_HPP
#define PERSISTENT_EXCEPTION_HPP

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <derecho/config.h>

namespace persistent {

/** Base type for persistent exceptions, just a renaming of std::runtime_error */
struct persistent_exception : public std::runtime_error {
    persistent_exception(const std::string& message) : runtime_error(message) {}
};

struct persistent_not_implemented : public persistent_exception {
    persistent_not_implemented(const std::string& message = "persistent_not_implemented")
            : persistent_exception(message) {}
};

struct persistent_log_full : public persistent_exception {
    persistent_log_full(const std::string& message = "persistent_log_full")
            : persistent_exception(message) {}
};

/** Indicates that there was an error using a pthread lock, and includes the resulting errno */
struct persistent_lock_error : public persistent_exception {
    const int error_num;
    persistent_lock_error(const std::string& message, int errno_value)
            : persistent_exception(message + " Errno: " + std::to_string(errno_value) + ". Description: " + std::strerror(errno_value)),
              error_num(errno_value) {}
};

/** Indicates that there was an error with a file operation, and includes the resulting errno */
struct persistent_file_error : public persistent_exception {
    const int error_num;
    persistent_file_error(const std::string& message, int errno_value)
            : persistent_exception(message + " Errno: " + std::to_string(errno_value) + ". Description: " + std::strerror(errno_value)),
              error_num(errno_value) {}
};

/**
 * Type of persistent exception that indicates a requested index, version, etc.
 * was out-of-range for the log. Subtypes specify the type of thing that was out of range.
 */
struct persistent_out_of_range : public persistent_exception {
    persistent_out_of_range(const std::string& message) : persistent_exception(message) {}
};

/**
 * A persistent exception that indicates the requested version is invalid (out-of-range)
 */
struct persistent_invalid_version : public persistent_out_of_range {
    // Use int64_t instead of version_t so this file won't depend on Persistent.hpp
    const int64_t invalid_version;
    persistent_invalid_version(int64_t ver)
            : persistent_out_of_range("Invalid version: " + std::to_string(ver)),
              invalid_version(ver) {}
};

/**
 * A persistent exception that indicates the requested HLC is invalid (out-of-range)
 */
struct persistent_invalid_hlc : public persistent_out_of_range {
    // For now, this doesn't include the requested HLC because HLC doesn't have a string conversion
    persistent_invalid_hlc() : persistent_out_of_range("Invalid HLC.") {}
};

/**
 * A persistent exception that indicates the requested log index is invalid (out-of-range)
 */
struct persistent_invalid_index : public persistent_out_of_range {
    const int64_t invalid_index;
    persistent_invalid_index(int64_t index)
            : persistent_out_of_range("Invalid index: " + std::to_string(index)),
              invalid_index(index) {}
};

/**
 * A Derecho-specific error indicating that a requested version or HLC is beyond
 * the global stability frontier and thus not available.
 */
struct persistent_version_not_stable : public persistent_out_of_range {
    persistent_version_not_stable()
            : persistent_out_of_range("Requested version is beyond the global stability frontier") {}
};

}  //namespace persistent
#endif  //PERSISTENT_EXCEPTION_HPP
