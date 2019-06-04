/**
 * @file derecho_exception.h
 *
 * @date Feb 13, 2017
 */

#pragma once

#include <derecho/core/git_version.hpp>
#include <exception>
#include <sstream>
#include <string>

namespace derecho {

/**
 * Base exception class for all exceptions raised by Derecho.
 */
struct derecho_exception : public std::exception {
    const std::string message;
    derecho_exception(const std::string& message)
            : message(message + " Derecho version: " + VERSION_STRING_PLUS_COMMITS) {}

    const char* what() const noexcept {
        return message.c_str();
    }
};

/**
 * Exception that means a reference-like type is "empty" (does not contain a
 * valid object).
 */
struct empty_reference_exception : public derecho_exception {
    empty_reference_exception(const std::string& message) : derecho_exception(message) {}
};

/**
 * Exception that means the user made an invalid request for a subgroup handle,
 * such as by supplying an out-of-bounds subgroup index.
 */
struct invalid_subgroup_exception : public derecho_exception {
    invalid_subgroup_exception(const std::string& message) : derecho_exception(message) {}
};

/**
 * Exception that means the user requested an operation targeting a specific node
 * and that node was not as valid target, e.g. because the node is not currently
 * a member of the group.
 */
struct invalid_node_exception : public derecho_exception {
    invalid_node_exception(const std::string& message) : derecho_exception(message) {}
};
}  // namespace derecho
