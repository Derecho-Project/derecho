/**
 * @file    derecho_exception.hpp
 * @brief   This file defines the exception types in Derecho.
 */

#pragma once

#include "derecho/config.h"
#include "derecho/core/git_version.hpp"

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
 * Exception thrown when Derecho's View Manager detects a partitioning event,
 * which means more than half of the group appears to have failed. This is a
 * separate type because clients may want to handle this exception differently
 * from other kinds of exceptions, and it can provide more information about
 * how many nodes have failed at once.
 */
struct derecho_partitioning_exception : public derecho_exception {
    const int failed_count;
    const int remaining_members;
    derecho_partitioning_exception(int failed_count, int remaining_members)
            : derecho_exception("Potential partitioning event: this node is no longer in the majority! "
                                + std::to_string(failed_count) + " nodes out of " + std::to_string(remaining_members) + " have failed."),
              failed_count(failed_count),
              remaining_members(remaining_members) {}
};

/**
 * Exception that means the user has attempted to fill a fixed-size buffer with
 * a message that is too large. This usually means the maximum message size for
 * a subgroup has been configured to too small a size.
 */
struct buffer_overflow_exception : public derecho_exception {
    buffer_overflow_exception(const std::string& message) : derecho_exception(message) {}
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
