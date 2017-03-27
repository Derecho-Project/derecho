/**
 * @file derecho_exception.h
 *
 * @date Feb 13, 2017
 * @author edward
 */

#pragma once

#include <exception>
#include <string>

namespace derecho {

/**
 * Base exception class for all exceptions raised by Derecho.
 */
struct derecho_exception : public std::exception {
public:
    const std::string message;
    derecho_exception(const std::string& message) : message(message) {}

    const char* what() const noexcept { return message.c_str(); }
};

/**
 * Exception that means a reference-like type is "empty" (does not contain a
 * valid object).
 */
struct empty_reference_exception : public derecho_exception {
public:
    empty_reference_exception(const std::string& message) : derecho_exception(message) {}
};
}
