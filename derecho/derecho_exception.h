/**
 * @file derecho_exception.h
 *
 * @date Feb 13, 2017
 * @author edward
 */

#pragma once

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
}
