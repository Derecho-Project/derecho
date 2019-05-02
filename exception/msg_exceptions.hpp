#pragma once

#include "exceptions.hpp"

namespace msg {
class MsgException : public std::exception {
    std::string exception_str;
    const char* what() const throw() {
        return exception_str.c_str();
    }

public:
    MsgException(std::string exception_str)
            : std::exception(),
              exception_str(std::string("Message Exception: ") + exception_str) {
    }
};

class MsgSizeViolation : public MsgException {
public:
    MsgSizeViolation(std::string exception_str)
            : MsgException(exception_str) {
    }
};

class NotASender : public MsgException {
public:
    NotASender(std::string exception_str)
            : MsgException(exception_str) {
    }
};
}  // namespace msg
