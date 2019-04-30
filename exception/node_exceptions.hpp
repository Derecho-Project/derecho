#pragma once

#include "exceptions.hpp"

namespace node {
class NodeException : public std::exception {
    std::string exception_str;
    const char* what() const throw() {
        return exception_str.c_str();
    }

public:
    NodeException(std::string exception_str)
            : std::exception(),
              exception_str(std::string("Node Exception: ") + exception_str) {
    }
};

class NodeIDNotFound : public NodeException {
public:
    NodeIDNotFound(std::string exception_str)
            : NodeException(exception_str) {
    }
};

class IDNotInMembers : public NodeException {
public:
    IDNotInMembers(std::string exception_str)
            : NodeException(exception_str) {
    }  
};
}  // namespace node
