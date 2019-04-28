#pragma once

#include "exceptions.hpp"

namespace rdma {
class RDMAException : public std::exception {
    std::string exception_str;
    const char* what() const throw() {
        return exception_str.c_str();
    }

public:
    RDMAException(std::string exception_str)
            : std::exception(),
              exception_str(std::string("RDMA Exception: ") + exception_str) {
    }
};

class RDMAConnectionRemoved : public RDMAException {
public:
    RDMAConnectionRemoved(std::string exception_str)
            : RDMAException(exception_str) {
    }
};

class RDMAConnectionBroken : public RDMAException {
public:
    RDMAConnectionBroken(std::string exception_str)
            : RDMAException(exception_str) {
    }
};
}  // namespace rdma
