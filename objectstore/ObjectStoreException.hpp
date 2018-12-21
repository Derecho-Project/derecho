#ifndef OBJECTSTORE_EXCEPTION_HPP
#define OBJECTSTORE_EXCEPTION_HPP

#include <inttypes.h>

namespace objectstore{ 
// Exception definition
#define OBJECTSTORE_EXP(errcode, usercode) \
    ((((errcode)&0xffffffffull) << 32) | ((usercode)&0xffffffffull))
#define OBJECTSTORE_EXP_MALLOC(x) OBJECTSTORE_EXP(0, (x))
}

#endif//OBJECTSTORE_EXCEPTION_HPP
