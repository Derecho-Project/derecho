/**
 * @file PersistentTypenames.hpp
 *
 * This file defines some type aliases used by the Persistence library. It's
 * separated out from Persistence.hpp so that other Derecho components can see
 * these definitions without needing to compile all of Persistence.hpp.
 */

#pragma once
#include <cstdint>
#include <functional>

namespace persistent {

using version_t = int64_t;

// function types to be registered for create version
// , persist version, and trim a version
using VersionFunc = std::function<void(const version_t &,const HLC &)>;
using PersistFunc = std::function<const version_t(void)>;
using TrimFunc = std::function<void(const version_t &)>;
using LatestPersistedGetterFunc = std::function<const version_t(void)>;
using TruncateFunc = std::function<void(const int64_t &)>;
// this function is obsolete, now we use a shared pointer to persistence registry
// using PersistentCallbackRegisterFunc = std::function<void(const char*,VersionFunc,PersistFunc,TrimFunc)>;
}

