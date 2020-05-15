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

#include "../openssl/signature.hpp"

namespace persistent {

using version_t = int64_t;

//These function types represent the API of a Persistent Object. All versions
//of the Persistent<T> template have functions that implement this interface.
/**
 * The "version" function in a persistent object should create a new record of
 * the object's current state that is associated with the provided version
 * number (parameter 1) and HLC time (parameter 2).
 */
using VersionFunc = std::function<void(const version_t&, const HLC&)>;
/**
 * The "sign" function in a persistent object should update the provided Signer
 * object (parameter 1) with the state of the object in its latest version.
 */
using SignFunc = std::function<void(openssl::Signer&)>;
/**
 * The "persist" function in a persistent object should persist as many
 * versions as possible to persistent storage, up to the current version. Its
 * return value is the latest version that was successfully persisted.
 * In addition, it takes as input a signature to add to the log, which should
 * have been computed by an earlier call to the sign function.
 */
using PersistFunc = std::function<const version_t(const unsigned char*, const std::size_t)>;
/**
 * The "trim" function in a persistent object should discard old versions from
 * the object's persistent log, deleting all records earlier than the provided
 * version number (paramter 1).
 */
using TrimFunc = std::function<void(const version_t&)>;
/**
 * The "latest persisted getter" function in a persistent object should return
 * the latest version that has been successfully persisted to storage.
 */
using LatestPersistedGetterFunc = std::function<const version_t(void)>;
/**
 * The "truncate" function in a persistent object should truncate the object's
 * persistent log, deleting all versions newer than the specified version
 * (parameter 1). This is used during failure recovery to delete recent versions
 * that must be aborted.
 */
using TruncateFunc = std::function<void(const int64_t&)>;
}  // namespace persistent
