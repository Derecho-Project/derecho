/**
 * @file PersistentTypenames.hpp
 *
 * This file defines some interface types used by the Persistence library. It's
 * separated out from Persistence.hpp so that other Derecho components can see
 * these definitions without needing to compile all of Persistence.hpp.
 */

#pragma once
#include <cstdint>
#include <functional>

#include "../openssl/signature.hpp"

namespace persistent {

using version_t = int64_t;

/**
 * This bundle of function pointers represents the API of a Persistent Object.
 * All versions of the Persistent<T> template have functions that implement
 * this interface.
 */
struct PersistentObjectFunctions {
    /**
     * The "version" function in a persistent object should create a new record of
     * the object's current state that is associated with the provided version
     * number (parameter 1) and HLC time (parameter 2).
     */
    std::function<void(const version_t&, const HLC&)> version;
    /**
     * The "update signature" function in a persistent object should update the
     * provided Signer object (parameter 2) with the state of the object in a
     * specific version (parameter 1). It returns the number of bytes that were
     * added to the Signer.
     */
    std::function<std::size_t(const version_t&, openssl::Signer&)> update_signature;
    /**
     * The "add signature" function in a persistent object should add the
     * provided signature (parameter 2) to the log at the specified version
     * number (parameter 1).
     */
    std::function<void(const version_t&, const unsigned char*)> add_signature;
    /**
     * The "update verifier" function should update the provided Verifier object
     * (parameter 2) with the state of the object in a specific version
     * (parameter 1).
     */
    std::function<void(const version_t&, openssl::Verifier&)> update_verifier;
    /**
     * The "persist" function in a persistent object should persist a batch of
     * versions to persistent storage, up to the specified version.
     */
    std::function<void(const version_t&)> persist;
    /**
     * The "trim" function in a persistent object should discard old versions from
     * the object's persistent log, deleting all records earlier than the provided
     * version number (paramter 1).
     */
    std::function<void(const version_t&)> trim;
    /**
     * The "get latest version" function in a persistent object should return the
     * object's current version number.
     */
    std::function<version_t(void)> get_latest_version;
    /**
     * The "get latest persisted" function in a persistent object should return
     * the latest version that has been successfully persisted to storage.
     */
    std::function<version_t(void)> get_latest_persisted;
    /**
     * The "truncate" function in a persistent object should truncate the object's
     * persistent log, deleting all versions newer than the specified version
     * (parameter 1). This is used during failure recovery to delete recent versions
     * that must be aborted.
     */
    std::function<void(const int64_t&)> truncate;
};
}  // namespace persistent
