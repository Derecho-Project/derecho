#pragma once

#include "../openssl/signature.hpp"
#include "HLC.hpp"

#include <cstdint>
#include <functional>

namespace persistent {

using version_t = int64_t;

/**
 * This interface represents the API of a Persistent Object, and is inherited
 * by all versions of the Persistent<T> template. It can be used to call
 * functions on a Persistent<T> without knowing the template parameter T.
 */
class PersistentObject {
public:
    /**
     * Creates a new record of the object's current state that is associated
     * with the provided version number and HLC time.
     * @param version The version number to apply
     * @param hlc The HLC timestamp to apply
     */
    virtual void version(version_t version, const HLC& hlc) = 0;
    /**
     * Updates the provided Signer object with the state of the Persistent
     * object at a specific version. Does nothing if signatures are disabled.
     * @param version The version being signed
     * @param signer The Signer object to update with bytes from this version
     * @return The number of bytes added to the Signer object
     */
    virtual std::size_t updateSignature(version_t version, openssl::Signer& signer) = 0;
    /**
     * Adds a signature to the Persistent object's log at the specified version
     * number, and records the previous signed version (whose signature is
     * included in this signature). Does nothing if signatures are disabled.
     * @param version The version to add the signature to
     * @param signature A byte buffer containing the signature; assumed to be
     * the correct length for signatures on this Persistent (which is a constant
     * based on the key size).
     * @param prev_signed_ver The previous version whose signature is included
     * in (signed as part of) this signature
     */
    virtual void addSignature(version_t version, const uint8_t* signature, version_t prev_signed_ver) = 0;
    /**
     * Retrieves the signature associated with a specific version of the
     * Persistent object, as well as the previous signed version. Does
     * nothing and returns false if signatures are disabled.
     * @param version The version to retrieve the signature for
     * @param signature A byte buffer in which the signature will be placed
     * @param prev_signed_ver A reference to a version_t that will be updated
     * with the previous version whose signature is signed as part of this
     * signature
     * @return True if a signature was retrieved successfully, false if there
     * was no version matching the requested version number (in which case the
     * output parameters will be unmodified), or if signatures are disabled.
     */
    virtual bool getSignature(version_t version, uint8_t* signature, version_t& prev_signed_ver) const = 0;
    /**
     * @return the size, in bytes, of each signature in this Persistent object's log.
     * Useful for allocating a correctly-sized buffer before calling get_signature.
     */
    virtual std::size_t getSignatureSize() const = 0;
    /**
     * Updates the provided Verifier object with the state of the Persistent
     * object at a specific version. Does nothing if signatures are disabled.
     * @param version The version to add to the verifier
     * @param verifier The Verifier to update
     */
    virtual void updateVerifier(version_t version, openssl::Verifier& verifier) = 0;
    /**
     * Persists versions to persistent storage, up to the provided version.
     * @param version The highest version number to persist
     */
    virtual void persist(version_t version) = 0;
    /**
     * Trims the beginning (oldest part) of the log, discarding versions older
     * than the specified version
     * @param earliest_version The earliest version to keep
     */
    virtual void trim(version_t earliest_version) = 0;
    /**
     * @return the Persistent object's current version number
     */
    virtual version_t getLatestVersion() const = 0;
    /**
     * @return the Persistent object's newest version that has been persisted
     * successfully
     */
    virtual version_t getLastPersistedVersion() const = 0;
    /**
     * Truncates the log, deleting all versions newer than the provided argument.
     * Since this throws away recently-used data, it should only be used during
     * failure recovery when those versions must be rolled back.
     * @param latest_version The latest version to keep
     */
    virtual void truncate(version_t latest_version) = 0;
    /**
     * Ensure destructors continue to work with inheritance
     */
    virtual ~PersistentObject() = default;
};
}  // namespace persistent
