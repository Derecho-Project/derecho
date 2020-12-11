/**
 * @file public_key_store.hpp
 */
#pragma once

#include <map>
#include <string>

#include "../derecho_exception.hpp"
#include "derecho_internal.hpp"
#include <derecho/openssl/signature.hpp>

namespace derecho {

//Some exceptions that may occur when loading public key files
//There might be a better place to put them, like maybe derecho_exception.hpp

/**
 * An exception that reports that a filename is invalid for some reason, for
 * example because it does not match the expected pattern.
 */
struct invalid_filename : public derecho_exception {
    const std::string filename;
    invalid_filename(const std::string& filename, const std::string& reason)
            : derecho_exception("File: " + filename + ", Reason: " + reason), filename(filename) {}
};

/**
 * An exception that reports that the PublicKeyStore could not find a public
 * key for the requested node.
 */
struct public_key_not_found : public derecho_exception {
    public_key_not_found(const std::string& message) : derecho_exception(message) {}
};

/**
 * A class that keeps track of all the public keys that this node knows about
 * for other nodes in the group. Mostly just a wrapper around a map from IP
 * addresses to keys, with some convenience methods for reading keys from PEM
 * files.
 */
class PublicKeyStore {
    std::map<ip_addr_t, openssl::EnvelopeKey> keys;
    const std::string key_directory;

public:
    /**
     * Constructs a new PublicKeyStore that assumes all public key files are
     * stored in the specified directory. This will automatically search the
     * directory for any existing key files and load them into memory. This
     * makes the constructor more expensive (it does several filesystem I/O
     * operations), but eliminates the need to check for existing keys at "run-
     * time" when a new node joins the group.
     * @param key_file_directory The path to a directory where public key files
     * should be stored, in a format suitable for opendir() to interpret
     */
    PublicKeyStore(const std::string& key_file_directory);
    /**
     * Copy constructor
     */
    PublicKeyStore(const PublicKeyStore& other) = default;
    /**
     * Move constructor
     */
    PublicKeyStore(PublicKeyStore&& other) = default;
    /**
     * Loads a public key from a PEM file and adds it to the key store as the
     * public key for the specified IP address, assuming that file is in the key
     * directory that this PublicKeyStore was constructed with.
     * @param node_ip The IP address of the node identified by this public key
     * @param key_file_name The name of the file to load the key from, which is
     * assumed to be in the directory specified when this PublicKeyStore was
     * constructed
     */
    void add_public_key(const ip_addr_t& node_ip, const std::string& key_file_name);
    /**
     * Adds a public key to the key store from a byte array in memory, assuming the
     * bytes represent a PEM file.
     * @param node_ip The IP address of the node identified by this public key
     * @param key_bytes A byte array containing the PEM file in memory
     * @param key_size The size of the byte array
     */
    void add_public_key(const ip_addr_t& node_ip, const char* key_bytes, std::size_t key_size);
    /**
     * Saves the public key for the specified IP address to the filesystem as a
     * PEM file in the directory that this PublicKeyStore was constructed with.
     * @param node_ip The IP address of a node whose public key should be saved
     * to disk.
     */
    void persist_key_for(const ip_addr_t& node_ip);
    /**
     * Checks whether the PublicKeyStore currently has a key for an IP address.
     */
    bool contains_key_for(const ip_addr_t& ip_address);
    /**
     * Returns (a copy of) the public key corresponding to a given IP address.
     * Since EnvelopeKey actually wraps a pointer to an OpenSSL object, the
     * "copy" required to return one by value is an inexpensive pointer copy.
     * @param ip_address The IP address of a node
     * @return The public key for that node, as an EnvelopeKey
     * @throw public_key_not_found if the PublicKeyStore does not contain a key
     * for that IP address
     */
    openssl::EnvelopeKey get_key_for(const ip_addr_t& ip_address);

    /**
     * Constructs a filename for a PEM file that corresponds to an IP address.
     * Currently this is just [ip_addr].pem
     */
    static std::string filename_for_ip(const ip_addr_t& ip_address);
    /**
     * Attempts to parse the name of a PEM file and extract the IP address in
     * it, assuming the file is named in the standard format produced by
     * filename_for_ip().
     * @throw invalid_filename if the file name does not match the pattern
     * NNN.NNN.NNN.NNN.pem
     */
    static ip_addr_t ip_from_filename(const std::string& key_file_name);
};

}  // namespace derecho