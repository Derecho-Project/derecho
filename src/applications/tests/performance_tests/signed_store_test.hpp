#pragma once
#include <array>
#include <chrono>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <tuple>
#include <vector>

#include <derecho/core/derecho.hpp>
#include <derecho/openssl/hash.hpp>

/**
 * This object, representing arbitrary binary data, was (mostly) copied and
 * pasted from the Cascade code.
 */
class Blob : public mutils::ByteRepresentable {
private:
    //True when this Blob wraps a byte buffer within a serialized message and doesn't own the bytes
    bool is_temporary;

public:
    uint8_t* bytes;
    std::size_t size;

    // constructor - copy to own the data
    Blob(const uint8_t* const b, const decltype(size) s);

    // copy constructor - copy to own the data
    Blob(const Blob& other);

    // move constructor - accept the memory from another object
    Blob(Blob&& other);

    // default constructor - no data at all
    Blob();

    // destructor
    virtual ~Blob();

    // move evaluator:
    Blob& operator=(Blob&& other);

    // copy evaluator:
    Blob& operator=(const Blob& other);

    // serialization/deserialization supports
    std::size_t to_bytes(uint8_t* v) const;

    std::size_t bytes_size() const;

    void post_object(const std::function<void(uint8_t const* const, std::size_t)>& f) const;

    void ensure_registered(mutils::DeserializationManager&) {}

    static std::unique_ptr<Blob> from_bytes(mutils::DeserializationManager*, const uint8_t* const buffer);

    static mutils::context_ptr<Blob> from_bytes_noalloc(mutils::DeserializationManager* ctx,
                                                        const uint8_t* const buffer);

    static mutils::context_ptr<const Blob> from_bytes_noalloc_const(mutils::DeserializationManager* m,
                                                                    const uint8_t* const buffer);

private:
    //Non-owning constructor, used only by from_bytes_noalloc
    Blob(uint8_t* buffer, std::size_t size, bool temporary);
};

class ObjectStore : public mutils::ByteRepresentable,
                    public derecho::PersistsFields,
                    public derecho::GroupReference {
    persistent::Persistent<Blob> object_log;
    /** A local cache of QueryResults from ordered_update() calls. Not part of the replicated state. */
    mutable std::map<persistent::version_t, derecho::rpc::QueryResults<void>> update_results;

    using derecho::GroupReference::group;
    /**
     * Shared with the main thread to tell it when the experiment is done and it should
     * call group.leave() (which can't be done from inside this subgroup object).
     */
    std::shared_ptr<std::atomic<bool>> experiment_done;

public:
    ObjectStore(persistent::PersistentRegistry* pr,
                std::shared_ptr<std::atomic<bool>> experiment_done);
    /** Deserialization constructor */
    ObjectStore(persistent::Persistent<Blob>& other_log) : object_log(std::move(other_log)) {}
    /**
     * P2P-callable function that creates a new log entry with the provided data.
     * @return The version assigned to the new log entry, and the timestamp assigned to the new log entry
    */
    std::pair<persistent::version_t, uint64_t> update(const Blob& new_data) const;

    /** Actual implementation of update, only callable from within the subgroup as an ordered send. */
    void ordered_update(const Blob& new_data);

    /**
     * P2P-callable function that blocks until the specified version has finished persisting.
     * This only works if it is called on the same replica that was originally contacted to
     * submit the update via update(), since other replicas won't have a QueryResults for it.
     * Returns "true" so that the RPC function sends a reply message, since there is currently
     * no way to determine when a void RPC function actually finishes executing on the callee.
     */
    bool await_persistence(const persistent::version_t& version) const;

    /**
     * Retrieves the data in the log at a specific version number. P2P-callable.
     */
    Blob get(const persistent::version_t& version) const;
    /**
     * Retrieves the data in the log at the latest (current) version. P2P-callable.
     */
    Blob get_latest() const;

    /**
     * Causes the program to exit. Called at the end of the test to signal that the
     * client nodes are done sending updates.
     */
    void end_test() const;

    DEFAULT_SERIALIZATION_SUPPORT(ObjectStore, object_log);
    REGISTER_RPC_FUNCTIONS(ObjectStore, ORDERED_TARGETS(ordered_update),
                           P2P_TARGETS(update, await_persistence, get, get_latest, end_test));
};

using SHA256Hash = std::array<unsigned char, 32>;

class SignatureStore : public mutils::ByteRepresentable,
                       public derecho::SignedPersistentFields,
                       public derecho::GroupReference {
    using derecho::GroupReference::group;

    persistent::Persistent<SHA256Hash> hashes;

    /**
     * Shared with the main thread to tell it when the experiment is done and it should
     * call group.leave() (which can't be done from inside this subgroup object).
     */
    std::shared_ptr<std::atomic<bool>> experiment_done;

public:
    SignatureStore(persistent::PersistentRegistry* pr,
                   std::shared_ptr<std::atomic<bool>> experiment_done);
    /** Deserialization constructor */
    SignatureStore(persistent::Persistent<SHA256Hash>& other_hashes) : hashes(std::move(other_hashes)) {}

    /**
     * P2P-callable function that appends a new object-update hash to the signed log.
     * @return The signature generated on this hash (to eventually return to the client)
     */
    std::vector<unsigned char> add_hash(const SHA256Hash& hash) const;

    /**
     * Ordered-send component of add_hash: actually appends to the log and
     * generates a new version, which will be signed.
     * @param hash The object hash to append and sign.
     */
    void ordered_add_hash(const SHA256Hash& hash);

    /**
     * Causes the program to exit. Called at the end of the test to signal that the
     * client nodes are done sending updates.
     */
    void end_test() const;

    DEFAULT_SERIALIZATION_SUPPORT(SignatureStore, hashes);
    REGISTER_RPC_FUNCTIONS(SignatureStore, P2P_TARGETS(add_hash, end_test), ORDERED_TARGETS(ordered_add_hash));
};

class ClientTier : public mutils::ByteRepresentable,
                   public derecho::GroupReference {
    using derecho::GroupReference::group;
    //Used to pick random members of the storage and signature subgroups to contact; not replicated
    mutable std::mt19937 random_engine;
    //This ensures the test data is allocated before the test starts
    Blob test_data;

public:
    ClientTier(std::size_t test_data_size);

    //Type alias for the overly-long return type of submit_update
    using version_signature = std::tuple<persistent::version_t, uint64_t, std::vector<unsigned char>>;
    /**
     * RPC function that submits an update to the object store and gets its hash signed;
     * intended to be called by an outside client using ExternalGroupClient.
     * @return The version assigned to the update, the timestamp assigned to the update,
     * and the signature assigned to the update.
     */
    version_signature submit_update(const Blob& data) const;
    /**
     * The main function of the signed store bandwidth test. Returns a useless bool so that
     * the actual "main" thread can block waiting for it to complete.
     */
    bool update_batch_test(const int& num_updates) const;
    REGISTER_RPC_FUNCTIONS(ClientTier, P2P_TARGETS(submit_update, update_batch_test));

    //This class has no serialized state, so DEFAULT_SERIALIZATION_SUPPORT won't work.
    //We must still provide trivial implementations of these functions.
    std::size_t to_bytes(uint8_t* v) const { return 0; };

    std::size_t bytes_size() const { return 0; };

    void post_object(const std::function<void(uint8_t const* const, std::size_t)>& f) const {};

    void ensure_registered(mutils::DeserializationManager&) {}

    //This should never be called, but needs to exist
    static std::unique_ptr<ClientTier> from_bytes(mutils::DeserializationManager*, const uint8_t* const v) {
        return std::make_unique<ClientTier>(0);
    };
};
