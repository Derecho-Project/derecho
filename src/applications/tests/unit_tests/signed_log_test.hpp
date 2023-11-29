#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>

#include <derecho/core/derecho.hpp>

/**
 * This object contains state that is shared between the replicated test objects
 * and the main thread, rather than stored inside the replicated objects. It's
 * used to provide a way for the replicated objects to "call back" to the main
 * thread. Each replicated object will get a pointer to this object when it is
 * constructed or deserialized, set up by the deserialization manager.
 */
struct TestState : public derecho::DeserializationContext {
    // The next 3 are set by the main thread after it figures out which subgroup this node was assigned to
    derecho::subgroup_id_t my_subgroup_id;
    uint32_t subgroup_total_updates;
    bool my_subgroup_is_unsigned;
    // Set by each replicated object when the last update is delivered and its version is known
    persistent::version_t last_version;
    // Used to alert other threads (i.e. global callbacks) that last_version has been set
    std::atomic<bool> last_version_ready;
    // Mutex for subgroup_finished
    std::mutex finish_mutex;
    // Condition variable used to indicate when this node's subgroup has finished persisting/verifying all updates
    std::condition_variable subgroup_finished_condition;
    // Boolean to set to true when signaling the condition variable
    bool subgroup_finished;
    // Called from replicated object update_state methods to notify the main thread that an update was delivered
    void notify_update_delivered(uint64_t update_counter, persistent::version_t version);
    // Called by Derecho's global persistence callback
    void notify_global_persistence(derecho::subgroup_id_t subgroup_id, persistent::version_t version);
    // Called by Derecho's global verified callback
    void notify_global_verified(derecho::subgroup_id_t subgroup_id, persistent::version_t version);
};

/**
 * Test object with one signed persistent field
 */
class OneFieldObject : public mutils::ByteRepresentable,
                       public derecho::GroupReference,
                       public derecho::SignedPersistentFields {
    persistent::Persistent<std::string> string_field;
    // Counts the number of updates delivered within this subgroup.
    // Not persisted, but needs to be replicated so that all replicas have a consistent count of
    // the total number of messages, even across view changes
    uint64_t updates_delivered;
    // Pointer to an object held by the main thread, set from DeserializationManager
    TestState* test_state;

public:
    /** Factory constructor */
    OneFieldObject(persistent::PersistentRegistry* registry, TestState* test_state);
    /** Deserialization constructor */
    OneFieldObject(persistent::Persistent<std::string>& other_value,
                   uint64_t other_updates_delivered,
                   TestState* test_state);

    std::string get_state() const {
        return *string_field;
    }

    void update_state(const std::string& new_value);

    REGISTER_RPC_FUNCTIONS(OneFieldObject, P2P_TARGETS(get_state), ORDERED_TARGETS(update_state));

    DEFAULT_SERIALIZE(string_field, updates_delivered);
    DEFAULT_DESERIALIZE_NOALLOC(OneFieldObject);
    static std::unique_ptr<OneFieldObject> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer);
};

/**
 * Test object with two signed persistent fields
 */
class TwoFieldObject : public mutils::ByteRepresentable,
                       public derecho::GroupReference,
                       public derecho::SignedPersistentFields {
    persistent::Persistent<std::string> foo;
    persistent::Persistent<std::string> bar;
    uint64_t updates_delivered;
    TestState* test_state;

public:
    /** Factory constructor */
    TwoFieldObject(persistent::PersistentRegistry* registry, TestState* test_state);
    /** Deserialization constructor */
    TwoFieldObject(persistent::Persistent<std::string>& other_foo,
                   persistent::Persistent<std::string>& other_bar,
                   uint64_t other_updates_delivered,
                   TestState* test_state);

    std::string get_foo() const {
        return *foo;
    }

    std::string get_bar() const {
        return *bar;
    }

    void update(const std::string& new_foo, const std::string& new_bar);

    REGISTER_RPC_FUNCTIONS(TwoFieldObject, P2P_TARGETS(get_foo, get_bar), ORDERED_TARGETS(update));
    DEFAULT_SERIALIZE(foo, bar, updates_delivered);
    DEFAULT_DESERIALIZE_NOALLOC(TwoFieldObject);
    static std::unique_ptr<TwoFieldObject> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer);
};

/**
 * Test object with one un-signed persistent field
 */
class UnsignedObject : public mutils::ByteRepresentable,
                       public derecho::GroupReference,
                       public derecho::PersistsFields {
    persistent::Persistent<std::string> string_field;
    uint64_t updates_delivered;
    TestState* test_state;

public:
    /** Factory constructor */
    UnsignedObject(persistent::PersistentRegistry* registry, TestState* test_state);
    /** Deserialization constructor */
    UnsignedObject(persistent::Persistent<std::string>& other_field,
                   uint64_t other_updates_delivered,
                   TestState* test_state);
    std::string get_state() const {
        return *string_field;
    }

    void update_state(const std::string& new_value);

    REGISTER_RPC_FUNCTIONS(UnsignedObject, P2P_TARGETS(get_state), ORDERED_TARGETS(update_state));
    DEFAULT_SERIALIZE(string_field, updates_delivered);
    DEFAULT_DESERIALIZE_NOALLOC(UnsignedObject);
    static std::unique_ptr<UnsignedObject> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buffer);
};