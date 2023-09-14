#include <derecho/core/derecho.hpp>

#include <iostream>
#include <map>
#include <random>
#include <string>

// A basic implementation of the IDeltaSupport interface, copied from persistent/test.cpp
class IntegerWithDelta : public mutils::ByteRepresentable, persistent::IDeltaSupport<IntegerWithDelta> {
public:
    int value;
    int delta;
    IntegerWithDelta(int v) : value(v), delta(0) {}
    IntegerWithDelta() : value(0), delta(0) {}
    int add(int op) {
        this->value += op;
        this->delta += op;
        return this->value;
    }
    int sub(int op) {
        this->value -= op;
        this->delta -= op;
        return this->value;
    }
    // Unlike the one in test.cpp, this finalizeCurrentDelta only writes any data if delta is nonzero
    // This simulates a more advanced object like CascadeStoreCore
    virtual void finalizeCurrentDelta(const persistent::DeltaFinalizer& finalizer) {
        if(delta != 0) {
            finalizer(reinterpret_cast<const uint8_t*>(&delta), sizeof(delta));
        } else {
            finalizer(nullptr, 0);
        }
        // clear delta after writing
        this->delta = 0;
    }
    virtual void applyDelta(uint8_t const* const pdat) {
        this->value += *reinterpret_cast<const int*>(pdat);
    }
    static std::unique_ptr<IntegerWithDelta> create(mutils::DeserializationManager* dm) {
        return std::make_unique<IntegerWithDelta>();
    }

    virtual const std::string to_string() const {
        return std::to_string(this->value);
    };

    DEFAULT_SERIALIZATION_SUPPORT(IntegerWithDelta, value);
};

class MixedPersistenceObject : public mutils::ByteRepresentable,
                               public derecho::PersistsFields,
                               public derecho::GroupReference {
    using derecho::GroupReference::group;

    // A persistent field with delta support
    persistent::Persistent<IntegerWithDelta> pers_int;
    // A persistent field without delta support
    persistent::Persistent<std::string> pers_string;
    // A non-persistent field, a map from string to int
    std::map<std::string, std::int32_t> nonpers_map;

public:
    /** Initial constructor, takes the PersistentRegistry argument */
    MixedPersistenceObject(persistent::PersistentRegistry* registry)
            : pers_int(std::make_unique<IntegerWithDelta>, "PersistentIntWithDelta", registry),
              pers_string(std::make_unique<std::string>, "PersistentString", registry) {}
    /** Deserialization constructor */
    MixedPersistenceObject(persistent::Persistent<IntegerWithDelta>& the_int,
                           persistent::Persistent<std::string>& the_string,
                           std::map<std::string, std::int32_t>& the_map)
            : pers_int(std::move(the_int)),
              pers_string(std::move(the_string)),
              nonpers_map(std::move(the_map)) {}

    /** Adds an int value to the Persistent<IntegerWithDelta> and returns the version associated with it */
    persistent::version_t ordered_put_int(std::int32_t int_val);

    /** Ordered-callable function that changes non-persistent replicated state. Puts a (key, value) entry in the map. */
    void add_map_entry(const std::string& key, std::int32_t value) {
        dbg_default_debug("add_map_entry called with ({}, {})", key, value);
        nonpers_map[key] = value;
    }

    DEFAULT_SERIALIZATION_SUPPORT(MixedPersistenceObject, pers_int, pers_string, nonpers_map);
    REGISTER_RPC_FUNCTIONS(MixedPersistenceObject, ORDERED_TARGETS(ordered_put_int, add_map_entry));
};

persistent::version_t MixedPersistenceObject::ordered_put_int(std::int32_t int_val) {
    dbg_default_debug("ordered_put_int called with {}", int_val);
    auto version_and_timestamp = this->group->get_subgroup<MixedPersistenceObject>(this->subgroup_index).get_current_version();
    pers_int->add(int_val);
    return std::get<0>(version_and_timestamp);
}

/*
 * Command-line arguments:
 * 1. Total number of nodes in the test
 * 2. Number of shards to configure
 * 3. Whether this node is a sender (1 for true, 0 for false)
 */
int main(int argc, char** argv) {
    pthread_setname_np(pthread_self(), "mixperstest");
    std::mt19937 random_generator(getpid());
    const int num_args = 3;
    const uint32_t num_nodes = std::stoi(argv[argc - num_args]);
    const uint32_t num_shards = std::stoi(argv[argc - num_args + 1]);
    const bool is_sender = std::stoi(argv[argc - num_args + 2]);
    const uint32_t nodes_per_shard = num_nodes / num_shards;
    const uint32_t num_persistent_updates = 100;

    derecho::Conf::initialize(argc, argv);

    derecho::SubgroupInfo subgroup_layout(derecho::DefaultSubgroupAllocator(
            {{std::type_index(typeid(MixedPersistenceObject)),
              // Using fixed shards means there won't be any view changes, so the Persistent versions will be readable
              derecho::one_subgroup_policy(derecho::fixed_even_shards(num_shards, nodes_per_shard))}}));

    derecho::Group<MixedPersistenceObject> group(subgroup_layout,
                                                 [](persistent::PersistentRegistry* registry, derecho::subgroup_id_t subgroup_id) {
                                                     return std::make_unique<MixedPersistenceObject>(registry);
                                                 });

    // Send some persistent updates, then send a non-persistent update
    if(is_sender) {
        derecho::Replicated<MixedPersistenceObject>& subgroup_handle = group.get_subgroup<MixedPersistenceObject>();
        std::cout << "Sending " << num_persistent_updates << " calls to ordered_put" << std::endl;
        std::vector<derecho::QueryResults<persistent::version_t>> results_list;
        for(uint32_t counter = 0; counter < num_persistent_updates; ++counter) {
            // Ensure the put method is called with a nonzero value, so IntegerWithDelta records a delta
            results_list.emplace_back(subgroup_handle.ordered_send<RPC_NAME(ordered_put_int)>(counter * 10 + 5));
        }
        std::cout << "Waiting for last ordered_put to complete" << std::endl;
        persistent::version_t last_version;
        for(auto& reply_pair : results_list.back().get()) {
            last_version = reply_pair.second.get();
        }
        std::cout << "Last ordered_put got version " << last_version << ". Press enter to call to add_map_entry." << std::endl;
        std::cin.get();
        subgroup_handle.ordered_send<RPC_NAME(add_map_entry)>(
                std::to_string(derecho::getConfUInt32(derecho::Conf::DERECHO_LOCAL_ID)) + "_last_version", last_version);
    }
    std::cout << "Press enter when finished with test." << std::endl;
    std::cin.get();
    group.barrier_sync();
    group.leave(true);
}