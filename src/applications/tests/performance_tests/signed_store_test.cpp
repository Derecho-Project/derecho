#include <chrono>
#include <cstring>
#include <future>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>

#include <derecho/core/derecho.hpp>
#include <derecho/openssl/hash.hpp>
#include <spdlog/fmt/bin_to_hex.h>

#include "log_results.hpp"
#include "signed_store_test.hpp"

/* ---------------- Blob implementation ---------------- */
Blob::Blob(const char* const b, const decltype(size) s)
        : is_temporary(false), bytes(nullptr), size(0) {
    if(s > 0) {
        bytes = new char[s];
        if(b != nullptr) {
            memcpy(bytes, b, s);
        } else {
            memset(bytes, 0, s);
        }
        size = s;
    }
}

//Dangerous: copy the pointer to buffer and share ownership with it, even though these are raw pointers
Blob::Blob(char* buffer, std::size_t size, bool temporary)
        : is_temporary(true), bytes(buffer), size(size) {}

Blob::Blob(const Blob& other) : bytes(nullptr), size(0) {
    if(other.size > 0) {
        bytes = new char[other.size];
        memcpy(bytes, other.bytes, other.size);
        size = other.size;
    }
}

Blob::Blob(Blob&& other) : bytes(other.bytes), size(other.size) {
    other.bytes = nullptr;
    other.size = 0;
}

Blob::Blob() : bytes(nullptr), size(0) {}

Blob::~Blob() {
    if(bytes && !is_temporary) delete[] bytes;
}

Blob& Blob::operator=(Blob&& other) {
    char* swp_bytes = other.bytes;
    std::size_t swp_size = other.size;
    other.bytes = bytes;
    other.size = size;
    bytes = swp_bytes;
    size = swp_size;
    return *this;
}

Blob& Blob::operator=(const Blob& other) {
    if(bytes != nullptr && !is_temporary) {
        delete bytes;
    }
    size = other.size;
    if(size > 0) {
        bytes = new char[size];
        memcpy(bytes, other.bytes, size);
    } else {
        bytes = nullptr;
    }
    return *this;
}

std::size_t Blob::to_bytes(char* v) const {
    ((std::size_t*)(v))[0] = size;
    if(size > 0) {
        memcpy(v + sizeof(size), bytes, size);
    }
    return size + sizeof(size);
}

std::size_t Blob::bytes_size() const {
    return size + sizeof(size);
}

void Blob::post_object(const std::function<void(char const* const, std::size_t)>& f) const {
    f((char*)&size, sizeof(size));
    f(bytes, size);
}

std::unique_ptr<Blob> Blob::from_bytes(mutils::DeserializationManager*, const char* const buffer) {
    return std::make_unique<Blob>(buffer + sizeof(std::size_t), ((std::size_t*)(buffer))[0]);
}

mutils::context_ptr<Blob> Blob::from_bytes_noalloc(mutils::DeserializationManager* ctx, const char* const buffer) {
    //Wrap the buffer in a Blob, whose "bytes" pointer actually points to the buffer
    return mutils::context_ptr<Blob>{new Blob((char*)buffer + sizeof(std::size_t), ((std::size_t*)(buffer))[0], true)};
}

mutils::context_ptr<const Blob> Blob::from_bytes_noalloc_const(mutils::DeserializationManager* m, const char* const buffer) {
    return mutils::context_ptr<const Blob>{new Blob((char*)buffer + sizeof(std::size_t), ((std::size_t*)(buffer))[0], true)};
}

/* ----------------- CompletionTracker implementation ---------------- */
void CompletionTracker::start_tracking_version(persistent::version_t version) {
    //Create a new promise-future pair and insert it into the maps
    std::promise<void> finished_promise;
    std::future<void> finished_future = finished_promise.get_future();
    {
        std::unique_lock<std::mutex> lock(promise_map_mutex);
        version_finished_promises.emplace(version, std::move(finished_promise));
    }
    {
        std::unique_lock<std::mutex> lock(future_map_mutex);
        version_finished_futures.emplace(version, std::move(finished_future));
    }
}

void CompletionTracker::notify_version_finished(persistent::version_t version) {
    //Fullfill the promise for this version, then delete it from the map
    std::unique_lock<std::mutex> lock(promise_map_mutex);
    auto promise_in_map = version_finished_promises.find(version);
    promise_in_map->second.set_value();
    version_finished_promises.erase(promise_in_map);
}

void CompletionTracker::await_version_finished(persistent::version_t version) {
    //Pull the future out of the map, so we don't need to hold the map lock for as long
    std::future<void> version_finished_future;
    {
        std::unique_lock<std::mutex> lock(future_map_mutex);
        auto future_in_map = version_finished_futures.find(version);
        version_finished_future = std::move(future_in_map->second);
        version_finished_futures.erase(future_in_map);
    }
    //This blocks until the future is fulfilled
    version_finished_future.get();
}

void CompletionTracker::set_subgroup_id(derecho::subgroup_id_t id) {
    my_subgroup_id = id;
}

derecho::subgroup_id_t CompletionTracker::get_subgroup_id() {
    return my_subgroup_id;
}

/* ------------------- ClientTier implementation ------------------- */

ClientTier::ClientTier(std::size_t test_data_size,
                       std::shared_ptr<std::chrono::steady_clock::time_point> persistence_finished_time,
                       std::shared_ptr<std::chrono::steady_clock::time_point> signature_finished_time)
        : hasher(openssl::DigestAlgorithm::SHA256),
          persistence_finished_time(persistence_finished_time),
          signature_finished_time(signature_finished_time),
          test_data(nullptr, test_data_size){};

std::tuple<persistent::version_t, uint64_t, std::vector<unsigned char>> ClientTier::submit_update(const Blob& data) {
    derecho::ExternalCaller<ObjectStore>& storage_subgroup = group->template get_nonmember_subgroup<ObjectStore>();
    derecho::ExternalCaller<SignatureStore>& signature_subgroup = group->template get_nonmember_subgroup<SignatureStore>();
    std::vector<std::vector<node_id_t>> storage_members = group->get_subgroup_members<ObjectStore>();
    std::vector<std::vector<node_id_t>> signature_members = group->get_subgroup_members<SignatureStore>();
    //For now, always use the first-ranked member as the proxy to contact with the P2P send
    node_id_t storage_member_to_contact = storage_members[0][0];
    node_id_t signature_member_to_contact = signature_members[0][0];
    //Send the new data to the storage subgroup
    auto storage_query_results = storage_subgroup.p2p_send<RPC_NAME(update)>(storage_member_to_contact, data);
    //Meanwhile, start hashing the update (this might take a long time)
    SHA256Hash update_hash;
    hasher.init();
    hasher.add_bytes(data.bytes, data.size);
    //Wait for the storage query to complete and return the assigned version and timestamp (which must get hashed)
    dbg_default_debug("Waiting for storage query to complete");
    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = storage_query_results.get().get(storage_member_to_contact);
    hasher.add_bytes(&std::get<0>(version_and_timestamp), sizeof(persistent::version_t));
    hasher.add_bytes(&std::get<1>(version_and_timestamp), sizeof(uint64_t));
    hasher.finalize(update_hash.data());
    //When the hash is complete, send it to the signature subgroup
    dbg_default_debug("Hashing complete, sending hash to node {}", signature_member_to_contact);
    auto signature_query_results = signature_subgroup.p2p_send<RPC_NAME(add_hash)>(signature_member_to_contact, update_hash);
    //Now wait for persistence and verification stability
    dbg_default_debug("Querying node {} to await persistence of version {}", storage_member_to_contact, std::get<0>(version_and_timestamp));
    auto persistence_query_results = storage_subgroup.p2p_send<RPC_NAME(await_persistence)>(
            storage_member_to_contact, std::get<0>(version_and_timestamp));
    persistence_query_results.get().get(storage_member_to_contact);
    dbg_default_debug("Waiting for hash query to complete");
    std::vector<unsigned char> signature_reply = signature_query_results.get().get(signature_member_to_contact);
    return {std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp), signature_reply};
}

bool ClientTier::update_batch_test(const int& num_updates) {
    using derecho::rpc::QueryResults;
    using persistent::version_t;
    using namespace std::chrono;
    derecho::ExternalCaller<ObjectStore>& storage_subgroup = group->template get_nonmember_subgroup<ObjectStore>();
    derecho::ExternalCaller<SignatureStore>& signature_subgroup = group->template get_nonmember_subgroup<SignatureStore>();
    const std::vector<std::vector<node_id_t>> storage_members = group->get_subgroup_members<ObjectStore>();
    const std::vector<std::vector<node_id_t>> signature_members = group->get_subgroup_members<SignatureStore>();
    const node_id_t storage_member_to_contact = storage_members[0][0];
    const node_id_t signature_member_to_contact = signature_members[0][0];
    std::vector<QueryResults<std::tuple<version_t, uint64_t>>> storage_query_results;
    std::vector<QueryResults<bool>> persistence_query_results;
    std::vector<QueryResults<std::vector<unsigned char>>> signature_query_results;
    //First, launch all the object store update queries
    dbg_default_debug("Submitting batch of updates to storage servers");
    for(int counter = 0; counter < num_updates; ++counter) {
        storage_query_results.emplace_back(storage_subgroup.p2p_send<RPC_NAME(update)>(
                storage_member_to_contact, test_data));
    }
    dbg_default_debug("Starting to hash all the updates");
    auto hash_start_time = std::chrono::steady_clock::now();
    SHA256Hash update_hash;
    //Pretend to hash the whole batch of updates, though really they're all the same
    for(int counter = 0; counter < num_updates; ++counter) {
        hasher.hash_bytes(test_data.bytes, test_data.size, update_hash.data());
    }
    auto hash_end_time = std::chrono::steady_clock::now();
    //Now launch all the hash/signature queries
    dbg_default_debug("Submitting hashes to signature servers");
    for(int counter = 0; counter < num_updates; ++counter) {
        signature_query_results.emplace_back(signature_subgroup.p2p_send<RPC_NAME(add_hash)>(
                signature_member_to_contact, update_hash));
    }

    //Wait for all of the updates to be done persisting. Hopefully this happens first, and takes longer than all the hashes.
    for(int counter = 0; counter < num_updates; ++counter) {
        //Wait for the RPC call that submits the update to finish
        dbg_default_debug("Waiting for version to be returned from RPC call #{}", counter);
        std::tuple<version_t, uint64_t> version_and_timestamp = storage_query_results[counter].get().get(storage_member_to_contact);
        //Make an RPC call that waits for persistence
        dbg_default_debug("Sending RPC call to await persistence on {}", std::get<0>(version_and_timestamp));
        persistence_query_results.emplace_back(storage_subgroup.p2p_send<RPC_NAME(await_persistence)>(
                storage_member_to_contact, std::get<0>(version_and_timestamp)));
    }
    dbg_default_debug("Awaiting persistence on all updates");
    //Wait for all the await-persistence calls to return; this waits for the slowest update to finish persisting
    for(int counter = 0; counter < num_updates; ++counter) {
        persistence_query_results[counter].get().get(storage_member_to_contact);
    }
    *persistence_finished_time = steady_clock::now();

    dbg_default_debug("Awaiting signatures on all updates");
    //Wait for all of the updates to be done signing. Hopefully this happens last, and takes longest.
    for(int counter = 0; counter < num_updates; ++counter) {
        std::vector<unsigned char> signature_reply = signature_query_results[counter].get().get(signature_member_to_contact);
    }
    *signature_finished_time = steady_clock::now();
    int64_t hash_nanosec = duration_cast<nanoseconds>(hash_end_time - hash_start_time).count();

    std::cout << "Hashing took a total of " << static_cast<double>(hash_nanosec) / 1000000 << " ms" << std::endl;

    return true;
}

/* ---------------- SignatureStore implementation --------------------- */

SignatureStore::SignatureStore(persistent::PersistentRegistry* pr, std::shared_ptr<CompletionTracker> tracker)
        : hashes(std::make_unique<SHA256Hash>, "SignedHashLog", pr, true), verified_tracker(tracker) {}

std::vector<unsigned char> SignatureStore::add_hash(const SHA256Hash& hash) {
    derecho::Replicated<SignatureStore>& this_subgroup = group->get_subgroup<SignatureStore>(this->subgroup_index);
    auto query_results = this_subgroup.ordered_send<RPC_NAME(ordered_add_hash)>(hash);
    auto& replies = query_results.get();
    std::vector<unsigned char> signature(hashes.getSignatureSize());
    persistent::version_t hash_log_version;
    for(auto& reply_pair : replies) {
        hash_log_version = reply_pair.second.get();
    }
    verified_tracker->await_version_finished(hash_log_version);
    persistent::version_t previous_signed_version;
    hashes.getSignature(hash_log_version, signature.data(), previous_signed_version);
    return signature;
}

persistent::version_t SignatureStore::ordered_add_hash(const SHA256Hash& hash) {
    derecho::Replicated<SignatureStore>& this_subgroup = group->get_subgroup<SignatureStore>(this->subgroup_index);
    //Ask the Replicated interface what version it's about to generate
    std::tuple<persistent::version_t, uint64_t> next_version = this_subgroup.get_next_version();
    verified_tracker->start_tracking_version(std::get<0>(next_version));
    //Append the new hash to the Persistent log, thus generating a version
    *hashes = hash;
    return std::get<0>(next_version);
}

void SignatureStore::end_test() {
    std::cout << "Received the end_test message, shutting down" << std::endl;
    exit(EXIT_SUCCESS);
}

/* ----------------- ObjectStore implementation ------------------------ */

ObjectStore::ObjectStore(persistent::PersistentRegistry* pr, std::shared_ptr<CompletionTracker> tracker)
        : object_log(std::make_unique<Blob>, "BlobLog", pr, false),
          persistence_tracker(tracker) {}

std::tuple<persistent::version_t, uint64_t> ObjectStore::update(const Blob& new_data) {
    derecho::Replicated<ObjectStore>& this_subgroup = group->get_subgroup<ObjectStore>(this->subgroup_index);
    auto query_results = this_subgroup.ordered_send<RPC_NAME(ordered_update)>(new_data);
    auto& replies = query_results.get();
    std::tuple<persistent::version_t, uint64_t> version_and_timestamp;
    for(auto& reply_pair : replies) {
        version_and_timestamp = reply_pair.second.get();
    }
    dbg_default_debug("Returning ({}, {}) from update", std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));
    return version_and_timestamp;
}

std::tuple<persistent::version_t, uint64_t> ObjectStore::ordered_update(const Blob& new_data) {
    derecho::Replicated<ObjectStore>& this_subgroup = group->get_subgroup<ObjectStore>(this->subgroup_index);
    //Ask the Replicated interface what version it's about to generate
    std::tuple<persistent::version_t, uint64_t> next_version = this_subgroup.get_next_version();
    persistence_tracker->start_tracking_version(std::get<0>(next_version));
    //Update the Persistent object, generating a new version
    *object_log = new_data;
    dbg_default_debug("Returning ({},{}) from ordered_update", std::get<0>(next_version), std::get<1>(next_version));
    return next_version;
}

bool ObjectStore::await_persistence(const persistent::version_t& version) {
    dbg_default_debug("Awaiting persistence on version {}", version);
    persistence_tracker->await_version_finished(version);
    return true;
}

Blob ObjectStore::get(const persistent::version_t& version) {
    return *object_log[version];
}

Blob ObjectStore::get_latest() {
    return *object_log;
}

void ObjectStore::end_test() {
    std::cout << "Recieved the end_test message, shutting down" << std::endl;
    exit(EXIT_SUCCESS);
}

/* -------------------------------------------------------------------- */

//Determines whether a node ID is a member of any shard in a list of shards
bool member_of_shards(node_id_t node_id, const std::vector<std::vector<node_id_t>>& shard_member_lists) {
    for(const auto& shard_members : shard_member_lists) {
        if(std::find(shard_members.begin(), shard_members.end(), node_id) != shard_members.end()) {
            return true;
        }
    }
    return false;
}

struct signed_store_results {
    int num_client_nodes;
    int update_size;
    int num_updates;
    double persisted_bw;
    double signed_bw;

    void print(std::ofstream& fout) {
        fout << num_client_nodes << " " << update_size << " "
             << num_updates << " " << persisted_bw << " "
             << signed_bw << std::endl;
    }
};

/**
 * Command line arguments: [num_client_nodes] [num_storage_nodes] [num_signature_nodes] [num_updates]
 * num_client_nodes: Number of nodes that will be assigned the "ClientTier" role. Each node will be
 *                   assigned to its own shard of size 1, so that they all have independent state
 * num_storage_nodes: Number of nodes that will be assigned to the "ObjectStore" role. They will all
 *                    be assigned to a single shard containing all the members
 * num_signature_nodes: Number of nodes that will be assigned to the "SignatureStore" role. These will
 *                      also be assigned to one big shard.
 * num_updates: The number of fake "client updates" the ClientTier nodes should generate and submit to
 *              the ObjectStore and SignatureStore nodes. Each update will be a byte blob of the maximum
 *              possible size that can fit in an RPC payload (given the configured max_payload_size)
 */
int main(int argc, char** argv) {
    using namespace std::chrono;
    //Parse command line arguments
    const int num_args = 4;
    const unsigned int num_client_nodes = std::stoi(argv[argc - num_args]);
    const unsigned int num_storage_nodes = std::stoi(argv[argc - num_args + 1]);
    const unsigned int num_signature_nodes = std::stoi(argv[argc - num_args + 2]);
    const unsigned int num_updates = std::stoi(argv[argc - 1]);
    derecho::Conf::initialize(argc, argv);
    const std::size_t rpc_header_size = sizeof(std::size_t) + sizeof(std::size_t)
                                        + derecho::remote_invocation_utilities::header_space();
    const std::size_t update_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE) - rpc_header_size;
    steady_clock::time_point begin_time, send_complete_time, persist_complete_time, verify_complete_time;

    //Set up the persistence-complete tracker for the ObjectStore subgroup
    std::shared_ptr<CompletionTracker> objectstore_persisted_tracker = std::make_shared<CompletionTracker>();
    auto global_persistence_callback = [&](derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
        if(subgroup_id == objectstore_persisted_tracker->get_subgroup_id()) {
            objectstore_persisted_tracker->notify_version_finished(version);
        }
    };
    auto object_subgroup_factory = [&](persistent::PersistentRegistry* registry, derecho::subgroup_id_t subgroup_id) {
        objectstore_persisted_tracker->set_subgroup_id(subgroup_id);
        return std::make_unique<ObjectStore>(registry, objectstore_persisted_tracker);
    };
    //Set up the verification-finished tracker for the SignatureStore subgroup
    std::shared_ptr<CompletionTracker> verified_tracker = std::make_shared<CompletionTracker>();
    auto global_verification_callback = [&](derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
        if(subgroup_id == verified_tracker->get_subgroup_id()) {
            verified_tracker->notify_version_finished(version);
        }
    };
    auto signature_subgroup_factory = [&](persistent::PersistentRegistry* registry, derecho::subgroup_id_t subgroup_id) {
        verified_tracker->set_subgroup_id(subgroup_id);
        return std::make_unique<SignatureStore>(registry, verified_tracker);
    };

    //Subgroup and shard layout
    derecho::SubgroupInfo subgroup_layout(derecho::DefaultSubgroupAllocator(
            {{std::type_index(typeid(ClientTier)),
              derecho::one_subgroup_policy(derecho::fixed_even_shards(num_client_nodes, 1))},
             {std::type_index(typeid(ObjectStore)),
              derecho::one_subgroup_policy(derecho::fixed_even_shards(1, num_storage_nodes))},
             {std::type_index(typeid(SignatureStore)),
              derecho::one_subgroup_policy(derecho::fixed_even_shards(1, num_signature_nodes))}}));

    std::shared_ptr<steady_clock::time_point> persistence_finished_time = std::make_shared<steady_clock::time_point>();
    std::shared_ptr<steady_clock::time_point> signature_finished_time = std::make_shared<steady_clock::time_point>();
    auto client_node_factory = [&](persistent::PersistentRegistry* pr, derecho::subgroup_id_t id) {
        return std::make_unique<ClientTier>(update_size, persistence_finished_time, signature_finished_time);
    };
    //Set up and join the group
    derecho::Group<ClientTier, ObjectStore, SignatureStore> group(
            {nullptr, nullptr, global_persistence_callback, global_verification_callback},
            subgroup_layout,
            {}, {},
            client_node_factory,
            object_subgroup_factory,
            signature_subgroup_factory);

    //Figure out which subgroup this node got assigned to
    uint32_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    std::vector<node_id_t> storage_members = group.get_subgroup_members<ObjectStore>(0)[0];
    std::vector<node_id_t> signature_members = group.get_subgroup_members<SignatureStore>(0)[0];
    std::vector<std::vector<node_id_t>> client_tier_shards = group.get_subgroup_members<ClientTier>(0);
    if(member_of_shards(my_id, client_tier_shards)) {
        std::cout << "Assigned the ClientTier role" << std::endl;
        derecho::Replicated<ClientTier>& this_subgroup = group.get_subgroup<ClientTier>();
        derecho::ExternalCaller<ObjectStore>& object_store_subgroup = group.get_nonmember_subgroup<ObjectStore>();
        derecho::ExternalCaller<SignatureStore>& signature_store_subgroup = group.get_nonmember_subgroup<SignatureStore>();
        begin_time = steady_clock::now();
        auto self_query_results = this_subgroup.p2p_send<RPC_NAME(update_batch_test)>(my_id, num_updates);
        self_query_results.get().get(my_id);
        //Compute throughputs based on overall times for the batch
        int64_t persistence_complete_nanosec = duration_cast<nanoseconds>(*persistence_finished_time - begin_time).count();
        int64_t signature_complete_nanosec = duration_cast<nanoseconds>(*signature_finished_time - begin_time).count();
        //Should I multiply by the number of client-tier nodes here?
        double persistence_thpt_gbs = (static_cast<double>(num_updates) * update_size)
                                      / persistence_complete_nanosec;
        double signed_thpt_gbs = (static_cast<double>(num_updates) * update_size)
                                 / signature_complete_nanosec;
        std::cout << "Total persistence timespan: " << static_cast<double>(persistence_complete_nanosec) / 1000000 << "ms" << std::endl;
        std::cout << "Persistence throughput: " << persistence_thpt_gbs << " GB/s" << std::endl;

        std::cout << "Total signatures timespan: " << static_cast<double>(signature_complete_nanosec) / 1000000 << "ms" << std::endl;
        std::cout << "Signature-stable throughput: " << signed_thpt_gbs << " GB/s" << std::endl;
        log_results(signed_store_results{(int)client_tier_shards.size(), update_size, num_updates,
                                         persistence_thpt_gbs, signed_thpt_gbs},
                    "data_signed_store_test");
        //Unfortunately, there's no good way to tell the other nodes to exit gracefully with a P2P message
        //So just send them one that makes them exit abruptly, then exit myself.
        for(node_id_t nid : storage_members) {
            object_store_subgroup.p2p_send<RPC_NAME(end_test)>(nid);
        }
        for(node_id_t nid : signature_members) {
            signature_store_subgroup.p2p_send<RPC_NAME(end_test)>(nid);
        }

    } else if(std::find(signature_members.begin(), signature_members.end(), my_id) != signature_members.end()) {
        std::cout << "Assigned the SignatureStore role." << std::endl;
        std::cout << "Waiting to be terminated by the client tier." << std::endl;
        while(true) {
        }
    } else if(std::find(storage_members.begin(), storage_members.end(), my_id) != storage_members.end()) {
        std::cout << "Assigned the ObjectStore role." << std::endl;
        std::cout << "Waiting to be terminated by the client tier." << std::endl;
        while(true) {
        }
    } else {
        std::cout << "Not assigned to any role (?!)" << std::endl;
    }
}