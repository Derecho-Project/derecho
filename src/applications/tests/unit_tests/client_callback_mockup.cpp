#include <cstring>
#include <random>

#include "client_callback_mockup.hpp"

/* ---------------- Blob implementation ---------------- */
Blob::Blob(const uint8_t* const b, const decltype(size) s)
        : is_temporary(false), bytes(nullptr), size(0) {
    if(s > 0) {
        bytes = new uint8_t[s];
        if(b != nullptr) {
            memcpy(bytes, b, s);
        } else {
            memset(bytes, 0, s);
        }
        size = s;
    }
}

//Dangerous: copy the pointer to buffer and share ownership with it, even though these are raw pointers
Blob::Blob(uint8_t* buffer, std::size_t size, bool temporary)
        : is_temporary(true), bytes(buffer), size(size) {}

Blob::Blob(const Blob& other) : bytes(nullptr), size(0) {
    if(other.size > 0) {
        bytes = new uint8_t[other.size];
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
    uint8_t* swp_bytes = other.bytes;
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
        bytes = new uint8_t[size];
        memcpy(bytes, other.bytes, size);
    } else {
        bytes = nullptr;
    }
    return *this;
}

std::size_t Blob::to_bytes(uint8_t* v) const {
    ((std::size_t*)(v))[0] = size;
    if(size > 0) {
        memcpy(v + sizeof(size), bytes, size);
    }
    return size + sizeof(size);
}

std::size_t Blob::bytes_size() const {
    return size + sizeof(size);
}

void Blob::post_object(const std::function<void(uint8_t const* const, std::size_t)>& f) const {
    f((uint8_t*)&size, sizeof(size));
    f(bytes, size);
}

std::unique_ptr<Blob> Blob::from_bytes(mutils::DeserializationManager*, const uint8_t* const buffer) {
    return std::make_unique<Blob>(buffer + sizeof(std::size_t), ((std::size_t*)(buffer))[0]);
}

mutils::context_ptr<Blob> Blob::from_bytes_noalloc(mutils::DeserializationManager* ctx, const uint8_t* const buffer) {
    //Wrap the buffer in a Blob, whose "bytes" pointer actually points to the buffer
    return mutils::context_ptr<Blob>{new Blob((uint8_t*)buffer + sizeof(std::size_t), ((std::size_t*)(buffer))[0], true)};
}

mutils::context_ptr<const Blob> Blob::from_bytes_noalloc_const(mutils::DeserializationManager* m, const uint8_t* const buffer) {
    return mutils::context_ptr<const Blob>{new Blob((uint8_t*)buffer + sizeof(std::size_t), ((std::size_t*)(buffer))[0], true)};
}

std::ostream& operator<<(std::ostream& os, const ClientCallbackType& cb_type) {
    switch(cb_type) {
        case ClientCallbackType::GLOBAL_PERSISTENCE:
            os << "GLOBAL_PERSISTENCE";
            break;
        case ClientCallbackType::LOCAL_PERSISTENCE:
            os << "LOCAL_PERSISTENCE";
            break;
        case ClientCallbackType::SIGNATURE_VERIFICATION:
            os << "SIGNATURE_VERIFICATION";
        default:
            os << "UNKNOWN";
    }
    return os;
}

/* ------------------- ClientNode implementation ------------------- */

InternalClientNode::InternalClientNode(const derecho::persistence_callback_t& global_persistence_callback)
        : global_persistence_callback(global_persistence_callback),
          thread_shutdown(false) {
    client_callbacks_thread = std::thread([this]() { callback_thread_function(); });
};
InternalClientNode::~InternalClientNode() {
    thread_shutdown = true;
    event_queue_nonempty.notify_all();
}

std::pair<persistent::version_t, uint64_t> InternalClientNode::submit_update(uint32_t update_counter,
                                                                             const Blob& new_data) const {
    derecho::PeerCaller<StorageNode>& storage_subgroup = group->template get_nonmember_subgroup<StorageNode>();
    std::vector<std::vector<node_id_t>> storage_members = group->get_subgroup_members<StorageNode>();

    const node_id_t storage_node_to_contact = storage_members[0][0];
    node_id_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);

    //Submit the update to the chosen storage node
    dbg_default_debug("Submitting update number {} to node {}", update_counter, storage_node_to_contact);
    auto storage_query_results = storage_subgroup.p2p_send<RPC_NAME(update)>(storage_node_to_contact,
                                                                             my_id, update_counter, new_data);
    //Wait for the response so we learn the update's version number
    std::pair<persistent::version_t, uint64_t> version_and_timestamp = storage_query_results.get().get(storage_node_to_contact);
    //Send another message requesting a callback for this version number
    dbg_default_debug("Sending a request for callback to node {}, for version {}", storage_node_to_contact, version_and_timestamp.first);
    auto callback_query_results = storage_subgroup.p2p_send<RPC_NAME(register_callback)>(
            storage_node_to_contact, my_id, ClientCallbackType::GLOBAL_PERSISTENCE, version_and_timestamp.first);
    //Wait for it to be sent before returning (the method is void)
    callback_query_results.get();
    dbg_default_debug("Returning ({},{}) from submit_update", version_and_timestamp.first, version_and_timestamp.second);
    return version_and_timestamp;
}

void InternalClientNode::receive_callback(const ClientCallbackType& callback_type, persistent::version_t version, derecho::subgroup_id_t sending_subgroup) const {
    dbg_default_debug("Got a callback of type {} for version {}", callback_type, version);
    //Send the callback event to the callback thread then return, so the P2P receive thread doesn't block for too long
    std::unique_lock<std::mutex> lock(event_queue_mutex);
    callback_event_queue.emplace(CallbackEvent{callback_type, version, sending_subgroup});
    event_queue_nonempty.notify_all();
}

void InternalClientNode::callback_thread_function() {
    pthread_setname_np(pthread_self(), "client_callback");
    while(!thread_shutdown) {
        CallbackEvent next_event;
        {
            std::unique_lock<std::mutex> queue_lock(event_queue_mutex);
            event_queue_nonempty.wait(queue_lock, [this]() {
                return !callback_event_queue.empty() || thread_shutdown;
            });
            if(thread_shutdown) {
                break;
            }
            next_event = callback_event_queue.front();
            dbg_default_debug("Callback thread got a callback event for subgroup {}, version {}", next_event.sending_subgroup, next_event.version);
            callback_event_queue.pop();
        }
        if(next_event.callback_type == ClientCallbackType::GLOBAL_PERSISTENCE && global_persistence_callback) {
            dbg_default_debug("Callback thread invoking global_persistence_callback");
            global_persistence_callback(next_event.sending_subgroup, next_event.version);
        }
        //Other event types not implemented yet
    }
}

/* ----------------- StorageNode implementation ------------------------ */

StorageNode::StorageNode(persistent::PersistentRegistry* pr, derecho::subgroup_id_t my_subgroup_id)
        : object_log(std::make_unique<Blob>, "BlobLog", pr, false),
          my_subgroup_id(my_subgroup_id),
          thread_shutdown(false) {
    callback_sending_thread = std::thread([this]() { callback_thread_function(); });
}

StorageNode::StorageNode(persistent::Persistent<Blob>& other_log, derecho::subgroup_id_t subgroup_id)
        : object_log(std::move(other_log)),
          my_subgroup_id(subgroup_id),
          thread_shutdown(false) {
    callback_sending_thread = std::thread([this]() { callback_thread_function(); });
}

StorageNode::~StorageNode() {
    thread_shutdown = true;
    request_queue_nonempty.notify_all();
}

std::pair<persistent::version_t, uint64_t> StorageNode::update(node_id_t sender_id,
                                                               uint32_t update_counter,
                                                               const Blob& new_data) const {
    dbg_default_debug("Received an update call from node {} for update {}", sender_id, update_counter);
    derecho::Replicated<StorageNode>& this_subgroup = group->get_subgroup<StorageNode>(this->subgroup_index);
    auto query_results = this_subgroup.ordered_send<RPC_NAME(ordered_update)>(new_data);
    std::pair<persistent::version_t, uint64_t> version_and_timestamp = query_results.get_persistent_version();
    {
        std::unique_lock<std::mutex> results_map_lock(callback_thread_mutex);
        update_results.emplace(version_and_timestamp.first, std::move(query_results));
    }
    dbg_default_debug("Returning ({}, {}) from update", version_and_timestamp.first, version_and_timestamp.second);
    return version_and_timestamp;
}

void StorageNode::ordered_update(const Blob& new_data) {
    dbg_default_debug("Received an ordered_update call");
    *object_log = new_data;
}

Blob StorageNode::get(const persistent::version_t& version) const {
    return *object_log[version];
}

void StorageNode::register_callback(node_id_t client_node_id,
                                    const ClientCallbackType& callback_type,
                                    persistent::version_t version) const {
    dbg_default_debug("Received a call to register_callback from node {} for version {}", client_node_id, version);
    std::unique_lock<std::mutex> request_queue_lock(callback_thread_mutex);
    callback_request_queue.emplace(CallbackRequest{callback_type, client_node_id, version});
    request_queue_nonempty.notify_all();
}

void StorageNode::callback_thread_function() {
    pthread_setname_np(pthread_self(), "server_callback");
    //Thread-local list of pending callback requests
    std::map<persistent::version_t, CallbackRequest> requests_by_version;
    //Thread-local list of QueryResults that are related to callback requests
    std::map<persistent::version_t, derecho::rpc::QueryResults<void>> queryresults_for_requests;
    while(!thread_shutdown) {
        //In general, callback requests will arrive in increasing version order, since
        //clients will usually request a callback for a version they recently submitted.
        //Thus, if we already have some requests in the map, we should check them first
        //before waiting for new requests
        if(requests_by_version.empty()) {
            CallbackRequest next_request;
            {
                std::unique_lock<std::mutex> queue_lock(callback_thread_mutex);
                request_queue_nonempty.wait(queue_lock, [this]() {
                    return !callback_request_queue.empty() || thread_shutdown;
                });
                if(thread_shutdown) {
                    break;
                }
                next_request = callback_request_queue.front();
                dbg_default_debug("Callback thread got a request from {} for version {}", next_request.client, next_request.version);
                callback_request_queue.pop();
            }
            requests_by_version.emplace(next_request.version, next_request);
        }
        //Check the futures for each requested version in increasing order, but don't block
        for(auto requests_iter = requests_by_version.begin();
            requests_iter != requests_by_version.end();) {
            persistent::version_t requested_version = requests_iter->first;
            dbg_default_debug("Callback thread checking a callback of type {} for version {}", requests_iter->second.callback_type, requested_version);
            bool requested_event_happened = false;
            {
                std::unique_lock<std::mutex> query_results_lock(callback_thread_mutex);
                auto result_search = update_results.find(requested_version);
                //The QueryResults might not be in update_results if we previously moved it
                //to queryresults_for_requests (and now there's a second callback for the same version)
                if(result_search != update_results.end()) {
                    dbg_default_debug("Checking for stability events for version {}", requested_version);
                    //If the result has already reached the requested callback state, immediately consume it
                    if(requests_iter->second.callback_type == ClientCallbackType::LOCAL_PERSISTENCE
                       && result_search->second.local_persistence_is_ready()) {
                        result_search->second.await_local_persistence();
                        requested_event_happened = true;
                    } else if(requests_iter->second.callback_type == ClientCallbackType::GLOBAL_PERSISTENCE
                              && result_search->second.global_persistence_is_ready()) {
                        result_search->second.await_global_persistence();
                        requested_event_happened = true;
                    } else if(requests_iter->second.callback_type == ClientCallbackType::SIGNATURE_VERIFICATION
                              && result_search->second.global_verification_is_ready()) {
                        result_search->second.await_signature_verification();
                        requested_event_happened = true;
                    }
                } else {
                    auto cached_result_search = queryresults_for_requests.find(requested_version);
                    if(cached_result_search != queryresults_for_requests.end()) {
                        dbg_default_debug("Checking for stability events in thread-local cache for version {}", requested_version);
                        if(requests_iter->second.callback_type == ClientCallbackType::LOCAL_PERSISTENCE
                           && cached_result_search->second.local_persistence_is_ready()) {
                            cached_result_search->second.await_local_persistence();
                            requested_event_happened = true;
                        } else if(requests_iter->second.callback_type == ClientCallbackType::GLOBAL_PERSISTENCE
                                  && cached_result_search->second.global_persistence_is_ready()) {
                            cached_result_search->second.await_global_persistence();
                            requested_event_happened = true;
                        } else if(requests_iter->second.callback_type == ClientCallbackType::SIGNATURE_VERIFICATION
                                  && cached_result_search->second.global_verification_is_ready()) {
                            cached_result_search->second.await_signature_verification();
                            requested_event_happened = true;
                        }
                    }
                }
            }
            if(requested_event_happened) {
                //Send a callback to the client
                dbg_default_debug("Callback thread sending a callback to node {} for version {}", requests_iter->second.client, requested_version);
                derecho::PeerCaller<InternalClientNode>& client_subgroup = group->template get_nonmember_subgroup<InternalClientNode>();
                auto p2p_send_results = client_subgroup.p2p_send<RPC_NAME(receive_callback)>(
                        requests_iter->second.client, requests_iter->second.callback_type,
                        requested_version, my_subgroup_id);
                dbg_default_debug("Temporary: Waiting for callback P2P message to complete");
                p2p_send_results.get();
                //Remove the request from the list
                requests_iter = requests_by_version.erase(requests_iter);
            } else {
                requests_iter++;
            }
        }
        //If this cleared the requests queue, go back and wait for another one
        if(requests_by_version.empty()) {
            continue;
        }
        //If there are still requests to handle, block on the lowest-numbered one,
        //which will probably complete first. Take its corresponding QueryResults out of
        //the map that is shared with the RPC thread to avoid deadlock.
        {
            std::unique_lock<std::mutex> query_results_lock(callback_thread_mutex);
            auto result_search = update_results.find(requests_by_version.begin()->first);
            queryresults_for_requests.emplace(std::move(*result_search));
            update_results.erase(result_search);
        }
        switch(requests_by_version.begin()->second.callback_type) {
            case ClientCallbackType::LOCAL_PERSISTENCE:
                dbg_default_debug("Callback thread awaiting local persistence for version {}", requests_by_version.begin()->first);
                queryresults_for_requests.at(requests_by_version.begin()->first).await_local_persistence();
                break;
            case ClientCallbackType::GLOBAL_PERSISTENCE:
                dbg_default_debug("Callback thread awaiting global persistence for version {}", requests_by_version.begin()->first);
                queryresults_for_requests.at(requests_by_version.begin()->first).await_global_persistence();
                break;
            case ClientCallbackType::SIGNATURE_VERIFICATION:
                dbg_default_debug("Callback thread awaiting signature verification for version {}", requests_by_version.begin()->first);
                queryresults_for_requests.at(requests_by_version.begin()->first).await_signature_verification();
                break;
        }
        //Send a callback to the client
        derecho::PeerCaller<InternalClientNode>& client_subgroup = group->template get_nonmember_subgroup<InternalClientNode>();
        dbg_default_debug("Callback thread sending a callback to node {} for version {}", requests_by_version.begin()->second.client, requests_by_version.begin()->first);
        auto p2p_send_results = client_subgroup.p2p_send<RPC_NAME(receive_callback)>(
                requests_by_version.begin()->second.client, requests_by_version.begin()->second.callback_type,
                requests_by_version.begin()->first, my_subgroup_id);
        dbg_default_debug("Temporary: Waiting for callback P2P message to complete");
        p2p_send_results.get();
        //Remove the request from the list
        requests_by_version.erase(requests_by_version.begin());
    }
}

//Determines whether a node ID is a member of any shard in a list of shards
bool member_of_shards(node_id_t node_id, const std::vector<std::vector<node_id_t>>& shard_member_lists) {
    for(const auto& shard_members : shard_member_lists) {
        if(std::find(shard_members.begin(), shard_members.end(), node_id) != shard_members.end()) {
            return true;
        }
    }
    return false;
}

/**
 * Command line arguments: [num_client_nodes] [num_storage_nodes] [num_updates]
 * num_client_nodes: Number of nodes that will be assigned the "ClientNode" role. Each node will be
 *                   assigned to its own shard of size 1, so that they all have independent state
 * num_storage_nodes: Number of nodes that will be assigned to the "StorageNode" role. They will all
 *                    be assigned to a single shard containing all the members
 * num_updates: The number of fake "client updates" the ClientTier nodes should generate and submit to
 *              the ObjectStore and SignatureStore nodes. Each update will be a byte blob of the maximum
 *              possible size that can fit in an RPC payload (given the configured max_payload_size)
 */
int main(int argc, char** argv) {
    pthread_setname_np(pthread_self(), "main");
    //Parse command line arguments
    const int num_args = 3;
    const unsigned int num_client_nodes = std::stoi(argv[argc - num_args]);
    const unsigned int num_storage_nodes = std::stoi(argv[argc - num_args + 1]);
    const unsigned int num_updates = std::stoi(argv[argc - 1]);
    derecho::Conf::initialize(argc, argv);
    const std::size_t rpc_header_size = sizeof(std::size_t) + sizeof(std::size_t)
                                        + derecho::remote_invocation_utilities::header_space();
    //An update plus the two other parameters must fit in the available payload size
    const std::size_t update_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE)
                                    - rpc_header_size - sizeof(node_id_t) - sizeof(uint32_t);
    //For generating random updates
    const std::string characters("abcdefghijklmnopqrstuvwxyz");
    std::mt19937 random_generator(getpid());
    std::uniform_int_distribution<std::size_t> char_distribution(0, characters.size() - 1);

    auto storage_subgroup_factory = [&](persistent::PersistentRegistry* registry, derecho::subgroup_id_t subgroup_id) {
        return std::make_unique<StorageNode>(registry, subgroup_id);
    };

    auto client_callback_function = [](derecho::subgroup_id_t subgroup_id, persistent::version_t version) {
        std::cout << "Got a client-side callback for global persistence of version "
                  << version << " from subgroup " << subgroup_id << std::endl;
    };

    auto client_subgroup_factory = [&](persistent::PersistentRegistry* registry, derecho::subgroup_id_t subgroup_id) {
        return std::make_unique<InternalClientNode>(client_callback_function);
    };

    //Subgroup and shard layout
    derecho::SubgroupInfo subgroup_layout(derecho::DefaultSubgroupAllocator(
            {{std::type_index(typeid(InternalClientNode)),
              derecho::one_subgroup_policy(derecho::fixed_even_shards(num_client_nodes, 1))},
             {std::type_index(typeid(StorageNode)),
              derecho::one_subgroup_policy(derecho::fixed_even_shards(1, num_storage_nodes))}}));

    //Set up and join the group
    derecho::Group<InternalClientNode, StorageNode> group(
            {nullptr, nullptr, nullptr, nullptr},
            subgroup_layout,
            {}, {},
            client_subgroup_factory,
            storage_subgroup_factory);

    //Figure out which subgroup this node got assigned to
    uint32_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    std::vector<node_id_t> storage_members = group.get_subgroup_members<StorageNode>(0)[0];
    std::vector<std::vector<node_id_t>> client_tier_shards = group.get_subgroup_members<InternalClientNode>(0);
    if(member_of_shards(my_id, client_tier_shards)) {
        std::cout << "Assigned the ClientNode role" << std::endl;
        //Send some updates to the storage nodes and request callbacks when they have globally persisted
        Blob test_update(nullptr, update_size);
        derecho::Replicated<InternalClientNode>& this_subgroup = group.get_subgroup<InternalClientNode>();
        for(unsigned counter = 0; counter < num_updates; ++counter) {
            std::generate(&test_update.bytes[0], &test_update.bytes[test_update.size], [&]() {
                return characters[char_distribution(random_generator)];
            });
            std::cout << "Submitting update " << counter << std::endl;
            //P2P send to myself
            auto query_result = this_subgroup.p2p_send<RPC_NAME(submit_update)>(my_id, counter, test_update);
            //Block and wait for the results
            std::pair<persistent::version_t, uint64_t> version_and_timestamp = query_result.get().get(my_id);

            std::cout << "Update " << counter << " submitted. Result: Version = " << version_and_timestamp.first
                      << " timestamp = " << version_and_timestamp.second << std::endl;
        }
    } else if(std::find(storage_members.begin(), storage_members.end(), my_id) != storage_members.end()) {
        std::cout << "Assigned the StorageNode role." << std::endl;
        std::cout << "Press enter when finished with test." << std::endl;
        std::cin.get();
    } else {
        std::cout << "Not assigned to any role (?!)" << std::endl;
        std::cin.get();
    }
    group.barrier_sync();
    group.leave();
}
