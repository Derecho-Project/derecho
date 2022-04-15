#include "persistence_notification_test.hpp"

#include <random>

/* ------------------------ StorageNode implementation ------------------------ */

StorageNode::StorageNode(persistent::PersistentRegistry* pr, derecho::subgroup_id_t my_subgroup_id)
        : object_log(std::make_unique<derecho::Bytes>, "BytesLog", pr, false),
          my_subgroup_id(my_subgroup_id),
          thread_shutdown(false) {
    notification_sending_thread = std::thread([this]() { notification_thread_function(); });
}

StorageNode::StorageNode(persistent::Persistent<derecho::Bytes>& other_log, derecho::subgroup_id_t subgroup_id)
        : object_log(std::move(other_log)),
          my_subgroup_id(subgroup_id),
          thread_shutdown(false) {
    notification_sending_thread = std::thread([this]() { notification_thread_function(); });
}

StorageNode::~StorageNode() {
    thread_shutdown = true;
    request_queue_nonempty.notify_all();
}

std::pair<persistent::version_t, uint64_t> StorageNode::update(uint32_t update_counter,
                                                               const derecho::Bytes& new_data) const {
    dbg_default_debug("Received a P2P update call for update {}", update_counter);
    derecho::Replicated<StorageNode>& this_subgroup = group->get_subgroup<StorageNode>(this->subgroup_index);
    auto query_results = this_subgroup.ordered_send<RPC_NAME(ordered_update)>(new_data);
    std::pair<persistent::version_t, uint64_t> version_and_timestamp = query_results.get_persistent_version();
    {
        std::unique_lock<std::mutex> results_map_lock(notification_thread_mutex);
        update_results.emplace(version_and_timestamp.first, std::move(query_results));
    }
    dbg_default_debug("Returning ({}, {}) from update", version_and_timestamp.first, version_and_timestamp.second);
    return version_and_timestamp;
}

void StorageNode::ordered_update(const derecho::Bytes& new_data) {
    dbg_default_debug("Received an ordered_update call");
    *object_log = new_data;
}

derecho::Bytes StorageNode::get(const persistent::version_t& version) const {
    return *object_log[version];
}

void StorageNode::request_notification(node_id_t client_node_id,
                                       NotificationMessageType notification_type,
                                       persistent::version_t version) const {
    dbg_default_debug("Received a call to request_notification from node {} for version {}", client_node_id, version);
    std::unique_lock<std::mutex> request_queue_lock(notification_thread_mutex);
    notification_request_queue.emplace(NotificationRequest{notification_type, client_node_id, version});
    request_queue_nonempty.notify_all();
}

void StorageNode::notification_thread_function() {
    pthread_setname_np(pthread_self(), "notif_check");
    //Thread-local list of pending notification requests
    std::map<persistent::version_t, NotificationRequest> requests_by_version;
    //Thread-local list of QueryResults that are related to notification requests
    std::map<persistent::version_t, derecho::rpc::QueryResults<void>> queryresults_for_requests;
    while(!thread_shutdown) {
        //In general, notification requests will arrive in increasing version order, since
        //clients will usually request a notification for a version they recently submitted.
        //Thus, if we already have some requests in the map, we should check them first
        //before waiting for new requests
        if(requests_by_version.empty()) {
            NotificationRequest next_request;
            {
                std::unique_lock<std::mutex> queue_lock(notification_thread_mutex);
                request_queue_nonempty.wait(queue_lock, [this]() {
                    return !notification_request_queue.empty() || thread_shutdown;
                });
                if(thread_shutdown) {
                    break;
                }
                next_request = notification_request_queue.front();
                dbg_default_debug("Notification thread got a request from {} for version {}", next_request.client_id, next_request.version);
                notification_request_queue.pop();
            }
            requests_by_version.emplace(next_request.version, next_request);
        }
        //Check the futures for each requested version in increasing order, but don't block
        for(auto requests_iter = requests_by_version.begin();
            requests_iter != requests_by_version.end();) {
            persistent::version_t requested_version = requests_iter->first;
            dbg_default_debug("Notification thread checking a notification of type {} for version {}", requests_iter->second.notification_type, requested_version);
            bool requested_event_happened = false;
            {
                std::unique_lock<std::mutex> query_results_lock(notification_thread_mutex);
                auto result_search = update_results.find(requested_version);
                //The QueryResults might not be in update_results if we previously moved it
                //to queryresults_for_requests (and now there's a second notification for the same version)
                if(result_search != update_results.end()) {
                    dbg_default_debug("Checking for stability events for version {}", requested_version);
                    //If the result has already reached the requested notification state, immediately consume it
                    if(requests_iter->second.notification_type == NotificationMessageType::LOCAL_PERSISTENCE
                       && result_search->second.local_persistence_is_ready()) {
                        result_search->second.await_local_persistence();
                        requested_event_happened = true;
                    } else if(requests_iter->second.notification_type == NotificationMessageType::GLOBAL_PERSISTENCE
                              && result_search->second.global_persistence_is_ready()) {
                        result_search->second.await_global_persistence();
                        requested_event_happened = true;
                    }
                } else {
                    auto cached_result_search = queryresults_for_requests.find(requested_version);
                    if(cached_result_search != queryresults_for_requests.end()) {
                        dbg_default_debug("Checking for stability events in thread-local cache for version {}", requested_version);
                        if(requests_iter->second.notification_type == NotificationMessageType::LOCAL_PERSISTENCE
                           && cached_result_search->second.local_persistence_is_ready()) {
                            cached_result_search->second.await_local_persistence();
                            requested_event_happened = true;
                        } else if(requests_iter->second.notification_type == NotificationMessageType::GLOBAL_PERSISTENCE
                                  && cached_result_search->second.global_persistence_is_ready()) {
                            cached_result_search->second.await_global_persistence();
                            requested_event_happened = true;
                        }
                    }
                }
            }
            if(requested_event_happened) {
                //Send a notification to the client
                dbg_default_debug("Notification thread sending a notification to node {} for version {}", requests_iter->second.client_id, requested_version);
                //Message body format: Object version the notification is about, subgroup ID of the sending node
                std::size_t message_size = mutils::bytes_size(requested_version) + mutils::bytes_size(my_subgroup_id);
                derecho::NotificationMessage message(requests_iter->second.notification_type, message_size);
                std::size_t body_offset = 0;
                body_offset += mutils::to_bytes(requested_version, message.body + body_offset);
                body_offset += mutils::to_bytes(my_subgroup_id, message.body + body_offset);
                derecho::ExternalClientCallback<StorageNode>& client_callback = group->template get_client_callback<StorageNode>(subgroup_index);
                //Do we need to wait for the p2p_send to complete here, to avoid message going out of scope while p2p_send is still using it?
                client_callback.p2p_send<RPC_NAME(notify)>(requests_iter->second.client_id, message);
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
            std::unique_lock<std::mutex> query_results_lock(notification_thread_mutex);
            auto result_search = update_results.find(requests_by_version.begin()->first);
            if(result_search != update_results.end()) {
                queryresults_for_requests.emplace(std::move(*result_search));
                update_results.erase(result_search);
            } else {
                dbg_default_warn("Notification thread could not find a QueryResults for version {}", requests_by_version.begin()->first);
            }
        }
        switch(requests_by_version.begin()->second.notification_type) {
            case NotificationMessageType::LOCAL_PERSISTENCE:
                dbg_default_debug("notification thread awaiting local persistence for version {}", requests_by_version.begin()->first);
                queryresults_for_requests.at(requests_by_version.begin()->first).await_local_persistence();
                break;
            case NotificationMessageType::GLOBAL_PERSISTENCE:
                dbg_default_debug("notification thread awaiting global persistence for version {}", requests_by_version.begin()->first);
                queryresults_for_requests.at(requests_by_version.begin()->first).await_global_persistence();
                break;
        }
        //Send a notification to the client
        derecho::ExternalClientCallback<StorageNode>& client_callback = group->template get_client_callback<StorageNode>(subgroup_index);
        dbg_default_debug("notification thread sending a notification to node {} for version {}", requests_by_version.begin()->second.client_id, requests_by_version.begin()->first);
        std::size_t message_size = mutils::bytes_size(requests_by_version.begin()->first) + mutils::bytes_size(my_subgroup_id);
        derecho::NotificationMessage message(requests_by_version.begin()->second.notification_type, message_size);
        std::size_t body_offset = 0;
        body_offset += mutils::to_bytes(requests_by_version.begin()->first, message.body + body_offset);
        body_offset += mutils::to_bytes(my_subgroup_id, message.body + body_offset);
        client_callback.p2p_send<RPC_NAME(notify)>(requests_by_version.begin()->second.client_id, message);
        //Remove the request from the list
        requests_by_version.erase(requests_by_version.begin());
    }
}

/* ------------------------ End of StorageNode implementation ------------------------ */


/**
 * This test creates a group with a single StorageNode subgroup, then has an external client
 * submit several updates and request notifications for when they are persisted.
 *
 * Command line arguments [external_node_id] [num_storage_nodes] [num_updates]
 * external_node_id: The node ID of the machine that should act as the external client.
 *                   This node will not join the group, and will submit updates instead.
 * num_storage_nodes: The number of nodes that should be in the StorageNode subgroup. The
 *                    group will wait until this many nodes join before starting up.
 * num_updates: The number of randomly-generated updates the external client should submit.
 *              Each update will be a byte blob of the maximum possible size that can fit in
 *              an RPC payload possible size that can fit in an RPC payload
 */
int main(int argc, char** argv) {
    pthread_setname_np(pthread_self(), "main");
    //Parse command line arguments
    const int num_args = 3;
    const unsigned int external_node_id = std::stoi(argv[argc - num_args]);
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

    auto client_callback_function = [](const derecho::NotificationMessage& message) {
        std::size_t body_offset = 0;
        auto version = mutils::from_bytes<persistent::version_t>(nullptr, message.body + body_offset);
        body_offset += mutils::bytes_size(*version);
        auto subgroup_id = mutils::from_bytes<derecho::subgroup_id_t>(nullptr,message.body + body_offset);
        if(message.message_type == NotificationMessageType::GLOBAL_PERSISTENCE) {
            std::cout << "Got a client-side callback for global persistence of version "
                      << *version << " from subgroup " << *subgroup_id << std::endl;
        } else if(message.message_type == NotificationMessageType::LOCAL_PERSISTENCE) {
            std::cout << "Got a client-side callback for local persistence of version "
                      << *version << " from subgroup " << *subgroup_id << std::endl;
        } else {
            std::cout << "Got a client-side callback with an unknown message type: "
                      << message.message_type << std::endl;
        }
    };

    //Subgroup and shard layout
    derecho::SubgroupInfo subgroup_layout(derecho::DefaultSubgroupAllocator(
            {{std::type_index(typeid(StorageNode)),
              derecho::one_subgroup_policy(derecho::fixed_even_shards(1, num_storage_nodes))}}));

    uint32_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    if(external_node_id != my_id) {
        //Set up and join the group
        derecho::Group<StorageNode> group(subgroup_layout, storage_subgroup_factory);
        std::cout << "Assigned the StorageNode role." << std::endl;
        std::cout << "Press enter when finished with test." << std::endl;
        std::cin.get();
        group.barrier_sync();
        group.leave();
    } else {
        std::cout << "Acting as an external client" << std::endl;
        auto dummy_storage_factory = []() { return std::make_unique<StorageNode>(nullptr, 0); };
        derecho::ExternalGroupClient<StorageNode> client(dummy_storage_factory);
        derecho::ExternalClientCaller<StorageNode, decltype(client)>& storage_caller = client.get_subgroup_caller<StorageNode>(0);
        //Since we know there is only 1 subgroup and 1 shard of StorageNode, getting the node IDs is easy
        std::vector<node_id_t> storage_node_ids = client.get_shard_members<StorageNode>(0, 0);
        //Pick a node to contact to send updates
        node_id_t update_node = storage_node_ids[0];
        //Right now, we must request notifications from that same node, because only the P2P
        //update method can record QueryResults; other replicas have no way of tracking their updates
        node_id_t notification_node = update_node;
        //Register the client callback handler
        storage_caller.add_p2p_connection(notification_node);
        storage_caller.register_notification_handler(client_callback_function);
        //Send some updates to the storage nodes and request callbacks when they have globally persisted
        derecho::Bytes test_update(update_size);
        for(unsigned counter = 0; counter < num_updates; ++counter) {
            std::generate(&test_update.get()[0], &test_update.get()[test_update.size()], [&]() {
                return characters[char_distribution(random_generator)];
            });
            std::cout << "Submitting update " << counter << " to node " << update_node << std::endl;
            auto query_result = storage_caller.p2p_send<RPC_NAME(update)>(update_node, counter, test_update);
            //Wait for the response so we learn the version number
            std::pair<persistent::version_t, uint64_t> version_and_timestamp = query_result.get().get(update_node);
            std::cout << "Update " << counter << " submitted. Result: Version = " << version_and_timestamp.first
                      << " timestamp = " << version_and_timestamp.second << std::endl;
            //Request a callback for this version number
            auto callback_query_result = storage_caller.p2p_send<RPC_NAME(request_notification)>(
                    notification_node, my_id, NotificationMessageType::GLOBAL_PERSISTENCE, version_and_timestamp.first);
            std::cout << "Requested a callback from node " << notification_node << " for version " << version_and_timestamp.first << std::endl;
        }
        std::cout << "Done sending all updates" << std::endl;
        std::cout << "Press enter when finished with test." << std::endl;
        std::cin.get();
    }
}
