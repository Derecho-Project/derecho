/**
 * @file ViewManager.cpp
 *
 * @date Feb 6, 2017
 */

#include <arpa/inet.h>

#include "derecho_exception.h"
#include "persistence.h"
#include "view_manager.h"

using namespace derecho::persistence;

namespace derecho {

using lock_guard_t = std::lock_guard<std::mutex>;
using unique_lock_t = std::unique_lock<std::mutex>;
using shared_lock_t = std::shared_lock<std::shared_timed_mutex>;

ViewManager::ViewManager(const node_id_t my_id,
                         const ip_addr my_ip,
                         CallbackSet callbacks,
                         const SubgroupInfo& subgroup_info,
                         const DerechoParams& derecho_params,
                         const post_persistence_request_func_t & post_persistence_request,
                         std::vector<view_upcall_t> _view_upcalls,
                         const int gms_port)
        : logger(spdlog::get("debug_log")),
          gms_port(gms_port),
          curr_view(std::make_unique<View>(0, std::vector<node_id_t>{0}, std::vector<ip_addr>{my_ip}, std::vector<char>{0}, 0, std::vector<node_id_t>{}, std::vector<node_id_t>{}, 1, 0)),
          last_suspected(2),  //The initial view always has 2 members
          server_socket(gms_port),
          thread_shutdown(false),
          view_upcalls(_view_upcalls),
          subgroup_info(subgroup_info),
          derecho_params(derecho_params),
          post_persistence_request_func(post_persistence_request) {
    initialize_rdmc_sst();
    //Wait for the first client (second group member) to join
    await_second_member(my_id);

    if(!derecho_params.filename.empty()) {
        view_file_name = std::string(derecho_params.filename + persistence::PAXOS_STATE_EXTENSION);
        std::string params_file_name(derecho_params.filename + persistence::PARAMATERS_EXTENSION);
        persist_object(*curr_view, view_file_name);
        persist_object(derecho_params, params_file_name);
    }

    logger->debug("Initializing SST and RDMC for the first time.");
    construct_multicast_group(callbacks, derecho_params);
}

ViewManager::ViewManager(const node_id_t my_id,
                         tcp::socket& leader_connection,
                         CallbackSet callbacks,
                         const SubgroupInfo& subgroup_info,
                         const post_persistence_request_func_t & post_persistence_request,
                         std::vector<view_upcall_t> _view_upcalls,
                         const int gms_port)
        : logger(spdlog::get("debug_log")),
          gms_port(gms_port),
          server_socket(gms_port),
          thread_shutdown(false),
          view_upcalls(_view_upcalls),
          subgroup_info(subgroup_info),
          derecho_params(0, 0),
          post_persistence_request_func(post_persistence_request) {
    //First, receive the view and parameters over the given socket
    receive_configuration(my_id, leader_connection);

    //Set this while we still know my_id
    curr_view->my_rank = curr_view->rank_of(my_id);

    last_suspected = std::vector<bool>(curr_view->members.size());

    initialize_rdmc_sst();
    if(!derecho_params.filename.empty()) {
        view_file_name = std::string(derecho_params.filename + persistence::PAXOS_STATE_EXTENSION);
        std::string params_file_name(derecho_params.filename + persistence::PARAMATERS_EXTENSION);
        persist_object(*curr_view, view_file_name);
        persist_object(derecho_params, params_file_name);
    }
    logger->debug("Initializing SST and RDMC for the first time.");

    construct_multicast_group(callbacks, derecho_params);
    curr_view->gmsSST->vid[curr_view->my_rank] = curr_view->vid;
}

ViewManager::ViewManager(const std::string& recovery_filename,
                         const node_id_t my_id,
                         const ip_addr my_ip,
                         CallbackSet callbacks,
                         const SubgroupInfo& subgroup_info,
                         const post_persistence_request_func_t & post_persistence_request,
                         std::experimental::optional<DerechoParams> _derecho_params,
                         std::vector<view_upcall_t> _view_upcalls,
                         const int gms_port)
        : logger(spdlog::get("debug_log")),
          gms_port(gms_port),
          server_socket(gms_port),
          thread_shutdown(false),
          view_file_name(recovery_filename + persistence::PAXOS_STATE_EXTENSION),
          view_upcalls(_view_upcalls),
          subgroup_info(subgroup_info),
          derecho_params(0, 0),
          post_persistence_request_func(post_persistence_request) {
    auto last_view = load_view(view_file_name);

    if(my_id != last_view->members[last_view->rank_of_leader()]) {
        tcp::socket leader_socket(last_view->member_ips[last_view->rank_of_leader()], gms_port);
        receive_configuration(my_id, leader_socket);
        //derecho_params will be initialized by the existing view's leader
    } else {
        /* This should only happen if an entire group failed and the leader is restarting;
         * otherwise the view obtained from the recovery script will have a leader that is
         * not me. So reset to an empty view and wait for the first non-leader member to
         * restart and join. */
        curr_view = std::make_unique<View>(last_view->vid + 1,
                                           std::vector<node_id_t>{my_id},
                                           std::vector<ip_addr>{my_ip},
                                           std::vector<char>{0});
        initialize_rdmc_sst();
        if(_derecho_params) {
            derecho_params = _derecho_params.value();
        } else {
            derecho_params = *(load_object<DerechoParams>(
                    std::string(recovery_filename + persistence::PARAMATERS_EXTENSION)));
        }

        await_second_member(my_id);
    }
    curr_view->my_rank = curr_view->rank_of(my_id);
    last_suspected = std::vector<bool>(curr_view->members.size());

    //since the View just changed, and we're definitely in persistent mode, persist it again
    view_file_name = std::string(derecho_params.filename + persistence::PAXOS_STATE_EXTENSION);
    std::string params_file_name(derecho_params.filename + persistence::PARAMATERS_EXTENSION);
    persist_object(*curr_view, view_file_name);
    persist_object(derecho_params, params_file_name);

    logger->debug("Initializing SST and RDMC for the first time.");
    construct_multicast_group(callbacks, derecho_params);
    curr_view->gmsSST->vid[curr_view->my_rank] = curr_view->vid;
}

ViewManager::~ViewManager() {
    thread_shutdown = true;
    // force accept to return.
    tcp::socket s{"localhost", gms_port};
    if(client_listener_thread.joinable()) {
        client_listener_thread.join();
    }
    old_views_cv.notify_all();
    if(old_view_cleanup_thread.joinable()) {
        old_view_cleanup_thread.join();
    }
}

/* ----------  1. Constructor Components ------------- */

void ViewManager::receive_configuration(node_id_t my_id, tcp::socket& leader_connection) {
    logger->debug("Successfully connected to leader, about to receive the View.");
    node_id_t leader_id = 0;
    leader_connection.exchange(my_id, leader_id);

    //The leader will first send the size of the necessary buffer, then the serialized View
    std::size_t size_of_view;
    bool success = leader_connection.read((char*)&size_of_view, sizeof(size_of_view));
    assert(success);
    char buffer[size_of_view];
    success = leader_connection.read(buffer, size_of_view);
    assert(success);
    curr_view = mutils::from_bytes<View>(nullptr, buffer);
    //The leader will first send the size of the necessary buffer, then the serialized DerechoParams
    std::size_t size_of_derecho_params;
    success = leader_connection.read((char*)&size_of_derecho_params, sizeof(size_of_derecho_params));
    char buffer2[size_of_derecho_params];
    success = leader_connection.read(buffer2, size_of_derecho_params);
    assert(success);
    std::unique_ptr<DerechoParams> params_ptr = mutils::from_bytes<DerechoParams>(nullptr, buffer2);
    derecho_params = *params_ptr;
}

void ViewManager::start() {
    curr_view->gmsSST->put();
    curr_view->gmsSST->sync_with_members();
    logger->debug("Done setting up initial SST and RDMC");

    if(curr_view->vid != 0) {
        // If this node is joining an existing group with a non-initial view, copy the leader's num_changes, num_acked, and num_committed
        // Otherwise, you'll immediately think that there's a new proposed view change because gmsSST.num_changes[leader] > num_acked[my_rank]
        curr_view->gmsSST->init_local_change_proposals(curr_view->rank_of_leader());
        curr_view->gmsSST->put();
        logger->debug("Joining node initialized its SST row from the leader");
    }

    create_threads();
    register_predicates();
    curr_view->gmsSST->start_predicate_evaluation();
    logger->debug("Starting predicate evaluation");

    shared_lock_t lock(view_mutex);
    for(auto& view_upcall : view_upcalls) {
        view_upcall(*curr_view);
    }
}

void ViewManager::await_second_member(const node_id_t my_id) {
    tcp::socket client_socket = server_socket.accept();
    node_id_t joiner_id = 0;
    client_socket.exchange(my_id, joiner_id);
    ip_addr& joiner_ip = client_socket.remote_ip;
    ip_addr my_ip = client_socket.get_self_ip();
    curr_view = std::make_unique<View>(0,
                                       std::vector<node_id_t>{my_id, joiner_id},
                                       std::vector<ip_addr>{my_ip, joiner_ip},
                                       std::vector<char>{0, 0},
                                       std::vector<node_id_t>{joiner_id});
    auto bind_socket_write = [&client_socket](const char* bytes, std::size_t size) {
        bool success = client_socket.write(bytes, size);
        assert(success);
    };

    std::size_t size_of_view = mutils::bytes_size(*curr_view);
    client_socket.write((char*)&size_of_view, sizeof(size_of_view));
    mutils::post_object(bind_socket_write, *curr_view);
    std::size_t size_of_derecho_params = mutils::bytes_size(derecho_params);
    client_socket.write((char*)&size_of_derecho_params, sizeof(size_of_derecho_params));
    mutils::post_object(bind_socket_write, derecho_params);
    //Send a "0" as the size of the "old shard leaders" vector, since there are no old leaders
    mutils::post_object(bind_socket_write, std::size_t{0});
    rdma::impl::verbs_add_connection(joiner_id, joiner_ip, my_id);
    sst::add_node(joiner_id, joiner_ip);
}

void ViewManager::initialize_rdmc_sst() {
    // construct member_ips
    auto member_ips_map = make_member_ips_map(*curr_view);
    if(!rdmc::initialize(member_ips_map, curr_view->members[curr_view->my_rank])) {
        std::cout << "Global setup failed" << std::endl;
        exit(0);
    }
    sst::verbs_initialize(member_ips_map, curr_view->members[curr_view->my_rank]);
}

std::map<node_id_t, ip_addr> ViewManager::make_member_ips_map(const View& view) {
    std::map<node_id_t, ip_addr> member_ips_map;
    size_t num_members = view.members.size();
    for(uint i = 0; i < num_members; ++i) {
        if(!view.failed[i]) {
            member_ips_map[view.members[i]] = view.member_ips[i];
        }
    }
    return member_ips_map;
}

void ViewManager::create_threads() {
    client_listener_thread = std::thread{[this]() {
        pthread_setname_np(pthread_self(), "client_thread");
        while(!thread_shutdown) {
            tcp::socket client_socket = server_socket.accept();
            logger->debug("Background thread got a client connection from {}", client_socket.remote_ip);
            pending_join_sockets.locked().access.emplace_back(std::move(client_socket));
        }
        std::cout << "Connection listener thread shutting down." << std::endl;
    }};

    old_view_cleanup_thread = std::thread([this]() {
        pthread_setname_np(pthread_self(), "old_view");
        while(!thread_shutdown) {
            unique_lock_t old_views_lock(old_views_mutex);
            old_views_cv.wait(old_views_lock, [this]() {
                return !old_views.empty() || thread_shutdown;
            });
            if(!thread_shutdown) {
                old_views.front().reset();
                old_views.pop();
            }
        }
        std::cout << "Old View cleanup thread shutting down." << std::endl;
    });
}

void ViewManager::register_predicates() {
    /* This predicate-trigger pair monitors the suspected[] array to detect failures
     * and, for the leader, proposes new views to exclude failed members */
    auto suspected_changed = [this](const DerechoSST& sst) {
        return suspected_not_equal(sst, last_suspected);
    };
    auto suspected_changed_trig = [this](DerechoSST& gmsSST) {
        logger->debug("Suspected[] changed");
        View& Vc = *curr_view;
        int myRank = curr_view->my_rank;
        // These fields had better be synchronized.
        assert(gmsSST.get_local_index() == curr_view->my_rank);
        // Aggregate suspicions into gmsSST[myRank].Suspected;
        for(int r = 0; r < Vc.num_members; r++) {
            for(int who = 0; who < Vc.num_members; who++) {
                gmssst::set(gmsSST.suspected[myRank][who], gmsSST.suspected[myRank][who] || gmsSST.suspected[r][who]);
            }
        }

        for(int q = 0; q < Vc.num_members; q++) {
            //If this is a new suspicion
            if(gmsSST.suspected[myRank][q] && !Vc.failed[q]) {
                //This is safer than copy_suspected, since suspected[] might change during this loop
                last_suspected[q] = gmsSST.suspected[myRank][q];
                logger->debug("Marking {} failed", Vc.members[q]);
                if(Vc.num_failed >= (Vc.num_members + 1) / 2) {
                    throw derecho_exception("Majority of a Derecho group simultaneously failed ... shutting down");
                }

                logger->debug("GMS telling SST to freeze row {} which is node {}", q, Vc.members[q]);
                gmsSST.freeze(q);  // Cease to accept new updates from q
                Vc.multicast_group->wedge();
                gmssst::set(gmsSST.wedged[myRank], true);  // RDMC has halted new sends and receives in theView
                Vc.failed[q] = true;
                Vc.num_failed++;

                if(Vc.num_failed >= (Vc.num_members + 1) / 2) {
                    throw derecho_exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
                }

                // push change to gmsSST.suspected[myRank]
                gmsSST.put(gmsSST.suspected.get_base() - gmsSST.getBaseAddress(), gmsSST.changes.get_base() - gmsSST.suspected.get_base());
                // push change to gmsSST.wedged[myRank]
                gmsSST.put(gmsSST.wedged.get_base() - gmsSST.getBaseAddress(), sizeof(gmsSST.wedged[0]));
                if(Vc.i_am_leader() && !changes_contains(gmsSST, Vc.members[q]))  // Leader initiated
                {
                    const int next_change_index = gmsSST.num_changes[myRank] - gmsSST.num_installed[myRank];
                    if(next_change_index == (int)gmsSST.changes.size()) {
                        throw derecho_exception("Ran out of room in the pending changes list");
                    }

                    gmssst::set(gmsSST.changes[myRank][next_change_index], Vc.members[q]);  // Reports the failure (note that q NotIn members)
                    gmssst::increment(gmsSST.num_changes[myRank]);
                    logger->debug("Leader proposed a change to remove failed node {}", Vc.members[q]);
                    gmsSST.put((char*)std::addressof(gmsSST.changes[0][next_change_index]) - gmsSST.getBaseAddress(),
                               sizeof(gmsSST.changes[0][next_change_index]));
                    gmsSST.put(gmsSST.num_changes.get_base() - gmsSST.getBaseAddress(), sizeof(gmsSST.num_changes[0]));
                }
            }
        }
    };

    /* This pair runs only on the leader and reacts to new client connections
     * by proposing a new view */
    auto start_join_pred = [this](const DerechoSST& sst) {
        return curr_view->i_am_leader() && has_pending_join();
    };
    auto start_join_trig = [this](DerechoSST& sst) {
        logger->debug("GMS handling a new client connection");
        //C++'s ugly two-step dequeue: leave queue.front() in an invalid state, then delete it
        proposed_join_sockets.emplace_back(std::move(pending_join_sockets.locked().access.front()));
        pending_join_sockets.locked().access.pop_front();
        receive_join(proposed_join_sockets.back());
    };

    /* These run only on the leader. They monitor the acks received from followers
     * and update the leader's nCommitted when all non-failed members have acked */
    auto change_commit_ready = [this](const DerechoSST& gmsSST) {
        return curr_view->i_am_leader()
               && min_acked(gmsSST, curr_view->failed) > gmsSST.num_committed[gmsSST.get_local_index()];
    };
    auto commit_change = [this](DerechoSST& gmsSST) {
        gmssst::set(gmsSST.num_committed[gmsSST.get_local_index()],
                    min_acked(gmsSST, curr_view->failed));  // Leader commits a new request
        logger->debug("Leader committing change proposal #{}", gmsSST.num_committed[gmsSST.get_local_index()]);
        gmsSST.put(gmsSST.num_committed.get_base() - gmsSST.getBaseAddress(), sizeof(gmsSST.num_committed[0]));
    };

    /* These are mostly intended for non-leaders, and update nAcked to acknowledge
     * a proposed change when the leader increments num_changes. Only one join can be
     * proposed at once, but multiple failures could be proposed and acknowledged. */
    auto leader_proposed_change = [this](const DerechoSST& gmsSST) {
        return gmsSST.num_changes[curr_view->rank_of_leader()]
               > gmsSST.num_acked[gmsSST.get_local_index()];
    };
    auto ack_proposed_change = [this](DerechoSST& gmsSST) {
        // These fields had better be synchronized.
        assert(gmsSST.get_local_index() == curr_view->my_rank);
        int myRank = gmsSST.get_local_index();
        int leader = curr_view->rank_of_leader();
        logger->debug("Detected that leader proposed change #{}. Acknowledging.", gmsSST.num_changes[leader]);
        if(myRank != leader) {
            // Echo the count
            gmssst::set(gmsSST.num_changes[myRank], gmsSST.num_changes[leader]);

            // Echo (copy) the vector including the new changes
            gmssst::set(gmsSST.changes[myRank], gmsSST.changes[leader], gmsSST.changes.size());
            // Echo the new member's IP
            gmssst::set(gmsSST.joiner_ips[myRank], gmsSST.joiner_ips[leader], gmsSST.joiner_ips.size());
            gmssst::set(gmsSST.num_committed[myRank], gmsSST.num_committed[leader]);
        }

        // Notice a new request, acknowledge it
        gmssst::set(gmsSST.num_acked[myRank], gmsSST.num_changes[myRank]);
        gmsSST.put(gmsSST.changes.get_base() - gmsSST.getBaseAddress(),
                   gmsSST.num_received.get_base() - gmsSST.changes.get_base());
        logger->debug("Wedging current view.");
        curr_view->wedge();
        logger->debug("Done wedging current view.");

    };

    /* This predicate detects when at least one new change has been committed by the leader.
     * The trigger starts the process of changing to a new view. */
    auto leader_committed_next_view = [this](const DerechoSST& gmsSST) {
        return gmsSST.num_committed[curr_view->rank_of_leader()]
               > gmsSST.num_installed[curr_view->my_rank];
    };
    auto start_view_change = [this](DerechoSST& gmsSST) {
        logger->debug("Starting view change to view {}", (curr_view->vid + 1));
        // Disable all the other SST predicates, except suspected_changed and the one I'm about to register
        gmsSST.predicates.remove(start_join_handle);
        gmsSST.predicates.remove(change_commit_ready_handle);
        gmsSST.predicates.remove(leader_proposed_handle);

        View& Vc = *curr_view;

        int myRank = curr_view->my_rank;
        // These fields had better be synchronized.
        assert(gmsSST.get_local_index() == curr_view->my_rank);

        Vc.wedge();
        std::set<int> leave_ranks;
        std::vector<int> join_indexes;
        //Look through pending changes up to num_committed and filter the joins and leaves
        const int committed_count = gmsSST.num_committed[Vc.rank_of_leader()]
                                    - gmsSST.num_installed[Vc.rank_of_leader()];
        for(int change_index = 0; change_index < committed_count; change_index++) {
            node_id_t change_id = gmsSST.changes[Vc.my_rank][change_index];
            int change_rank = Vc.rank_of(change_id);
            if(change_rank != -1) {
                //Might as well save the rank, since we'll need it again
                leave_ranks.emplace(change_rank);
            } else {
                join_indexes.emplace_back(change_index);
            }
        }

        int next_num_members = Vc.num_members - leave_ranks.size()
                               + join_indexes.size();
        //Initialize the next view
        std::vector<node_id_t> joined, members(next_num_members), departed;
        std::vector<char> failed(next_num_members);
        std::vector<ip_addr> member_ips(next_num_members);
        int next_unassigned_rank = curr_view->next_unassigned_rank;
        for(std::size_t i = 0; i < join_indexes.size(); ++i) {
            const int join_index = join_indexes[i];
            node_id_t joiner_id = gmsSST.changes[myRank][join_index];
            struct in_addr joiner_ip_packed;
            joiner_ip_packed.s_addr = gmsSST.joiner_ips[myRank][join_index];
            char* joiner_ip_cstr = inet_ntoa(joiner_ip_packed);
            std::string joiner_ip(joiner_ip_cstr);

            joined.emplace_back(joiner_id);
            //New members go at the end of the members list, but it may shrink in the new view
            int new_member_rank = Vc.num_members - leave_ranks.size() + i;
            members[new_member_rank] = joiner_id;
            member_ips[new_member_rank] = joiner_ip;
            logger->debug("Next view will add new member with ID {}", joiner_id);
        }
        for(const auto& leaver_rank : leave_ranks) {
            departed.emplace_back(Vc.members[leaver_rank]);
            //Decrement next_unassigned_rank for every failure, unless the failure wasn't in a subgroup anyway
            if(leaver_rank <= curr_view->next_unassigned_rank) {
                next_unassigned_rank--;
            }
        }
        logger->debug("Next view will exclude {} failed members.", leave_ranks.size());

        //Copy member information, excluding the members that have failed
        int m = 0;
        for(int n = 0; n < Vc.num_members; n++) {
            //This is why leave_ranks needs to be a set
            if(leave_ranks.find(n) == leave_ranks.end()) {
                members[m] = Vc.members[n];
                member_ips[m] = Vc.member_ips[n];
                failed[m] = Vc.failed[n];
                ++m;
            }
        }

        //Initialize my_rank in next_view
        int32_t my_new_rank = -1;
        node_id_t myID = Vc.members[myRank];
        for(int i = 0; i < next_num_members; ++i) {
            if(members[i] == myID) {
                my_new_rank = i;
                break;
            }
        }
        if(my_new_rank == -1) {
            throw derecho_exception("Some other node reported that I failed.  Node " + std::to_string(myID) + " terminating");
        }

        next_view = std::make_unique<View>(Vc.vid + 1, members, member_ips, failed,
                                           joined, departed, my_new_rank, next_unassigned_rank);
        next_view->i_know_i_am_leader = Vc.i_know_i_am_leader;

        // At this point we need to await "meta wedged."
        // To do that, we create a predicate that will fire when meta wedged is
        // true, and put the rest of the code in its trigger.

        auto is_meta_wedged = [this](const DerechoSST& gmsSST) {
            for(int n = 0; n < gmsSST.get_num_rows(); ++n) {
                if(!curr_view->failed[n] && !gmsSST.wedged[n]) {
                    return false;
                }
            }
            return true;
        };
        auto meta_wedged_continuation = [this](DerechoSST& gmsSST) {
            logger->debug("MetaWedged is true; continuing view change");
            std::unique_lock<std::shared_timed_mutex> write_lock(view_mutex);
            assert(next_view);

            //First, for subgroups in which I'm the shard leader, do RaggedEdgeCleanup for the leader
            auto follower_subgroups_and_shards = std::make_shared<std::map<subgroup_id_t, uint32_t>>();
            for(const auto& shard_rank_pair : curr_view->multicast_group->get_subgroup_to_shard_and_rank()) {
                const subgroup_id_t subgroup_id = shard_rank_pair.first;
                const uint32_t shard_num = shard_rank_pair.second.first;
                SubView& shard_view = curr_view->subgroup_shard_views.at(subgroup_id).at(shard_num);
                uint num_shard_senders = 0;
                for(auto v : shard_view.is_sender) {
                    if(v) num_shard_senders++;
                }
                if(num_shard_senders) {
                    if(shard_view.my_rank == curr_view->subview_rank_of_shard_leader(subgroup_id, shard_num)) {
                        leader_ragged_edge_cleanup(*curr_view, subgroup_id,
                                                   curr_view->multicast_group->get_subgroup_to_num_received_offset()
                                                           .at(subgroup_id),
                                                   shard_view.members, num_shard_senders);
                    } else {
                        //Keep track of which subgroups I'm a non-leader in, and what my corresponding shard ID is
                        follower_subgroups_and_shards->emplace(subgroup_id, shard_num);
                    }
                }
            }

            //Wait for the shard leaders of subgroups I'm not a leader in to post global_min_ready before continuing
            auto leader_global_mins_are_ready = [this, follower_subgroups_and_shards](const DerechoSST& gmsSST) {
                for(const auto& subgroup_shard_pair : *follower_subgroups_and_shards) {
                    SubView& shard_view = curr_view->subgroup_shard_views.at(subgroup_shard_pair.first)
                                                  .at(subgroup_shard_pair.second);
                    node_id_t shard_leader = shard_view.members[curr_view->subview_rank_of_shard_leader(
                            subgroup_shard_pair.first, subgroup_shard_pair.second)];
                    if(!gmsSST.global_min_ready[curr_view->rank_of(shard_leader)][subgroup_shard_pair.first])
                        return false;
                }
                return true;
            };

            auto global_min_ready_continuation = [this, follower_subgroups_and_shards](DerechoSST& gmsSST) {
                std::unique_lock<std::shared_timed_mutex> write_lock(view_mutex);
                assert(next_view);

                logger->debug("GlobalMins are ready for all {} subgroup leaders this node is waiting on", follower_subgroups_and_shards->size());
                //Finish RaggedEdgeCleanup for subgroups in which I'm not the leader
                for(const auto& subgroup_shard_pair : *follower_subgroups_and_shards) {
                    SubView& shard_view = curr_view->subgroup_shard_views.at(subgroup_shard_pair.first)
                                                  .at(subgroup_shard_pair.second);
                    uint num_shard_senders = 0;
                    for(auto v : shard_view.is_sender) {
                        if(v) num_shard_senders++;
                    }
                    node_id_t shard_leader = shard_view.members[curr_view->subview_rank_of_shard_leader(
                            subgroup_shard_pair.first, subgroup_shard_pair.second)];
                    follower_ragged_edge_cleanup(*curr_view, subgroup_shard_pair.first,
                                                 curr_view->rank_of(shard_leader),
                                                 curr_view->multicast_group->get_subgroup_to_num_received_offset()
                                                         .at(subgroup_shard_pair.first),
                                                 shard_view.members,
                                                 num_shard_senders);
                }
                //Calculate and save the IDs of shard leaders for the old view
                //If the old view was inadequately provisioned, this will be empty
                std::map<std::type_index, std::vector<std::vector<int64_t>>> old_shard_leaders_by_type
                        = make_shard_leaders_map(*curr_view);

                std::list<tcp::socket> joiner_sockets;
                if(curr_view->i_am_leader() && next_view->joined.size() > 0) {
                    //If j joins have been committed, pop the next j sockets off proposed_join_sockets
                    //and send them the new View (must happen before we try to do SST setup)
                    for(std::size_t c = 0; c < next_view->joined.size(); ++c) {
                        commit_join(*next_view, proposed_join_sockets.front());
                        //save the socket for later
                        joiner_sockets.emplace_back(std::move(proposed_join_sockets.front()));
                        proposed_join_sockets.pop_front();
                    }
                }

                // Delete the last two GMS predicates from the old SST in preparation for deleting it
                gmsSST.predicates.remove(leader_committed_handle);
                gmsSST.predicates.remove(suspected_changed_handle);

                node_id_t my_id = next_view->members[next_view->my_rank];
                logger->debug("Starting creation of new SST and DerechoGroup for view {}", next_view->vid);
                // if new members have joined, add their RDMA connections to SST and RDMC
                for(std::size_t i = 0; i < next_view->joined.size(); ++i) {
                    //The new members will be the last joined.size() elements of the members lists
                    int joiner_rank = next_view->num_members - next_view->joined.size() + i;
                    rdma::impl::verbs_add_connection(next_view->members[joiner_rank], next_view->member_ips[joiner_rank],
                                                     my_id);
                }
                for(std::size_t i = 0; i < next_view->joined.size(); ++i) {
                    int joiner_rank = next_view->num_members - next_view->joined.size() + i;
                    sst::add_node(next_view->members[joiner_rank], next_view->member_ips[joiner_rank]);
                }
                // This will block until everyone responds to SST/RDMC initial handshakes
                transition_multicast_group();

                // Translate the old shard leaders' indices from types to new subgroup IDs
                std::vector<std::vector<int64_t>> old_shard_leaders_by_id = translate_types_to_ids(old_shard_leaders_by_type, *next_view);

                if(curr_view->i_am_leader()) {
                    while(!joiner_sockets.empty()) {
                        //Send the array of old shard leaders, so the new member knows who to receive from
                        std::size_t size_of_vector = mutils::bytes_size(old_shard_leaders_by_id);
                        joiner_sockets.front().write((char*)&size_of_vector, sizeof(std::size_t));
                        mutils::post_object([&joiner_sockets](const char* bytes, std::size_t size) {
                            joiner_sockets.front().write(bytes, size);
                        },
                                            old_shard_leaders_by_id);
                        joiner_sockets.pop_front();
                    }
                }

                // New members can now proceed to view_manager.start(), which will call sync()
                next_view->gmsSST->put();
                next_view->gmsSST->sync_with_members();
                logger->debug("Done setting up SST and DerechoGroup for view {}", next_view->vid);
                {
                    lock_guard_t old_views_lock(old_views_mutex);
                    old_views.push(std::move(curr_view));
                    old_views_cv.notify_all();
                }
                curr_view = std::move(next_view);

                //If in persistent mode, write the new view to disk before using it
                if(!view_file_name.empty()) {
                    persist_object(*curr_view, view_file_name);
                }

                //Resize last_suspected to match the new size of suspected[]
                last_suspected.resize(curr_view->members.size());

                // Register predicates in the new view
                register_predicates();
                curr_view->gmsSST->start_predicate_evaluation();

                // First task with my new view...
                if(curr_view->i_am_new_leader())  // I'm the new leader and everyone who hasn't failed agrees
                {
                    curr_view->merge_changes();  // Create a combined list of Changes
                }

                // Announce the new view to the application
                for(auto& view_upcall : view_upcalls) {
                    view_upcall(*curr_view);
                }
                // One of those view upcalls is to RPCManager, which will set up TCP connections to the new members
                // After doing that, shard leaders can send them RPC objects
                for(subgroup_id_t subgroup_id = 0; subgroup_id < old_shard_leaders_by_id.size(); ++subgroup_id) {
                    for(uint32_t shard = 0; shard < old_shard_leaders_by_id[subgroup_id].size(); ++shard) {
                        //if I was the leader of the shard in the old view...
                        if(my_id == old_shard_leaders_by_id[subgroup_id][shard]) {
                            //send its object state to the new members
                            for(node_id_t shard_joiner : curr_view->subgroup_shard_views[subgroup_id][shard].joined) {
                                if(shard_joiner != my_id) {
                                    send_subgroup_object(subgroup_id, shard_joiner);
                                }
                            }
                        }
                    }
                }

                // Re-initialize this node's RPC objects, which includes receiving them
                // from shard leaders if it is newly a member of a subgroup
                initialize_subgroup_objects(my_id, *curr_view, old_shard_leaders_by_id);
                view_change_cv.notify_all();
            };

            //Last statement in meta_wedged_continuation: register global_min_ready_continuation
            gmsSST.predicates.insert(leader_global_mins_are_ready, global_min_ready_continuation, sst::PredicateType::ONE_TIME);

        };

        //Last statement in start_view_change: register meta_wedged_continuation
        gmsSST.predicates.insert(is_meta_wedged, meta_wedged_continuation, sst::PredicateType::ONE_TIME);

    };

    suspected_changed_handle = curr_view->gmsSST->predicates.insert(suspected_changed, suspected_changed_trig, sst::PredicateType::RECURRENT);
    start_join_handle = curr_view->gmsSST->predicates.insert(start_join_pred, start_join_trig, sst::PredicateType::RECURRENT);
    change_commit_ready_handle = curr_view->gmsSST->predicates.insert(change_commit_ready, commit_change, sst::PredicateType::RECURRENT);
    leader_proposed_handle = curr_view->gmsSST->predicates.insert(leader_proposed_change, ack_proposed_change, sst::PredicateType::RECURRENT);
    leader_committed_handle = curr_view->gmsSST->predicates.insert(leader_committed_next_view, start_view_change, sst::PredicateType::ONE_TIME);
}

/* ----------  2. Helper Functions for Predicates and Triggers ------------- */

void ViewManager::construct_multicast_group(CallbackSet callbacks,
                                            const DerechoParams& derecho_params) {
    std::map<subgroup_id_t, std::pair<uint32_t, uint32_t>> subgroup_to_shard_and_rank;
    std::map<subgroup_id_t, std::pair<std::vector<int>, int>> subgroup_to_senders_and_sender_rank;
    std::map<subgroup_id_t, uint32_t> subgroup_to_num_received_offset;
    std::map<subgroup_id_t, std::vector<node_id_t>> subgroup_to_membership;
    std::map<subgroup_id_t, Mode> subgroup_to_mode;

    uint32_t num_received_size = make_subgroup_maps(std::unique_ptr<View>(), *curr_view,
                                                    subgroup_to_shard_and_rank,
                                                    subgroup_to_senders_and_sender_rank,
                                                    subgroup_to_num_received_offset,
                                                    subgroup_to_membership,
                                                    subgroup_to_mode);
    const auto num_subgroups = curr_view->subgroup_shard_views.size();
    curr_view->gmsSST = std::make_shared<DerechoSST>(
            sst::SSTParams(curr_view->members, curr_view->members[curr_view->my_rank],
                           [this](const uint32_t node_id) { report_failure(node_id); }, curr_view->failed, false),
            num_subgroups, num_received_size, derecho_params.window_size);

    curr_view->multicast_group = std::make_unique<MulticastGroup>(
            curr_view->members, curr_view->members[curr_view->my_rank],
            curr_view->gmsSST, callbacks, num_subgroups, subgroup_to_shard_and_rank,
            subgroup_to_senders_and_sender_rank,
            subgroup_to_num_received_offset, subgroup_to_membership,
            subgroup_to_mode,
            derecho_params, 
            post_persistence_request_func,
            curr_view->failed);
}

void ViewManager::transition_multicast_group() {
    std::map<subgroup_id_t, std::pair<uint32_t, uint32_t>> subgroup_to_shard_and_rank;
    std::map<subgroup_id_t, std::pair<std::vector<int>, int>> subgroup_to_senders_and_sender_rank;
    std::map<subgroup_id_t, uint32_t> subgroup_to_num_received_offset;
    std::map<subgroup_id_t, std::vector<node_id_t>> subgroup_to_membership;
    std::map<subgroup_id_t, Mode> subgroup_to_mode;
    uint32_t num_received_size = make_subgroup_maps(curr_view, *next_view, subgroup_to_shard_and_rank,
                                                    subgroup_to_senders_and_sender_rank,
                                                    subgroup_to_num_received_offset,
                                                    subgroup_to_membership,
                                                    subgroup_to_mode);
    const auto num_subgroups = next_view->subgroup_shard_views.size();
    next_view->gmsSST = std::make_shared<DerechoSST>(
            sst::SSTParams(next_view->members, next_view->members[next_view->my_rank],
                           [this](const uint32_t node_id) { report_failure(node_id); }, next_view->failed, false),
            num_subgroups, num_received_size, derecho_params.window_size);

    next_view->multicast_group = std::make_unique<MulticastGroup>(
            next_view->members, next_view->members[next_view->my_rank], next_view->gmsSST,
            std::move(*curr_view->multicast_group), num_subgroups,
            subgroup_to_shard_and_rank, subgroup_to_senders_and_sender_rank,
            subgroup_to_num_received_offset, subgroup_to_membership,
            subgroup_to_mode,
            post_persistence_request_func,
            next_view->failed);

    curr_view->multicast_group.reset();

    // Initialize this node's row in the new SST
    int changes_installed = next_view->joined.size() + next_view->departed.size();
    next_view->gmsSST->init_local_row_from_previous((*curr_view->gmsSST), curr_view->my_rank, changes_installed);
    gmssst::set(next_view->gmsSST->vid[next_view->my_rank], next_view->vid);
}

void ViewManager::receive_join(tcp::socket& client_socket) {
    DerechoSST& gmsSST = *curr_view->gmsSST;
    if((gmsSST.num_changes[curr_view->my_rank] - gmsSST.num_committed[curr_view->my_rank]) == (int)gmsSST.changes.size()) {
        //TODO: this shouldn't throw an exception, it should just block the client until the group stabilizes
        throw derecho_exception("Too many changes to allow a Join right now");
    }

    struct in_addr joiner_ip_packed;
    inet_aton(client_socket.remote_ip.c_str(), &joiner_ip_packed);

    node_id_t joining_client_id = 0;
    client_socket.exchange(curr_view->members[curr_view->my_rank], joining_client_id);

    logger->debug("Proposing change to add node {}", joining_client_id);
    size_t next_change = gmsSST.num_changes[curr_view->my_rank] - gmsSST.num_installed[curr_view->my_rank];
    gmssst::set(gmsSST.changes[curr_view->my_rank][next_change], joining_client_id);
    gmssst::set(gmsSST.joiner_ips[curr_view->my_rank][next_change], joiner_ip_packed.s_addr);

    gmssst::increment(gmsSST.num_changes[curr_view->my_rank]);

    logger->debug("Wedging view {}", curr_view->vid);
    curr_view->wedge();
    logger->debug("Leader done wedging view.");
    gmsSST.put(gmsSST.changes.get_base() - gmsSST.getBaseAddress(), gmsSST.num_committed.get_base() - gmsSST.changes.get_base());
}

void ViewManager::commit_join(const View& new_view, tcp::socket& client_socket) {
    logger->debug("Sending client the new view");
    auto bind_socket_write = [&client_socket](const char* bytes, std::size_t size) { client_socket.write(bytes, size); };
    std::size_t size_of_view = mutils::bytes_size(new_view);
    client_socket.write((char*)&size_of_view, sizeof(size_of_view));
    mutils::post_object(bind_socket_write, new_view);
    std::size_t size_of_derecho_params = mutils::bytes_size(derecho_params);
    client_socket.write((char*)&size_of_derecho_params, sizeof(size_of_derecho_params));
    mutils::post_object(bind_socket_write, derecho_params);
}

uint32_t ViewManager::make_subgroup_maps(const std::unique_ptr<View>& prev_view,
                                         View& curr_view,
                                         std::map<subgroup_id_t, std::pair<uint32_t, uint32_t>>& subgroup_to_shard_and_rank,
                                         std::map<subgroup_id_t, std::pair<std::vector<int>, int>>& subgroup_to_senders_and_sender_rank,
                                         std::map<subgroup_id_t, uint32_t>& subgroup_to_num_received_offset,
                                         std::map<subgroup_id_t, std::vector<node_id_t>>& subgroup_to_membership,
                                         std::map<subgroup_id_t, Mode>& subgroup_to_mode) {
    uint32_t num_received_offset = 0;
    bool previous_was_ok = !prev_view || prev_view->is_adequately_provisioned;
    int32_t initial_next_unassigned_rank = curr_view.next_unassigned_rank;
    for(const auto& subgroup_type : subgroup_info.membership_function_order) {
        subgroup_shard_layout_t subgroup_shard_views;
        //This is the only place the subgroup membership functions are called; the results are then saved in the View
        try {
            auto temp = subgroup_info.subgroup_membership_functions.at(subgroup_type)(curr_view, curr_view.next_unassigned_rank, previous_was_ok);
            //Hack to ensure RVO still works even though subgroup_shard_views had to be declared outside this scope
            subgroup_shard_views = std::move(temp);
        } catch(subgroup_provisioning_exception& ex) {
            //Mark the view as inadequate and roll back everything done by previous allocation functions
            curr_view.is_adequately_provisioned = false;
            curr_view.next_unassigned_rank = initial_next_unassigned_rank;
            curr_view.subgroup_shard_views.clear();
            curr_view.subgroup_ids_by_type.clear();

            subgroup_to_shard_and_rank.clear();
            subgroup_to_senders_and_sender_rank.clear();
            subgroup_to_num_received_offset.clear();
            subgroup_to_membership.clear();
            subgroup_to_mode.clear();

            return 0;
        }
        std::size_t num_subgroups = subgroup_shard_views.size();
        curr_view.subgroup_ids_by_type[subgroup_type] = std::vector<subgroup_id_t>(num_subgroups);
        for(uint32_t subgroup_index = 0; subgroup_index < num_subgroups; ++subgroup_index) {
            //Assign this (type, index) pair a new unique subgroup ID
            subgroup_id_t next_subgroup_number = curr_view.subgroup_shard_views.size();
            curr_view.subgroup_ids_by_type[subgroup_type][subgroup_index] = next_subgroup_number;
            uint32_t num_shards = subgroup_shard_views.at(subgroup_index).size();
            uint32_t max_shard_senders = 0;
            for(uint shard_num = 0; shard_num < num_shards; ++shard_num) {
                SubView& shard_view = subgroup_shard_views.at(subgroup_index).at(shard_num);
                std::size_t shard_size = shard_view.members.size();
                uint32_t num_shard_senders = shard_view.num_senders();
                if(num_shard_senders > max_shard_senders) {
                    max_shard_senders = shard_size;
                }
                //Initialize my_rank in the SubView for this node's ID
                shard_view.my_rank = shard_view.rank_of(curr_view.members[curr_view.my_rank]);
                if(shard_view.my_rank != -1) {
                    subgroup_to_shard_and_rank[next_subgroup_number] = {shard_num, shard_view.my_rank};
                    subgroup_to_senders_and_sender_rank[next_subgroup_number] = {shard_view.is_sender, shard_view.sender_rank_of(shard_view.my_rank)};
                    subgroup_to_num_received_offset[next_subgroup_number] = num_received_offset;
                    subgroup_to_membership[next_subgroup_number] = shard_view.members;
                    subgroup_to_mode[next_subgroup_number] = shard_view.mode;
                }
                if(prev_view && prev_view->is_adequately_provisioned) {
                    //Initialize this shard's SubView.joined and SubView.departed
                    subgroup_id_t prev_subgroup_id = prev_view->subgroup_ids_by_type
                                                             .at(subgroup_type)
                                                             .at(subgroup_index);
                    SubView& prev_shard_view = prev_view->subgroup_shard_views[prev_subgroup_id][shard_num];
                    std::set<node_id_t> prev_members(prev_shard_view.members.begin(), prev_shard_view.members.end());
                    std::set<node_id_t> curr_members(shard_view.members.begin(), shard_view.members.end());
                    std::set_difference(curr_members.begin(), curr_members.end(),
                                        prev_members.begin(), prev_members.end(),
                                        std::back_inserter(shard_view.joined));
                    std::set_difference(prev_members.begin(), prev_members.end(),
                                        curr_members.begin(), curr_members.end(),
                                        std::back_inserter(shard_view.departed));
                }
            }
            /* Pull the shard->SubView mapping out of the subgroup membership list
             * and save it under its subgroup ID (which was shard_views_by_subgroup.size()) */
            curr_view.subgroup_shard_views.emplace_back(
                    std::move(subgroup_shard_views[subgroup_index]));
            num_received_offset += max_shard_senders;
        }
    }
    return num_received_offset;
}

/**
 * Constructs a map from subgroup type -> index -> shard -> node ID of that shard's leader.
 * If a shard has no leader in the current view (because it has no members), the vector will
 * contain -1 instead of a node ID
 */
std::map<std::type_index, std::vector<std::vector<int64_t>>> ViewManager::make_shard_leaders_map(const View& view) {
    std::map<std::type_index, std::vector<std::vector<int64_t>>> shard_leaders_by_type;
    for(const auto& type_to_ids : view.subgroup_ids_by_type) {
        //Raw subgroups don't have any state to send to new members
        if(type_to_ids.first == std::type_index(typeid(RawObject))) {
            continue;
        }
        shard_leaders_by_type[type_to_ids.first].resize(type_to_ids.second.size());
        for(uint32_t subgroup_index = 0; subgroup_index < type_to_ids.second.size(); ++subgroup_index) {
            subgroup_id_t subgroup_id = type_to_ids.second[subgroup_index];
            std::size_t num_shards = view.subgroup_shard_views[subgroup_id].size();
            shard_leaders_by_type[type_to_ids.first][subgroup_index].resize(num_shards, -1);
            for(uint32_t shard = 0; shard < num_shards; ++shard) {
                int shard_leader_rank = view.subview_rank_of_shard_leader(subgroup_id, shard);
                if(shard_leader_rank >= 0) {
                    shard_leaders_by_type[type_to_ids.first][subgroup_index][shard]
                            = view.subgroup_shard_views[subgroup_id][shard].members[shard_leader_rank];
                }
            }
        }
    }
    return shard_leaders_by_type;
}

/**
 * Translates the old shard leaders' indices from (type, index) pairs to new subgroup IDs.
 * An entry in this vector will have value -1 if there was no old leader for that shard.
 */
std::vector<std::vector<int64_t>> ViewManager::translate_types_to_ids(
        const std::map<std::type_index, std::vector<std::vector<int64_t>>>& old_shard_leaders_by_type,
        const View& new_view) {
    std::vector<std::vector<int64_t>> old_shard_leaders_by_id(new_view.subgroup_shard_views.size());
    if(!new_view.is_adequately_provisioned) {
        /* If we went from adequately provisioned to inadequately provisioned,
         * the new view won't have any subgroups!
         * We can't identify the old shard leaders in the new view at all,
         * so I guess we'll have to return an empty vector. */
        return old_shard_leaders_by_id;
    }
    for(const auto& type_vector_pair : old_shard_leaders_by_type) {
        const auto& leaders_by_index_and_shard = type_vector_pair.second;
        for(std::size_t subgroup_index = 0; subgroup_index < leaders_by_index_and_shard.size(); ++subgroup_index) {
            subgroup_id_t new_subgroup_id = new_view.subgroup_ids_by_type.at(type_vector_pair.first)
                                                    .at(subgroup_index);
            std::size_t num_shards = leaders_by_index_and_shard[subgroup_index].size();
            old_shard_leaders_by_id[new_subgroup_id].resize(num_shards, -1);
            for(std::size_t shard = 0; shard < num_shards; ++shard) {
                old_shard_leaders_by_id[new_subgroup_id][shard]
                        = leaders_by_index_and_shard[subgroup_index][shard];
            }
        }
    }
    return old_shard_leaders_by_id;
}

bool ViewManager::suspected_not_equal(const DerechoSST& gmsSST, const std::vector<bool>& old) {
    for(int r = 0; r < gmsSST.get_num_rows(); r++) {
        for(size_t who = 0; who < gmsSST.suspected.size(); who++) {
            if(gmsSST.suspected[r][who] && !old[who]) {
                return true;
            }
        }
    }
    return false;
}

void ViewManager::copy_suspected(const DerechoSST& gmsSST, std::vector<bool>& old) {
    for(size_t who = 0; who < gmsSST.suspected.size(); ++who) {
        old[who] = gmsSST.suspected[gmsSST.get_local_index()][who];
    }
}

bool ViewManager::changes_contains(const DerechoSST& gmsSST, const node_id_t q) {
    int myRow = gmsSST.get_local_index();
    for(int p_index = 0; p_index < gmsSST.num_changes[myRow] - gmsSST.num_installed[myRow]; p_index++) {
        const node_id_t p(const_cast<node_id_t&>(gmsSST.changes[myRow][p_index]));
        if(p == q) {
            return true;
        }
    }
    return false;
}

int ViewManager::min_acked(const DerechoSST& gmsSST, const std::vector<char>& failed) {
    int myRank = gmsSST.get_local_index();
    int min = gmsSST.num_acked[myRank];
    for(size_t n = 0; n < failed.size(); n++) {
        if(!failed[n] && gmsSST.num_acked[n] < min) {
            min = gmsSST.num_acked[n];
        }
    }

    return min;
}

void ViewManager::deliver_in_order(const View& Vc, const int shard_leader_rank,
                                   const uint32_t subgroup_num, const uint32_t num_received_offset,
                                   const std::vector<node_id_t>& shard_members, uint num_shard_senders) {
    // Ragged cleanup is finished, deliver in the implied order
    std::vector<long long int> max_received_indices(num_shard_senders);
    std::string deliveryOrder(" ");
    for(uint n = 0; n < num_shard_senders; n++) {
        deliveryOrder += "Subgroup " + std::to_string(subgroup_num)
                         + " " + std::to_string(Vc.members[Vc.my_rank])
                         + std::string(":0..")
                         + std::to_string(Vc.gmsSST->global_min[shard_leader_rank][num_received_offset + n])
                         + std::string(" ");
        max_received_indices[n] = Vc.gmsSST->global_min[shard_leader_rank][num_received_offset + n];
    }
    logger->debug("Delivering ragged-edge messages in order: {}", deliveryOrder);
    Vc.multicast_group->deliver_messages_upto(max_received_indices, subgroup_num, num_shard_senders);
}

void ViewManager::leader_ragged_edge_cleanup(View& Vc, const subgroup_id_t subgroup_num,
                                             const uint32_t num_received_offset,
                                             const std::vector<node_id_t>& shard_members, uint num_shard_senders) {
    logger->debug("Running leader RaggedEdgeCleanup for subgroup {}", subgroup_num);
    int myRank = Vc.my_rank;
    // int Leader = Vc.rank_of_leader();  // We don't want this to change under our feet
    bool found = false;
    for(uint n = 0; n < shard_members.size() && !found; n++) {
        const auto node_id = shard_members[n];
        const auto node_rank = Vc.rank_of(node_id);
        if(Vc.gmsSST->global_min_ready[node_rank][subgroup_num]) {
            gmssst::set(Vc.gmsSST->global_min[myRank] + num_received_offset,
                        Vc.gmsSST->global_min[node_rank] + num_received_offset, num_shard_senders);
            found = true;
        }
    }

    if(!found) {
        for(uint n = 0; n < num_shard_senders; n++) {
            int min = Vc.gmsSST->num_received[myRank][num_received_offset + n];
            for(uint r = 0; r < shard_members.size(); r++) {
                const auto node_id = shard_members[r];
                const auto node_rank = Vc.rank_of(node_id);
                if(/*!Vc.failed[r] && */ min > Vc.gmsSST->num_received[node_rank][num_received_offset + n]) {
                    min = Vc.gmsSST->num_received[node_rank][num_received_offset + n];
                }
            }

            gmssst::set(Vc.gmsSST->global_min[myRank][num_received_offset + n], min);
        }
    }

    logger->debug("Shard leader for subgroup {} finished computing global_min", subgroup_num);
    gmssst::set(Vc.gmsSST->global_min_ready[myRank][subgroup_num], true);
    Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_num),
                   (char*)std::addressof(Vc.gmsSST->global_min[0][num_received_offset]) - Vc.gmsSST->getBaseAddress(),
                   sizeof(Vc.gmsSST->global_min[0][num_received_offset]) * num_shard_senders);
    Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_num),
                   (char*)std::addressof(Vc.gmsSST->global_min_ready[0][subgroup_num]) - Vc.gmsSST->getBaseAddress(),
                   sizeof(Vc.gmsSST->global_min_ready[0][subgroup_num]));

    deliver_in_order(Vc, myRank, subgroup_num, num_received_offset, shard_members, num_shard_senders);
    logger->debug("Done with RaggedEdgeCleanup for subgroup {}", subgroup_num);
}

void ViewManager::follower_ragged_edge_cleanup(View& Vc, const subgroup_id_t subgroup_num,
                                               uint shard_leader_rank,
                                               const uint32_t num_received_offset,
                                               const std::vector<node_id_t>& shard_members, uint num_shard_senders) {
    int myRank = Vc.my_rank;
    // Learn the leader's data and push it before acting upon it
    logger->debug("Running follower RaggedEdgeCleanup for subgroup {}; echoing leader's global_min", subgroup_num);
    gmssst::set(Vc.gmsSST->global_min[myRank] + num_received_offset, Vc.gmsSST->global_min[shard_leader_rank] + num_received_offset,
                num_shard_senders);
    gmssst::set(Vc.gmsSST->global_min_ready[myRank][subgroup_num], true);
    Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_num),
                   (char*)std::addressof(Vc.gmsSST->global_min[0][num_received_offset]) - Vc.gmsSST->getBaseAddress(),
                   sizeof(Vc.gmsSST->global_min[0][num_received_offset]) * num_shard_senders);
    Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_num),
                   (char*)std::addressof(Vc.gmsSST->global_min_ready[0][subgroup_num]) - Vc.gmsSST->getBaseAddress(),
                   sizeof(Vc.gmsSST->global_min_ready[0][subgroup_num]));
    deliver_in_order(Vc, shard_leader_rank, subgroup_num, num_received_offset, shard_members, num_shard_senders);
    logger->debug("Done with RaggedEdgeCleanup for subgroup {}", subgroup_num);
}

/* ----------  3. Public-Interface methods of ViewManager ------------- */

void ViewManager::report_failure(const node_id_t who) {
    int r = curr_view->rank_of(who);
    logger->debug("Node ID {} failure reported; marking suspected[{}]", who, r);
    curr_view->gmsSST->suspected[curr_view->my_rank][r] = true;
    int cnt = 0;
    for(r = 0; r < (int)curr_view->gmsSST->suspected.size(); r++) {
        if(curr_view->gmsSST->suspected[curr_view->my_rank][r]) {
            ++cnt;
        }
    }

    if(cnt >= (curr_view->num_members + 1) / 2) {
        throw derecho_exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
    }
    curr_view->gmsSST->put((char*)std::addressof(curr_view->gmsSST->suspected[0][r]) - curr_view->gmsSST->getBaseAddress(), sizeof(curr_view->gmsSST->suspected[0][r]));
}

void ViewManager::leave() {
    shared_lock_t lock(view_mutex);
    logger->debug("Cleanly leaving the group.");
    curr_view->multicast_group->wedge();
    curr_view->gmsSST->predicates.clear();
    curr_view->gmsSST->suspected[curr_view->my_rank][curr_view->my_rank] = true;
    curr_view->gmsSST->put((char*)std::addressof(curr_view->gmsSST->suspected[0][curr_view->my_rank]) - curr_view->gmsSST->getBaseAddress(), sizeof(curr_view->gmsSST->suspected[0][curr_view->my_rank]));
    thread_shutdown = true;
}

char* ViewManager::get_sendbuffer_ptr(subgroup_id_t subgroup_num, unsigned long long int payload_size,
                                      bool transfer_medium, int pause_sending_turns,
                                      bool cooked_send, bool null_send) {
    shared_lock_t lock(view_mutex);
    return curr_view->multicast_group->get_sendbuffer_ptr(subgroup_num, payload_size, transfer_medium, pause_sending_turns, cooked_send, null_send);
}

void ViewManager::send(subgroup_id_t subgroup_num) {
    shared_lock_t lock(view_mutex);
    while(true) {
        if(curr_view->multicast_group->send(subgroup_num)) break;
        view_change_cv.wait(lock);
    }
}

void ViewManager::add_view_upcall(const view_upcall_t& upcall) {
    view_upcalls.emplace_back(upcall);
}

std::vector<node_id_t> ViewManager::get_members() {
    shared_lock_t read_lock(view_mutex);
    return curr_view->members;
}

void ViewManager::barrier_sync() {
    shared_lock_t read_lock(view_mutex);
    curr_view->gmsSST->sync_with_members();
}

SharedLockedReference<View> ViewManager::get_current_view() {
    return SharedLockedReference<View>(*curr_view, view_mutex);
}

void ViewManager::debug_print_status() const {
    std::cout << "curr_view = " << curr_view->debug_string() << std::endl;
}

//void ViewManager::print_log(std::ostream& output_dest) const {
//    for(size_t i = 0; i < util::debug_log().curr_event; ++i) {
//        output_dest << util::debug_log().times[i] << "," << util::debug_log().events[i] << "," << std::string("ABCDEFGHIJKLMNOPQRSTUVWXYZ")[curr_view->members[curr_view->my_rank]] << std::endl;
//    }
//}

} /* namespace derecho */
