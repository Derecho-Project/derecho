/**
 * @file ViewManager.cpp
 *
 * @date Feb 6, 2017
 * @author edward
 */

#include <arpa/inet.h>

#include "view_manager.h"
#include "persistence.h"
#include "derecho_exception.h"

namespace derecho {

using lock_guard_t = std::lock_guard<std::mutex>;
using unique_lock_t = std::unique_lock<std::mutex>;

ViewManager::ViewManager(
        const ip_addr my_ip,
        CallbackSet callbacks,
        const SubgroupInfo& subgroup_info,
        const DerechoParams& derecho_params,
        std::vector<view_upcall_t> _view_upcalls,
        const int gms_port) :
                gms_port(gms_port),
                curr_view(make_initial_view(0, my_ip)),
                last_suspected(2),  //The initial view always has 2 members
                server_socket(gms_port),
                thread_shutdown(false),
                view_upcalls(_view_upcalls),
                subgroup_info(subgroup_info),
                derecho_params(derecho_params) {
    const node_id_t my_id = 0;
    initialize_rdmc_sst();
    //Wait for the first client (second group member) to join
    await_second_member(my_id);

    if(!derecho_params.filename.empty()) {
        view_file_name = std::string(derecho_params.filename + persistence::PAXOS_STATE_EXTENSION);
        std::string params_file_name(derecho_params.filename + persistence::PARAMATERS_EXTENSION);
        persist_object(*curr_view, view_file_name);
        persist_object(derecho_params, params_file_name);
    }

    log_event("Initializing SST and RDMC for the first time.");
    construct_multicast_group(callbacks, derecho_params);
}

ViewManager::ViewManager(const node_id_t my_id,
                         tcp::socket& leader_connection,
                         CallbackSet callbacks,
                         const SubgroupInfo& subgroup_info,
                         std::vector<view_upcall_t> _view_upcalls,
                         const int gms_port)
        : gms_port(gms_port),
          server_socket(gms_port),
          thread_shutdown(false),
          view_upcalls(_view_upcalls),
          subgroup_info(subgroup_info),
          derecho_params(0, 0) {
    //First, receive the view and parameters over the given socket
    receive_configuration(my_id, leader_connection);

    //Set this while we still know my_id
    curr_view->my_rank = curr_view->rank_of(my_id);

    last_suspected = std::vector<bool>(curr_view->members.size());

    initialize_rdmc_sst();
    if (!derecho_params.filename.empty()) {
        view_file_name = std::string(derecho_params.filename + persistence::PAXOS_STATE_EXTENSION);
        std::string params_file_name(derecho_params.filename + persistence::PARAMATERS_EXTENSION);
        persist_object(*curr_view, view_file_name);
        persist_object(derecho_params, params_file_name);
    }
    log_event("Initializing SST and RDMC for the first time.");

    construct_multicast_group(callbacks, derecho_params);
    curr_view->gmsSST->vid[curr_view->my_rank] = curr_view->vid;
}


ViewManager::ViewManager(const std::string& recovery_filename,
                             const node_id_t my_id,
                             const ip_addr my_ip,
                             CallbackSet callbacks,
                             const SubgroupInfo& subgroup_info,
                             std::experimental::optional<DerechoParams> _derecho_params,
                             std::vector<view_upcall_t> _view_upcalls,
                             const int gms_port)
        : gms_port(gms_port),
          server_socket(gms_port),
          thread_shutdown(false),
          view_file_name(recovery_filename + persistence::PAXOS_STATE_EXTENSION),
          view_upcalls(_view_upcalls),
          subgroup_info(subgroup_info),
          derecho_params(0, 0) {
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
        curr_view = make_initial_view(my_id, my_ip);
        curr_view->vid = last_view->vid + 1;
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

    log_event("Initializing SST and RDMC for the first time.");
    construct_multicast_group(callbacks,
                  derecho_params);
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

void ViewManager::receive_configuration(node_id_t my_id, tcp::socket& leader_connection) {
    std::cout << "Successfully connected to leader, about to receive the View." << std::endl;
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
    log_event("Done setting up initial SST and RDMC");

    if (curr_view->vid != 0) {
        // If this node is joining an existing group with a non-initial view, copy the leader's num_changes, num_acked, and num_committed
        // Otherwise, you'll immediately think that there's a new proposed view change because gmsSST.num_changes[leader] > num_acked[my_rank]
        curr_view->gmsSST->init_local_change_proposals(curr_view->rank_of_leader());
        curr_view->gmsSST->put();
        log_event("Joining node initialized its SST row from the leader");
    }

    create_threads();
    register_predicates();
    curr_view->gmsSST->start_predicate_evaluation();
    log_event("Starting predicate evaluation");

    lock_guard_t lock(view_mutex);
    for(auto& view_upcall : view_upcalls) {
        view_upcall(*curr_view);
    }
}

void ViewManager::await_second_member(const node_id_t my_id) {
    tcp::socket client_socket = server_socket.accept();
    node_id_t joiner_id = 0;
    client_socket.exchange(my_id, joiner_id);
    ip_addr& joiner_ip = client_socket.remote_ip;
    curr_view->num_members++;
    curr_view->member_ips.emplace_back(joiner_ip);
    curr_view->members.emplace_back(joiner_id);
    curr_view->failed.emplace_back(false);
    curr_view->joined.emplace_back(joiner_id);

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
    //Sending a "0" will cause the second member's receive_objects to terminate immediately
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

void ViewManager::create_threads() {
    client_listener_thread = std::thread{[this]() {
        while(!thread_shutdown) {
            tcp::socket client_socket = server_socket.accept();
            util::debug_log().log_event(std::stringstream() << "Background thread got a client connection from " << client_socket.remote_ip);
            pending_join_sockets.locked().access.emplace_back(std::move(client_socket));
        }
        std::cout << "Connection listener thread shutting down." << std::endl;
    }};

    old_view_cleanup_thread = std::thread([this]() {
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
        log_event("Suspected[] changed");
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
                log_event(std::stringstream() << "Marking " << Vc.members[q] << " failed");
                if(Vc.num_failed >= (Vc.num_members + 1) / 2) {
                    throw derecho_exception("Majority of a Derecho group simultaneously failed ... shutting down");
                }

                log_event(std::stringstream() << "GMS telling SST to freeze row " << q << " which is node " << Vc.members[q]);
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
                gmsSST.put(gmsSST.wedged.get_base() - gmsSST.getBaseAddress(), sizeof(bool));
                if(Vc.i_am_leader() && !changes_contains(gmsSST, Vc.members[q]))  // Leader initiated
                {
                    const int next_change_index = gmsSST.num_changes[myRank] - gmsSST.num_installed[myRank];
                    if(next_change_index == (int) gmsSST.changes.size()) {
                        throw derecho_exception("Ran out of room in the pending changes list");
                    }

                    gmssst::set(gmsSST.changes[myRank][next_change_index], Vc.members[q]);  // Reports the failure (note that q NotIn members)
                    gmssst::increment(gmsSST.num_changes[myRank]);
                    log_event(std::stringstream() << "Leader proposed a change to remove failed node " << Vc.members[q]);
                    gmsSST.put((char*)std::addressof(gmsSST.changes[0][next_change_index]) - gmsSST.getBaseAddress(), sizeof(node_id_t));
                    gmsSST.put(gmsSST.num_changes.get_base() - gmsSST.getBaseAddress(), sizeof(int));
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
        log_event("GMS handling a new client connection");
        //C++'s ugly two-step dequeue: leave queue.front() in an invalid state, then delete it
        proposed_join_sockets.emplace_back(std::move(pending_join_sockets.locked().access.front()));
        pending_join_sockets.locked().access.pop_front();
        receive_join(proposed_join_sockets.back());
    };

    /* These run only on the leader. They monitor the acks received from followers
     * and update the leader's nCommitted when all non-failed members have acked */
    auto change_commit_ready = [this](const DerechoSST& gmsSST) {
        return curr_view->i_am_leader() &&
               min_acked(gmsSST, curr_view->failed) > gmsSST.num_committed[gmsSST.get_local_index()];
    };
    auto commit_change = [this](DerechoSST& gmsSST) {
        gmssst::set(gmsSST.num_committed[gmsSST.get_local_index()],
                    min_acked(gmsSST, curr_view->failed));  // Leader commits a new request
        log_event(std::stringstream() << "Leader committing change proposal #" << gmsSST.num_committed[gmsSST.get_local_index()]);
        gmsSST.put(gmsSST.num_committed.get_base() - gmsSST.getBaseAddress(), sizeof(int));
    };

    /* These are mostly intended for non-leaders, and update nAcked to acknowledge
     * a proposed change when the leader increments nChanges. Only one join can be
     * proposed at once, but multiple failures could be proposed and acknowledged. */
    auto leader_proposed_change = [this](const DerechoSST& gmsSST) {
        return gmsSST.num_changes[curr_view->rank_of_leader()] >
               gmsSST.num_acked[gmsSST.get_local_index()];
    };
    auto ack_proposed_change = [this](DerechoSST& gmsSST) {
        // These fields had better be synchronized.
        assert(gmsSST.get_local_index() == curr_view->my_rank);
        int myRank = gmsSST.get_local_index();
        int leader = curr_view->rank_of_leader();
        log_event(std::stringstream() << "Detected that leader proposed change #" << gmsSST.num_changes[leader] << ". Acknowledging.");
        if(myRank != leader) {
            // Echo (copy) the vector including the new changes
            gmssst::set(gmsSST.changes[myRank], gmsSST.changes[leader], gmsSST.changes.size());
            // Echo the new member's IP
            gmssst::set(gmsSST.joiner_ips[myRank], gmsSST.joiner_ips[leader], gmsSST.joiner_ips.size());
            // Echo the count
            gmssst::set(gmsSST.num_changes[myRank], gmsSST.num_changes[leader]);
            gmssst::set(gmsSST.num_committed[myRank], gmsSST.num_committed[leader]);
        }

        // Notice a new request, acknowledge it
        gmssst::set(gmsSST.num_acked[myRank], gmsSST.num_changes[leader]);
        gmsSST.put(gmsSST.changes.get_base() - gmsSST.getBaseAddress(), gmsSST.num_received.get_base() - gmsSST.changes.get_base());
        log_event("Wedging current view.");
        curr_view->wedge();
        log_event("Done wedging current view.");

    };

    /* This predicate detects when at least one new change has been committed by the leader.
     * The trigger starts the process of changing to a new view. */
    auto leader_committed_next_view = [this](const DerechoSST& gmsSST) {
        return gmsSST.num_committed[curr_view->rank_of_leader()] > gmsSST.num_installed[curr_view->my_rank];
    };
    auto start_view_change = [this](DerechoSST& gmsSST) {
        log_event(std::stringstream() << "Starting view change to view " << (curr_view->vid + 1));
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
        next_view = std::make_unique<View>(next_num_members);
        next_view->vid = Vc.vid + 1;
        next_view->num_failed = Vc.num_failed - leave_ranks.size();
        for(std::size_t i = 0; i < join_indexes.size(); ++i) {
            const int join_index = join_indexes[i];
            node_id_t joiner_id = gmsSST.changes[myRank][join_index];
            struct in_addr joiner_ip_packed;
            joiner_ip_packed.s_addr = gmsSST.joiner_ips[myRank][join_index];
            char* joiner_ip_cstr = inet_ntoa(joiner_ip_packed);
            std::string joiner_ip(joiner_ip_cstr);

            next_view->joined.emplace_back(joiner_id);
            //New members go at the end of the members list, but it may shrink in the new view
            int new_member_rank = Vc.num_members - leave_ranks.size() + i;
            next_view->members[new_member_rank] = joiner_id;
            next_view->member_ips[new_member_rank] = joiner_ip;
            log_event(std::stringstream() << "Next view will add new member with ID " << joiner_id);
        }
        for(const auto& leaver_rank : leave_ranks) {
            next_view->departed.emplace_back(Vc.members[leaver_rank]);
        }
        log_event(std::stringstream() << "Next view will exclude " << leave_ranks.size() << " failed members.");
        next_view->i_know_i_am_leader = Vc.i_know_i_am_leader;

        //Copy member information, excluding the members that have failed
        int m = 0;
        for(int n = 0; n < Vc.num_members; n++) {
            //This is why leave_ranks needs to be a set
            if(leave_ranks.find(n) == leave_ranks.end()) {
                next_view->members[m] = Vc.members[n];
                next_view->member_ips[m] = Vc.member_ips[n];
                next_view->failed[m] = Vc.failed[n];
                ++m;
            }
        }

        //Initialize my_rank in next_view
        node_id_t myID = Vc.members[myRank];
        if((next_view->my_rank = next_view->rank_of(myID)) == -1) {
            throw derecho_exception((std::stringstream() << "Some other node reported that I failed.  Node " << myID << " terminating").str());
        }

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
            log_event("MetaWedged is true; continuing view change");
            unique_lock_t lock(view_mutex);
            assert(next_view);

            auto globalMin_ready_continuation = [this](DerechoSST& gmsSST) {
                lock_guard_t lock(view_mutex);
                assert(next_view);

                ragged_edge_cleanup(*curr_view);
                std::list<tcp::socket> joiner_sockets;
                if(curr_view->i_am_leader() && next_view->joined.size() > 0) {
                    //If j joins have been committed, pop the next j sockets off proposed_join_sockets
                    //and send them the new View (must happen before we try to do SST setup)
                    for(std::size_t c = 0; c < next_view->joined.size(); ++c) {
                        commit_join(*next_view, proposed_join_sockets.front());
                        //Save the socket so we can send RPC objects to it after MulticastGroup setup
                        joiner_sockets.emplace_back(std::move(proposed_join_sockets.front()));
                        proposed_join_sockets.pop_front(); //ugh, can't actually move from a list
                    }
                }

                // Delete the last two GMS predicates from the old SST in preparation for deleting it
                gmsSST.predicates.remove(leader_committed_handle);
                gmsSST.predicates.remove(suspected_changed_handle);

                log_event(std::stringstream() << "Starting creation of new SST and DerechoGroup for view " << next_view->vid);
                // if new members have joined, add their RDMA connections to SST and RDMC
                for(std::size_t i = 0; i < next_view->joined.size(); ++i) {
                    //The new members will be the last joined.size() elements of the members lists
                    int joiner_rank = next_view->num_members - next_view->joined.size() + i;
                    rdma::impl::verbs_add_connection(next_view->members[joiner_rank], next_view->member_ips[joiner_rank],
                                                     next_view->members[next_view->my_rank]);
                }
                for(std::size_t i = 0; i < next_view->joined.size(); ++i) {
                    int joiner_rank = next_view->num_members - next_view->joined.size() + i;
                    sst::add_node(next_view->members[joiner_rank], next_view->member_ips[joiner_rank]);
                }
                // This will block until everyone responds to SST/RDMC initial handshakes
                transition_multicast_group(*next_view);
                // New members will now have their SST and MulticastGroup created, and need to receive RPC objects
                if(curr_view->i_am_leader()) {
                    while(!joiner_sockets.empty()) {
                        send_subgroup_objects(joiner_sockets.front());
                        //Close the socket, since we're done sending stuff now
                        joiner_sockets.pop_front();
                    }
                }
                // New members can now proceed to view_manager.start(), which will call sync()
                next_view->gmsSST->put();
                next_view->gmsSST->sync_with_members();
                log_event(std::stringstream() << "Done setting up SST and DerechoGroup for view " << next_view->vid);
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

                view_change_cv.notify_all();
            };

            if(curr_view->i_am_leader()) {
                // The leader doesn't need to wait any more, it can execute continuously from here.
                lock.unlock();
                globalMin_ready_continuation(gmsSST);
            } else {
                // Non-leaders need another level of continuation to wait for GlobalMinReady
                auto leader_globalMin_is_ready = [this](const DerechoSST& gmsSST) {
                    return gmsSST.globalMinReady[curr_view->rank_of_leader()];
                };
                gmsSST.predicates.insert(leader_globalMin_is_ready,
                                         globalMin_ready_continuation,
                                         sst::PredicateType::ONE_TIME);
            }

        };
        gmsSST.predicates.insert(is_meta_wedged, meta_wedged_continuation, sst::PredicateType::ONE_TIME);

    };

    suspected_changed_handle = curr_view->gmsSST->predicates.insert(suspected_changed, suspected_changed_trig, sst::PredicateType::RECURRENT);
    start_join_handle = curr_view->gmsSST->predicates.insert(start_join_pred, start_join_trig, sst::PredicateType::RECURRENT);
    change_commit_ready_handle = curr_view->gmsSST->predicates.insert(change_commit_ready, commit_change, sst::PredicateType::RECURRENT);
    leader_proposed_handle = curr_view->gmsSST->predicates.insert(leader_proposed_change, ack_proposed_change, sst::PredicateType::RECURRENT);
    leader_committed_handle = curr_view->gmsSST->predicates.insert(leader_committed_next_view, start_view_change, sst::PredicateType::ONE_TIME);
}

void ViewManager::construct_multicast_group(CallbackSet callbacks,
                                const DerechoParams& derecho_params) {
    std::map<subgroup_id_t, std::pair<uint32_t, uint32_t>> subgroup_to_shard_n_index;
    std::map<subgroup_id_t, uint32_t> subgroup_to_num_received_offset;
    std::map<subgroup_id_t, std::vector<node_id_t>> subgroup_to_membership;

    uint32_t num_received_size = make_subgroup_maps(*curr_view, subgroup_to_shard_n_index,
            subgroup_to_num_received_offset, subgroup_to_membership);
    const auto num_subgroups = curr_view->shard_views_by_subgroup.size() + 1; //largest subgroup ID + 1
    curr_view->gmsSST = std::make_shared<DerechoSST>(sst::SSTParams(
            curr_view->members, curr_view->members[curr_view->my_rank],
            [this](const uint32_t node_id) { report_failure(node_id); }, curr_view->failed, false),
            num_subgroups, num_received_size);

    //Still todo: Handle cases where curr_view->is_adequately_provisioned == false,
    //which means the subgroup maps may be invalid
    curr_view->multicast_group = std::make_unique<MulticastGroup>(
        curr_view->members, curr_view->members[curr_view->my_rank],
        curr_view->gmsSST, callbacks, num_subgroups, subgroup_to_shard_n_index,
        subgroup_to_num_received_offset, subgroup_to_membership, derecho_params,
        curr_view->failed);
}

void ViewManager::transition_multicast_group(View& newView) {
    std::map<subgroup_id_t, std::pair<uint32_t, uint32_t>> subgroup_to_shard_n_index;
    std::map<subgroup_id_t, uint32_t> subgroup_to_num_received_offset;
    std::map<subgroup_id_t, std::vector<node_id_t>> subgroup_to_membership;
    uint32_t num_received_size = make_subgroup_maps(newView, subgroup_to_shard_n_index,
            subgroup_to_num_received_offset, subgroup_to_membership);
    const auto num_subgroups = curr_view->shard_views_by_subgroup.size() + 1; //largest subgroup ID + 1
    newView.gmsSST = std::make_shared<DerechoSST>(sst::SSTParams(
            newView.members, newView.members[newView.my_rank],
            [this](const uint32_t node_id) { report_failure(node_id); }, newView.failed, false),
            num_subgroups, num_received_size);

    //Still todo: Handle cases where curr_view->is_adequately_provisioned == false,
    //which means the subgroup maps may be invalid
    newView.multicast_group = std::make_unique<MulticastGroup>(
        newView.members, newView.members[newView.my_rank], newView.gmsSST,
        std::move(*curr_view->multicast_group), num_subgroups,
        subgroup_to_shard_n_index,
        subgroup_to_num_received_offset, subgroup_to_membership,
        newView.failed);
    curr_view->multicast_group.reset();

    // Initialize this node's row in the new SST
    int changes_installed = newView.joined.size() + newView.departed.size();
    newView.gmsSST->init_local_row_from_previous((*curr_view->gmsSST), curr_view->my_rank, changes_installed);
    gmssst::set(newView.gmsSST->vid[newView.my_rank], newView.vid);
}

void ViewManager::receive_join(tcp::socket& client_socket) {
    DerechoSST& gmsSST = *curr_view->gmsSST;
    if((gmsSST.num_changes[curr_view->my_rank] -
        gmsSST.num_committed[curr_view->my_rank]) == (int)gmsSST.changes.size()) {
        //TODO: this shouldn't throw an exception, it should just block the client until the group stabilizes
        throw derecho_exception("Too many changes to allow a Join right now");
    }

    struct in_addr joiner_ip_packed;
    inet_aton(client_socket.remote_ip.c_str(), &joiner_ip_packed);

    node_id_t joining_client_id = 0;
    client_socket.exchange(curr_view->members[curr_view->my_rank], joining_client_id);

    log_event(std::stringstream() << "Proposing change to add node " << joining_client_id);
    size_t next_change = gmsSST.num_changes[curr_view->my_rank] - gmsSST.num_installed[curr_view->my_rank];
    gmssst::set(gmsSST.changes[curr_view->my_rank][next_change], joining_client_id);
    gmssst::set(gmsSST.joiner_ips[curr_view->my_rank][next_change], joiner_ip_packed.s_addr);

    gmssst::increment(gmsSST.num_changes[curr_view->my_rank]);

    log_event(std::stringstream() << "Wedging view " << curr_view->vid);
    curr_view->wedge();
    log_event("Leader done wedging view.");
    gmsSST.put(gmsSST.changes.get_base() - gmsSST.getBaseAddress(), gmsSST.num_committed.get_base() - gmsSST.changes.get_base());
}

void ViewManager::commit_join(const View& new_view,
                              tcp::socket& client_socket) {
    log_event("Sending client the new view");
    auto bind_socket_write = [&client_socket](const char* bytes, std::size_t size) { client_socket.write(bytes, size); };
    std::size_t size_of_view = mutils::bytes_size(new_view);
    client_socket.write((char*)&size_of_view, sizeof(size_of_view));
    mutils::post_object(bind_socket_write, new_view);
    std::size_t size_of_derecho_params = mutils::bytes_size(derecho_params);
    client_socket.write((char*)&size_of_derecho_params, sizeof(size_of_derecho_params));
    mutils::post_object(bind_socket_write, derecho_params);
}

void ViewManager::report_failure(const node_id_t who) {
    int r = curr_view->rank_of(who);
    log_event(std::stringstream() << "Node ID " << who << " failure reported; marking suspected[" << r << "]");
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
    curr_view->gmsSST->put((char*)std::addressof(curr_view->gmsSST->suspected[0][r]) - curr_view->gmsSST->getBaseAddress(), sizeof(bool));
}

void ViewManager::leave() {
    lock_guard_t lock(view_mutex);
    log_event("Cleanly leaving the group.");
    curr_view->multicast_group->wedge();
    curr_view->gmsSST->predicates.clear();
    curr_view->gmsSST->suspected[curr_view->my_rank][curr_view->my_rank] = true;
    curr_view->gmsSST->put((char*)std::addressof(curr_view->gmsSST->suspected[0][curr_view->my_rank]) - curr_view->gmsSST->getBaseAddress(), sizeof(bool));
    thread_shutdown = true;
}

char* ViewManager::get_sendbuffer_ptr(subgroup_id_t subgroup_num, unsigned long long int payload_size,
                                      int pause_sending_turns, bool cooked_send) {
    lock_guard_t lock(view_mutex);
    return curr_view->multicast_group->get_sendbuffer_ptr(subgroup_num, payload_size, pause_sending_turns, cooked_send);
}

void ViewManager::send(subgroup_id_t subgroup_num) {
    std::unique_lock<std::mutex> lock(view_mutex);
    while(true) {
        if(curr_view->multicast_group->send(subgroup_num)) break;
        view_change_cv.wait(lock);
    }
}

void ViewManager::add_view_upcall(const view_upcall_t& upcall) {
    view_upcalls.emplace_back(upcall);
}

std::vector<node_id_t> ViewManager::get_members() {
    lock_guard_t lock(view_mutex);
    return curr_view->members;
}

void ViewManager::barrier_sync() {
    lock_guard_t lock(view_mutex);
    curr_view->gmsSST->sync_with_members();
}

const View& ViewManager::get_current_view() {
    lock_guard_t lock(view_mutex);
    return *curr_view;
}

void ViewManager::debug_print_status() const {
    std::cout << "curr_view = " << curr_view->debug_string() << std::endl;
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

void ViewManager::print_log(std::ostream& output_dest) const {
    for(size_t i = 0; i < util::debug_log().curr_event; ++i) {
        output_dest << util::debug_log().times[i] << "," << util::debug_log().events[i] <<
                "," << std::string("ABCDEFGHIJKLMNOPQRSTUVWXYZ")[curr_view->members[curr_view->my_rank]] <<
                std::endl;
    }
}


uint32_t ViewManager::calc_total_num_subgroups() const {
    uint32_t total = 0;
    for(const auto& entry : subgroup_info.num_subgroups) {
        total += entry.second;
    }
    return total;
}

uint32_t ViewManager::make_subgroup_maps(View& curr_view,
                                         std::map<subgroup_id_t, std::pair<uint32_t, uint32_t>>& subgroup_to_shard_n_index,
                                         std::map<subgroup_id_t, uint32_t>& subgroup_to_num_received_offset,
                                         std::map<subgroup_id_t, std::vector<node_id_t>>& subgroup_to_membership) {
    uint32_t subgroup_offset = 0;
    for(const auto& subgroup_type_count : subgroup_info.num_subgroups) {
        std::vector<std::vector<std::unique_ptr<SubView>>> subgroup_shard_views;
        try {
            auto temp = subgroup_info.subgroup_membership_functions.at(subgroup_type_count.first)(curr_view);
            //Hack to ensure RVO still works even though subgroup_shard_views had to be declared outside this scope
            subgroup_shard_views = std::move(temp);
        } catch (subgroup_provisioning_exception& ex) {
            curr_view.is_adequately_provisioned = false;
            continue;
        }
        for(uint32_t subgroup_index = 0; subgroup_index < subgroup_type_count.second; ++subgroup_index) {
            //Assign this (type, index) pair a new unique subgroup ID
            subgroup_id_t next_subgroup_number = curr_view.shard_views_by_subgroup.size();
            subgroup_ids_by_type[{subgroup_type_count.first, subgroup_index}] = next_subgroup_number;
            uint32_t num_shards = subgroup_shard_views.at(subgroup_index).size() + 1;
            uint32_t max_shard_members = 0;
            for(uint shard_num = 0; shard_num < num_shards; ++shard_num) {
                SubView& shard_view = *subgroup_shard_views.at(subgroup_index).at(shard_num);
                std::size_t shard_size = shard_view.members.size();
                if(shard_size > max_shard_members) {
                    max_shard_members = shard_size;
                }
                auto shard_rank = shard_view.rank_of(curr_view.members[curr_view.my_rank]);
                if(shard_rank != -1) {
                    subgroup_to_shard_n_index[next_subgroup_number] = {shard_num, shard_rank};
                    subgroup_to_num_received_offset[next_subgroup_number] = subgroup_offset;
                    subgroup_to_membership[next_subgroup_number] = shard_view.members;
                }
            }
            /* Pull the shard->SubView mapping out of the subgroup membership list
             * and save it with its new subgroup ID */
            curr_view.shard_views_by_subgroup.emplace_back(
                    std::move(subgroup_shard_views[subgroup_index]));
            subgroup_offset += max_shard_members;
        }
    }
    return subgroup_offset;
}

/* ------------------------- Ken's helper methods ------------------------- */

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
                                   const std::vector<node_id_t>& shard_members) {
    // Ragged cleanup is finished, deliver in the implied order
    std::vector<long long int> max_received_indices(shard_members.size());
    std::string deliveryOrder(" ");
    for(uint n = 0; n < shard_members.size(); n++) {
        deliveryOrder += "Subgroup " + std::to_string(subgroup_num) + " " + std::to_string(Vc.members[Vc.my_rank]) +
                         std::string(":0..") +
                         std::to_string(Vc.gmsSST->globalMin[shard_leader_rank][num_received_offset + n]) +
                         std::string(" ");
        max_received_indices[n] = Vc.gmsSST->globalMin[shard_leader_rank][num_received_offset + n];
    }
    util::debug_log().log_event("Delivering ragged-edge messages in order: " +
                                deliveryOrder);
    //    std::cout << "Delivery Order (View " << Vc.vid << ") {" <<
    //    deliveryOrder << std::string("}") << std::endl;
    Vc.multicast_group->deliver_messages_upto(max_received_indices, subgroup_num, shard_members.size());
}

void ViewManager::ragged_edge_cleanup(View& Vc) {
    util::debug_log().log_event("Running RaggedEdgeCleanup");
    const auto subgroup_to_shard_n_index = Vc.multicast_group->get_subgroup_to_shard_n_index();
    const auto subgroup_to_num_received_offset = Vc.multicast_group->get_subgroup_to_num_received_offset();
    for(const auto p : subgroup_to_shard_n_index) {
        const subgroup_id_t subgroup_id = p.first;
        uint32_t shard_num, shard_rank;
        std::tie(shard_num, shard_rank) = p.second;
        const auto num_received_offset = subgroup_to_num_received_offset.at(subgroup_id);
        const std::vector<node_id_t>& shard_members = curr_view->shard_views_by_subgroup.at(subgroup_id).at(shard_num)->members;

        if(shard_rank == 0) {
            leader_ragged_edge_cleanup(Vc, subgroup_id, num_received_offset, shard_members);
        } else {
            follower_ragged_edge_cleanup(Vc, subgroup_id, num_received_offset, shard_members);
        }
    }
    util::debug_log().log_event("Done with RaggedEdgeCleanup");
}

void ViewManager::leader_ragged_edge_cleanup(View& Vc, const subgroup_id_t subgroup_num,
                                             const uint32_t num_received_offset,
                                             const std::vector<node_id_t>& shard_members) {
    int myRank = Vc.my_rank;
    // int Leader = Vc.rank_of_leader();  // We don't want this to change under our feet
    bool found = false;
    for(uint n = 0; n < shard_members.size() && !found; n++) {
        if(Vc.gmsSST->globalMinReady[n][subgroup_num]) {
            gmssst::set(Vc.gmsSST->globalMin[myRank] + num_received_offset,
                        Vc.gmsSST->globalMin[n] + num_received_offset, shard_members.size());
            found = true;
        }
    }

    if(!found) {
        for(uint n = 0; n < shard_members.size(); n++) {
            int min = Vc.gmsSST->num_received[myRank][num_received_offset + n];
            for(uint r = 0; r < shard_members.size(); r++) {
                const auto node_id = shard_members[r];
                const auto node_rank = Vc.rank_of(node_id);
                if(/*!Vc.failed[r] && */ min > Vc.gmsSST->num_received[node_rank][num_received_offset + n]) {
                    min = Vc.gmsSST->num_received[node_rank][num_received_offset + n];
                }
            }

            gmssst::set(Vc.gmsSST->globalMin[myRank][num_received_offset + n], min);
        }
    }

    util::debug_log().log_event("Shard leader finished computing globalMin");
    gmssst::set(Vc.gmsSST->globalMinReady[myRank][subgroup_num], true);
    Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_num), (char*)std::addressof(Vc.gmsSST->globalMin[0][num_received_offset]) - Vc.gmsSST->getBaseAddress(), sizeof(int) * shard_members.size());
    Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_num), (char*)std::addressof(Vc.gmsSST->globalMinReady[0][subgroup_num]) - Vc.gmsSST->getBaseAddress(), sizeof(bool));

    deliver_in_order(Vc, myRank, subgroup_num, num_received_offset, shard_members);
}


void ViewManager::follower_ragged_edge_cleanup(View& Vc, const subgroup_id_t subgroup_num,
                                               const uint32_t num_received_offset,
                                               const std::vector<node_id_t>& shard_members) {
    int myRank = Vc.my_rank;
    // Learn the leader's data and push it before acting upon it
    util::debug_log().log_event("Received leader's globalMin; echoing it");
    int shard_leader_rank = Vc.rank_of(shard_members[0]);
    gmssst::set(Vc.gmsSST->globalMin[myRank] + num_received_offset, Vc.gmsSST->globalMin[shard_leader_rank] + num_received_offset,
                shard_members.size());
    gmssst::set(Vc.gmsSST->globalMinReady[myRank][subgroup_num], true);
    Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_num), (char*)std::addressof(Vc.gmsSST->globalMin[0][num_received_offset]) - Vc.gmsSST->getBaseAddress(), sizeof(int) * shard_members.size());
    Vc.gmsSST->put(Vc.multicast_group->get_shard_sst_indices(subgroup_num), (char*)std::addressof(Vc.gmsSST->globalMinReady[0][subgroup_num]) - Vc.gmsSST->getBaseAddress(), sizeof(bool));
    deliver_in_order(Vc, shard_leader_rank, subgroup_num, num_received_offset, shard_members);
}


/* ------------------------------------------------------------------------- */

} /* namespace derecho */
