/**
 * @file managed_group_impl.h
 * @brief Contains implementations of all the ManagedGroup functions
 * @date Apr 22, 2016
 * @author Edward
 */

#include <algorithm>
#include <atomic>
#include <cstring>
#include <exception>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <signal.h>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "managed_group.h"

#include "derecho_group.h"
#include "derecho_row.h"
#include "persistence.h"
#include "mutils-serialization/SerializationSupport.hpp"
#include "sst/sst.h"
#include "sst/verbs.h"
#include "view.h"

namespace derecho {

using lock_guard_t = std::lock_guard<std::mutex>;
using unique_lock_t = std::unique_lock<std::mutex>;

template <typename dispatcherType>
ManagedGroup<dispatcherType>::ManagedGroup(
    const ip_addr my_ip,
    dispatcherType _dispatchers,
    CallbackSet callbacks,
    const DerechoParams derecho_params,
    std::vector<view_upcall_t> _view_upcalls,
    const int gms_port)
    : last_suspected(MAX_MEMBERS),
      gms_port(gms_port),
      server_socket(gms_port),
      thread_shutdown(false),
      next_view(nullptr),
      dispatchers(std::move(_dispatchers)),
      view_upcalls(_view_upcalls),
      derecho_params(derecho_params) {
    const node_id_t my_id = 0;
    curr_view = start_group(my_id, my_ip);
    rdmc_sst_setup();
    tcp::socket client_socket = server_socket.accept();
    node_id_t client_id = 0;
    client_socket.exchange(my_id, client_id);
    ip_addr& joiner_ip = client_socket.remote_ip;
    curr_view->num_members++;
    curr_view->member_ips.push_back(joiner_ip);
    curr_view->members.push_back(client_id);
    curr_view->failed.push_back(false);

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
    dispatchers.send_objects(client_socket);
    rdma::impl::verbs_add_connection(client_id, joiner_ip, my_id);
    sst::add_node(client_id, joiner_ip);

    if(!derecho_params.filename.empty()) {
        view_file_name = std::string(derecho_params.filename + persistence::PAXOS_STATE_EXTENSION);
        persist_view(*curr_view, view_file_name);
    }

    std::vector<MessageBuffer> message_buffers;
    auto max_msg_size = DerechoGroup<MAX_MEMBERS, dispatcherType>::compute_max_msg_size(derecho_params.max_payload_size, derecho_params.block_size);
    while(message_buffers.size() < derecho_params.window_size * MAX_MEMBERS) {
        message_buffers.emplace_back(max_msg_size);
    }

    log_event("Initializing SST and RDMC for the first time.");
    setup_derecho(message_buffers, callbacks, derecho_params);
    curr_view->gmsSST->put();
    curr_view->gmsSST->sync_with_members();
    log_event("Done setting up initial SST and RDMC");

    create_threads();
    register_predicates();
    curr_view->gmsSST->start_predicate_evaluation();
    log_event("Starting predicate evaluation");

    view_upcalls.push_back([this](std::vector<node_id_t> new_members,
                                  std::vector<node_id_t> old_members) {
        std::vector<node_id_t> removed_members;
        std::set_difference(
                old_members.begin(), old_members.end(), new_members.begin(),
                new_members.end(),
                std::back_inserter(removed_members));
        curr_view->derecho_group->set_exceptions_for_removed_nodes(removed_members);
    });
    lock_guard_t lock(view_mutex);
    vector<node_id_t> old_members(curr_view->members.begin(),
                                  curr_view->members.end() - 1);
    for(auto& view_upcall : view_upcalls) {
        view_upcall(curr_view->members, old_members);
    }
    // curr_view->derecho_group->debug_print();
}

template <typename dispatcherType>
ManagedGroup<dispatcherType>::ManagedGroup(const node_id_t my_id,
                                           const ip_addr my_ip,
                                           const node_id_t leader_id,
                                           const ip_addr leader_ip,
                                           dispatcherType _dispatchers,
                                           CallbackSet callbacks,
                                           std::vector<view_upcall_t> _view_upcalls,
                                           const int gms_port)
    : last_suspected(MAX_MEMBERS),
      gms_port(gms_port),
      server_socket(gms_port),
      thread_shutdown(false),
      next_view(nullptr),
      dispatchers(std::move(_dispatchers)),
      view_upcalls(_view_upcalls),
      derecho_params(0, 0) {
    curr_view = join_existing(my_id, leader_ip, gms_port);
    curr_view->my_rank = curr_view->rank_of(my_id);
    rdmc_sst_setup();
    if(!derecho_params.filename.empty()) {
        view_file_name = std::string(derecho_params.filename + persistence::PAXOS_STATE_EXTENSION);
        persist_view(*curr_view, view_file_name);
    }
    log_event("Initializing SST and RDMC for the first time.");

    std::vector<MessageBuffer> message_buffers;
    auto max_msg_size = DerechoGroup<MAX_MEMBERS, dispatcherType>::compute_max_msg_size(derecho_params.max_payload_size, derecho_params.block_size);
    while(message_buffers.size() < derecho_params.window_size * MAX_MEMBERS) {
        message_buffers.emplace_back(max_msg_size);
    }
    setup_derecho(message_buffers, callbacks, derecho_params);
    curr_view->gmsSST->put();
    curr_view->gmsSST->sync_with_members();
    log_event("Done setting up initial SST and RDMC");

    if(curr_view->vid != 0) {
        // If this node is joining an existing group with a non-initial view, copy the leader's nChanges and nAcked
        // Otherwise, you'll immediately think that there's as new proposed view change because [leader].nChanges > nAcked
        gmssst::init_from_existing(
            (*curr_view->gmsSST)[curr_view->my_rank],
            (*curr_view->gmsSST)[curr_view->rank_of_leader()]);
        curr_view->gmsSST->put();
        log_event("Joining node initialized its SST row from the leader");
    }

    create_threads();
    register_predicates();
    curr_view->gmsSST->start_predicate_evaluation();
    log_event("Starting predicate evaluation");

    view_upcalls.push_back([this](std::vector<node_id_t> new_members,
                                  std::vector<node_id_t> old_members) {
        std::vector<node_id_t> removed_members;
        std::set_difference(
                old_members.begin(), old_members.end(), new_members.begin(),
                new_members.end(),
                std::back_inserter(removed_members));
        curr_view->derecho_group->set_exceptions_for_removed_nodes(removed_members);
    });
    lock_guard_t lock(view_mutex);
    vector<node_id_t> old_members(curr_view->members.begin(),
                                  curr_view->members.end() - 1);
    for(auto& view_upcall : view_upcalls) {
        view_upcall(curr_view->members, old_members);
    }
    // curr_view->derecho_group->debug_print();
}

template <typename dispatcherType>
ManagedGroup<dispatcherType>::ManagedGroup(const std::string& recovery_filename,
                                           const node_id_t my_id,
                                           const ip_addr my_ip,
                                           dispatcherType _dispatchers,
                                           CallbackSet callbacks,
                                           DerechoParams derecho_params,
                                           std::vector<view_upcall_t> _view_upcalls,
                                           const int gms_port)
    : last_suspected(MAX_MEMBERS),
      gms_port(gms_port),
      server_socket(gms_port),
      thread_shutdown(false),
      view_file_name(recovery_filename + persistence::PAXOS_STATE_EXTENSION),
      dispatchers(std::move(_dispatchers)),
      view_upcalls(_view_upcalls),
      derecho_params(derecho_params) {
    auto last_view = load_view<dispatcherType>(view_file_name);
    std::vector<MessageBuffer> message_buffers;
    auto max_msg_size = DerechoGroup<MAX_MEMBERS, dispatcherType>::compute_max_msg_size(derecho_params.max_payload_size, derecho_params.block_size);
    while(message_buffers.size() < derecho_params.window_size * MAX_MEMBERS) {
        message_buffers.emplace_back(max_msg_size);
    }

    if(my_id != last_view->members[last_view->rank_of_leader()]) {
        curr_view = join_existing(my_id, last_view->member_ips[last_view->rank_of_leader()], gms_port, derecho_params);
    } else {
        /* This should only happen if an entire group failed and the leader is restarting;
         * otherwise the view obtained from the recovery script will have a leader that is
         * not me. So reset to an empty view and wait for the first non-leader member to
         * restart and join. */
        curr_view = start_group(my_id, my_ip);
        curr_view->vid = last_view->vid + 1;
        tcp::socket client_socket = server_socket.accept();
        node_id_t client_id = 0;
        client_socket.exchange(my_id, client_id);
        ip_addr& joiner_ip = client_socket.remote_ip;
        curr_view->num_members++;
        curr_view->member_ips.push_back(joiner_ip);
        curr_view->members.push_back(client_id);
        curr_view->failed.push_back(false);

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
    }
    curr_view->my_rank = curr_view->rank_of(my_id);

    log_event("Initializing SST and RDMC for the first time.");
    derecho_params.filename = recovery_filename;
    setup_derecho(message_buffers,
                  callbacks,
                  derecho_params);
    curr_view->gmsSST->put();
    curr_view->gmsSST->sync_with_members();
    log_event("Done setting up initial SST and RDMC");
    //Initialize nChanges and nAcked in the local SST row to the saved view's VID, so the next proposed change is detected
    gmssst::init((*curr_view->gmsSST)[curr_view->my_rank], curr_view->vid);

    create_threads();
    register_predicates();
    log_event("Starting predicate evaluation");
    curr_view->gmsSST->start_predicate_evaluation();

    view_upcalls.push_back([this](std::vector<node_id_t> new_members,
                                  std::vector<node_id_t> old_members) {
        std::vector<node_id_t> removed_members;
        std::set_difference(
                old_members.begin(), old_members.end(), new_members.begin(),
                new_members.end(),
                std::back_inserter(removed_members));
        curr_view->derecho_group->set_exceptions_for_removed_nodes(removed_members);
    });

    lock_guard_t lock(view_mutex);
    vector<node_id_t> old_members(curr_view->members.begin(),
                                  curr_view->members.end() - 1);
    for(auto& view_upcall : view_upcalls) {
        view_upcall(curr_view->members, old_members);
    }
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::rdmc_sst_setup() {
    cout << "Doing global setup of RDMC and SST" << endl;
    // construct member_ips
    auto member_ips_map = get_member_ips_map(curr_view->members, curr_view->member_ips, curr_view->failed);
    if(!rdmc::initialize(member_ips_map, curr_view->members[curr_view->my_rank])) {
        cout << "Global setup failed" << endl;
        exit(0);
    }
    sst::verbs_initialize(member_ips_map, curr_view->members[curr_view->my_rank]);
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::create_threads() {
    client_listener_thread = std::thread{[this]() {
        while(!thread_shutdown) {
            tcp::socket client_socket = server_socket.accept();
	    util::debug_log().log_event(std::stringstream() << "Background thread got a client connection from " << client_socket.remote_ip);
            pending_joins.locked().access.emplace_back(std::move(client_socket));
        }
        cout << "Connection listener thread shutting down." << endl;
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
        cout << "Old View cleanup thread shutting down." << endl;
    });
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::register_predicates() {
    using DerechoSST = typename View<dispatcherType>::DerechoSST;

    auto suspected_changed = [this](const DerechoSST& sst) {
        return suspected_not_equal(sst, last_suspected);
    };
    auto suspected_changed_trig = [this](DerechoSST& gmsSST) {
        log_event("Suspected[] changed");
        View<dispatcherType>& Vc = *curr_view;
        int myRank = curr_view->my_rank;
        // These fields had better be synchronized.
        assert(gmsSST.get_local_index() == curr_view->my_rank);
        // Aggregate suspicions into gmsSST[myRank].Suspected;
        for(int r = 0; r < Vc.num_members; r++) {
            for(int who = 0; who < Vc.num_members; who++) {
                gmssst::set(gmsSST[myRank].suspected[who], gmsSST[myRank].suspected[who] || gmsSST[r].suspected[who]);
            }
        }

        for(int q = 0; q < Vc.num_members; q++) {
            if(gmsSST[myRank].suspected[q] && !Vc.failed[q]) {
                log_event(std::stringstream() << "Marking " << Vc.members[q] << " failed");
                if(Vc.nFailed >= (Vc.num_members + 1) / 2) {
                    throw derecho_exception("Majority of a Derecho group simultaneously failed ... shutting down");
                }

                log_event(std::stringstream() << "GMS telling SST to freeze row " << q << " which is node " << Vc.members[q]);
                gmsSST.freeze(q);  // Cease to accept new updates from q
                Vc.derecho_group->wedge();
                gmssst::set(gmsSST[myRank].wedged, true);  // RDMC has halted new sends and receives in theView
                Vc.failed[q] = true;
                Vc.nFailed++;

                if (Vc.nFailed >= (Vc.num_members + 1) / 2) {
                    throw derecho_exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
                }

                gmsSST.put();
                if(Vc.IAmLeader() && !changes_contains(gmsSST, Vc.members[q]))  // Leader initiated
                {
                    if((gmsSST[myRank].nChanges - gmsSST[myRank].nCommitted) == MAX_MEMBERS) {
                        throw derecho_exception("Ran out of room in the pending changes list");
                    }

                    gmssst::set(gmsSST[myRank].changes[gmsSST[myRank].nChanges % MAX_MEMBERS], Vc.members[q]);  // Reports the failure (note that q NotIn members)
                    gmssst::increment(gmsSST[myRank].nChanges);
                    log_event(std::stringstream() << "Leader proposed a change to remove failed node " << Vc.members[q]);
                    gmsSST.put();
                }
            }
        }
        copy_suspected(gmsSST, last_suspected);
    };

    // Only start one join at a time
    auto start_join_pred = [this](const DerechoSST& sst) {
        return curr_view->IAmLeader() && has_pending_join() &&
               joining_client_socket.is_empty();
    };
    auto start_join_trig = [this](DerechoSST& sst) {
        log_event("GMS handling a new client connection");
        //C++'s ugly two-step dequeue: leave queue.front() in an invalid state, then delete it
        joining_client_socket = std::move(pending_joins.locked().access.front());
        pending_joins.locked().access.pop_front();
        receive_join(joining_client_socket);
    };

    auto change_commit_ready = [this](const DerechoSST& gmsSST) {
        return curr_view->my_rank == curr_view->rank_of_leader() &&
               min_acked(gmsSST, curr_view->failed) > gmsSST[gmsSST.get_local_index()].nCommitted;
    };
    auto commit_change = [this](DerechoSST& gmsSST) {
        gmssst::set(gmsSST[gmsSST.get_local_index()].nCommitted,
            min_acked(gmsSST, curr_view->failed));  // Leader commits a new request
        log_event(std::stringstream() << "Leader committing view proposal #" << gmsSST[gmsSST.get_local_index()].nCommitted);
        gmsSST.put();
    };

    auto leader_proposed_change = [this](const DerechoSST& gmsSST) {
        return gmsSST[curr_view->rank_of_leader()].nChanges >
               gmsSST[gmsSST.get_local_index()].nAcked;
    };
    auto ack_proposed_change = [this](DerechoSST& gmsSST) {
        // These fields had better be synchronized.
        assert(gmsSST.get_local_index() == curr_view->my_rank);
        int myRank = gmsSST.get_local_index();
        int leader = curr_view->rank_of_leader();
        log_event(std::stringstream() << "Detected that leader proposed view change #" << gmsSST[leader].nChanges << ". Acknowledging.");
        if(myRank != leader) {
            // Echo (copy) the vector including the new changes
            gmssst::set(gmsSST[myRank].changes, gmsSST[leader].changes);
            // Echo the new member's IP
            gmssst::set(gmsSST[myRank].joiner_ip, gmsSST[leader].joiner_ip);
            // Echo the count
            gmssst::set(gmsSST[myRank].nChanges, gmsSST[leader].nChanges);
            gmssst::set(gmsSST[myRank].nCommitted, gmsSST[leader].nCommitted);
        }

        // Notice a new request, acknowledge it
        gmssst::set(gmsSST[myRank].nAcked, gmsSST[leader].nChanges);
        gmsSST.put();
        log_event("Wedging current view.");
        curr_view->wedge();
        log_event("Done wedging current view.");

    };

    auto leader_committed_next_view = [this](const DerechoSST& gmsSST) {
        return gmsSST[curr_view->rank_of_leader()].nCommitted > curr_view->vid;
    };
    auto start_view_change = [this](DerechoSST& gmsSST) {
        log_event(std::stringstream() << "Starting view change to view " << curr_view->vid + 1);
        // Disable all the other SST predicates, except suspected_changed and the one I'm about to register
        gmsSST.predicates.remove(start_join_handle);
        gmsSST.predicates.remove(change_commit_ready_handle);
        gmsSST.predicates.remove(leader_proposed_handle);

        View<dispatcherType>& Vc = *curr_view;
        int myRank = curr_view->my_rank;
        // These fields had better be synchronized.
        assert(gmsSST.get_local_index() == curr_view->my_rank);

        Vc.wedge();
        node_id_t currChangeID = gmsSST[myRank].changes[Vc.vid % MAX_MEMBERS];
        next_view = std::make_unique<View<dispatcherType>>();
        next_view->vid = Vc.vid + 1;
        next_view->IKnowIAmLeader = Vc.IKnowIAmLeader;
        node_id_t myID = Vc.members[myRank];
        bool failed;
        int whoFailed = Vc.rank_of(currChangeID);
        if(whoFailed != -1) {
            failed = true;
            next_view->nFailed = Vc.nFailed - 1;
            next_view->num_members = Vc.num_members - 1;
            next_view->init_vectors();
        } else {
            failed = false;
            next_view->nFailed = Vc.nFailed;
            next_view->num_members = Vc.num_members;
            int new_member_rank = next_view->num_members++;
            next_view->init_vectors();
            next_view->members[new_member_rank] = currChangeID;
            next_view->member_ips[new_member_rank] = std::string(const_cast<cstring&>(gmsSST[myRank].joiner_ip));
        }

        int m = 0;
        for(int n = 0; n < Vc.num_members; n++) {
            if(n != whoFailed) {
                next_view->members[m] = Vc.members[n];
                next_view->member_ips[m] = Vc.member_ips[n];
                next_view->failed[m] = Vc.failed[n];
                ++m;
            }
        }

        next_view->who = std::make_shared<node_id_t>(currChangeID);
        if((next_view->my_rank = next_view->rank_of(myID)) == -1) {
            throw derecho_exception((std::stringstream() << "Some other node reported that I failed.  Node " << myID << " terminating").str());
        }

        if(next_view->gmsSST != nullptr) {
            std::cout << "Segmentation fault" << std::endl; raise(SIGSEGV);
            throw derecho_exception("Overwriting the SST");
        }

        // At this point we need to await "meta wedged."
        // To do that, we create a predicate that will fire when meta wedged is
        // true, and put the rest of the code in its trigger.

        auto is_meta_wedged = [this](const DerechoSST& gmsSST) mutable {
            for(int n = 0; n < gmsSST.get_num_rows(); ++n) {
                if(!curr_view->failed[n] && !gmsSST[n].wedged) {
                    return false;
                }
            }
            return true;
        };
        auto meta_wedged_continuation = [this, failed, whoFailed](DerechoSST& gmsSST) {
            log_event("MetaWedged is true; continuing view change");
            unique_lock_t lock(view_mutex);
            assert(next_view);

            auto globalMin_ready_continuation = [this, failed, whoFailed](DerechoSST& gmsSST) {
                lock_guard_t lock(view_mutex);
                assert(next_view);

                ragged_edge_cleanup(*curr_view);
                if(curr_view->IAmLeader() && !failed) {
                    // Send the view to the newly joined client before we try to do SST and RDMC setup
                    commit_join(*next_view, joining_client_socket);
		    curr_view->derecho_group->send_objects(joining_client_socket);
                    // Close the client's socket
                    joining_client_socket = tcp::socket();
                }

                // Delete the last two GMS predicates from the old SST in preparation for deleting it
                gmsSST.predicates.remove(leader_committed_handle);
                gmsSST.predicates.remove(suspected_changed_handle);

                log_event(std::stringstream() << "Starting creation of new SST and DerechoGroup for view " << next_view->vid);
		// if  a new member has joined
		if (curr_view->members.size() < next_view->members.size()) {
		  rdma::impl::verbs_add_connection(next_view->members.back(), next_view->member_ips.back(), next_view->members[next_view->my_rank]);
		  sst::add_node(next_view->members.back(), next_view->member_ips.back());
		}
                // This will block until everyone responds to SST/RDMC initial handshakes
                transition_sst_and_rdmc(*next_view, whoFailed);
                next_view->gmsSST->put();
                next_view->gmsSST->sync_with_members();
                log_event(std::stringstream() << "Done setting up SST and DerechoGroup for view " << next_view->vid);
                // for view upcall
                auto old_members = curr_view->members;
                {
                    lock_guard_t old_views_lock(old_views_mutex);
                    old_views.push(std::move(curr_view));
                    old_views_cv.notify_all();
                }
		curr_view = std::move(next_view);

		// Announce the new view to the application
                curr_view->newView(*curr_view);

                //If in persistent mode, write the new view to disk before using it
                if(!view_file_name.empty()) {
                    persist_view(*curr_view, view_file_name);
                }


                // Register predicates in the new view
                register_predicates();
                curr_view->gmsSST->start_predicate_evaluation();

                // First task with my new view...
                if(curr_view->IAmTheNewLeader())  // I'm the new leader and everyone who hasn't failed agrees
                {
                    curr_view->merge_changes();  // Create a combined list of Changes
                }

                for(auto& view_upcall : view_upcalls) {
                    view_upcall(curr_view->members, old_members);
                }

                view_change_cv.notify_all();
            };

            if(curr_view->IAmLeader()) {
                // The leader doesn't need to wait any more, it can execute continuously from here.
                lock.unlock();
                globalMin_ready_continuation(gmsSST);
            } else {
                // Non-leaders need another level of continuation to wait for GlobalMinReady
                auto leader_globalMin_is_ready = [this](const DerechoSST& gmsSST) {
                    return gmsSST[curr_view->rank_of_leader()].globalMinReady;
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

template <typename dispatcherType>
ManagedGroup<dispatcherType>::~ManagedGroup() {
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

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::setup_derecho(std::vector<MessageBuffer>& message_buffers,
                                                 CallbackSet callbacks,
                                                 const DerechoParams& derecho_params) {
    curr_view->gmsSST = std::make_shared<sst::SST<DerechoRow<MAX_MEMBERS>>>(
        curr_view->members, curr_view->members[curr_view->my_rank],
        [this](const uint32_t node_id) { report_failure(node_id); }, curr_view->failed);
    for(int r = 0; r < curr_view->num_members; ++r) {
        gmssst::init((*curr_view->gmsSST)[r]);
    }
    gmssst::set((*curr_view->gmsSST)[curr_view->my_rank].vid, curr_view->vid);

    curr_view->derecho_group = std::make_unique<DerechoGroup<MAX_MEMBERS, dispatcherType>>(
        curr_view->members, curr_view->members[curr_view->my_rank],
        curr_view->gmsSST, message_buffers, std::move(dispatchers), callbacks, derecho_params,
        get_member_ips_map(curr_view->members, curr_view->member_ips, curr_view->failed),
        curr_view->failed);
}

/**
 *
 * @param newView The new view in which to construct an SST and derecho_group
 * @param whichFailed The rank of the node in the old view that failed, if any.
 */
template <typename dispatcherType>
void ManagedGroup<dispatcherType>::transition_sst_and_rdmc(View<dispatcherType>& newView, int whichFailed) {
    //Temporarily disabled because all nodes are initialized at the beginning
    //    if(whichFailed == -1) { //This is a join
    //        rdmc::add_address(newView.members.back(), newView.member_ips.back());
    //    }
    //
    //    std::map<node_id_t, ip_addr> new_member_map {{newView.members[newView.my_rank], newView.member_ips[newView.my_rank]}, {newView.members.back(), newView.member_ips.back()}};
    //    sst::tcp::tcp_initialize(newView.members[newView.my_rank], new_member_map);
    newView.gmsSST = std::make_shared<sst::SST<DerechoRow<MAX_MEMBERS>>>(
        newView.members, newView.members[newView.my_rank],
        [this](const uint32_t node_id) { report_failure(node_id); }, newView.failed);
    std::cout << "Going to create the derecho group" << std::endl;
    newView.derecho_group = std::make_unique<DerechoGroup<MAX_MEMBERS, dispatcherType>>(
        newView.members, newView.members[newView.my_rank], newView.gmsSST,
        std::move(*curr_view->derecho_group),
        get_member_ips_map(newView.members, newView.member_ips, newView.failed), newView.failed);
    curr_view->derecho_group.reset();

    // Initialize this node's row in the new SST
    gmssst::template init_from_existing<MAX_MEMBERS>((*newView.gmsSST)[newView.my_rank], (*curr_view->gmsSST)[curr_view->my_rank]);
    gmssst::set((*newView.gmsSST)[newView.my_rank].vid, newView.vid);
}

template <typename dispatcherType>
std::unique_ptr<View<dispatcherType>> ManagedGroup<dispatcherType>::start_group(const node_id_t my_id, const ip_addr my_ip) {
    log_event("Starting new empty group with myself as leader");
    std::unique_ptr<View<dispatcherType>> newView = std::make_unique<View<dispatcherType>>(1);
    newView->members[0] = my_id;
    newView->my_rank = 0;
    newView->member_ips[0] = my_ip;
    newView->failed[0] = false;
    newView->IKnowIAmLeader = true;
    return newView;
}

template <typename dispatcherType>
std::unique_ptr<View<dispatcherType>> ManagedGroup<dispatcherType>::join_existing(
    const node_id_t my_id, const ip_addr& leader_ip, const int leader_port) {
    //    cout << "Joining group by contacting node at " << leader_ip << endl;
    log_event("Joining group: waiting for a response from the leader");
    tcp::socket leader_socket{leader_ip, leader_port};
    //Temporarily disabled because all node IDs are fixed at startup
    //First the leader sends the node ID this client has been assigned
    //    node_id_t myNodeID;
    //    bool success = leader_socket.read((char*)&myNodeID,sizeof(myNodeID));
    //    assert(success);

    // exchange ids
    node_id_t leader_id = 0;
    leader_socket.exchange(my_id, leader_id);

    //The leader will first send the size of the necessary buffer, then the serialized View
    std::size_t size_of_view;
    bool success = leader_socket.read((char*)&size_of_view, sizeof(size_of_view));
    assert(success);
    char buffer[size_of_view];
    success = leader_socket.read(buffer, size_of_view);
    assert(success);
    std::unique_ptr<View<dispatcherType>> newView = mutils::from_bytes<View<dispatcherType>>(nullptr, buffer);
    std::size_t size_of_derecho_params;
    success = leader_socket.read((char*)&size_of_derecho_params, sizeof(size_of_derecho_params));
    char buffer2[size_of_derecho_params];
    success = leader_socket.read(buffer2, size_of_derecho_params);
    assert(success);
    std::unique_ptr<DerechoParams> params_ptr = mutils::from_bytes<DerechoParams>(nullptr, buffer2);
    derecho_params = *params_ptr;
    dispatchers.receive_objects(leader_socket);

    log_event("Received View from leader");
    return newView;
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::receive_join(tcp::socket& client_socket) {
    ip_addr& joiner_ip = client_socket.remote_ip;
    using derechoSST = sst::SST<DerechoRow<View<dispatcherType>::MAX_MEMBERS>>;
    derechoSST& gmsSST = *curr_view->gmsSST;
    if((gmsSST[curr_view->my_rank].nChanges -
        gmsSST[curr_view->my_rank].nCommitted) == MAX_MEMBERS / 2) {
        throw derecho_exception("Too many changes to allow a Join right now");
    }

    node_id_t joining_client_id = 0;
    client_socket.exchange(curr_view->members[curr_view->my_rank], joining_client_id);

    log_event(std::stringstream() << "Proposing change to add node " << joining_client_id);
    size_t next_change = gmsSST[curr_view->my_rank].nChanges % MAX_MEMBERS;
    gmssst::set(gmsSST[curr_view->my_rank].changes[next_change], joining_client_id);
    gmssst::set(gmsSST[curr_view->my_rank].joiner_ip, joiner_ip);

    gmssst::increment(gmsSST[curr_view->my_rank].nChanges);

    log_event(std::stringstream() << "Wedging view " << curr_view->vid);
    curr_view->wedge();
    log_event("Leader done wedging view.");
    gmsSST.put();
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::commit_join(const View<dispatcherType>& new_view,
                                               tcp::socket& client_socket) {
    log_event("Sending client the new view");
    // Temporarily disabled because all node IDs are globally fixed at startup
    //    client_socket.write((char*) &joining_client_id, sizeof(joining_client_id));
    auto bind_socket_write = [&client_socket](const char* bytes, std::size_t size) {client_socket.write(bytes, size); };
    std::size_t size_of_view = mutils::bytes_size(new_view);
    client_socket.write((char*)&size_of_view, sizeof(size_of_view));
    mutils::post_object(bind_socket_write, new_view);
    std::size_t size_of_derecho_params = mutils::bytes_size(derecho_params);
    client_socket.write((char*)&size_of_derecho_params, sizeof(size_of_derecho_params));
    mutils::post_object(bind_socket_write, derecho_params);
}

/* ------------------------- Ken's helper methods ------------------------- */

template <typename dispatcherType>
bool ManagedGroup<dispatcherType>::suspected_not_equal(
    const typename View<dispatcherType>::DerechoSST& gmsSST,
    const vector<bool>& old) {
    for(int r = 0; r < gmsSST.get_num_rows(); r++) {
        for(int who = 0; who < MAX_MEMBERS; who++) {
            if(gmsSST[r].suspected[who] && !old[who]) {
                return true;
            }
        }
    }
    return false;
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::copy_suspected(
    const typename View<dispatcherType>::DerechoSST& gmsSST, vector<bool>& old) {
    for(int who = 0; who < gmsSST.get_num_rows(); ++who) {
        old[who] = gmsSST[gmsSST.get_local_index()].suspected[who];
    }
}

template <typename dispatcherType>
bool ManagedGroup<dispatcherType>::changes_contains(
    const typename View<dispatcherType>::DerechoSST& gmsSST, const node_id_t q) {
    auto& myRow = gmsSST[gmsSST.get_local_index()];
    for(int n = myRow.nCommitted; n < myRow.nChanges; n++) {
        int p_index = n % MAX_MEMBERS;
        const node_id_t p(const_cast<node_id_t&>(myRow.changes[p_index]));
        if(p_index < myRow.nChanges && p == q) {
            return true;
        }
    }
    return false;
}

template <typename dispatcherType>
int ManagedGroup<dispatcherType>::min_acked(
    const typename View<dispatcherType>::DerechoSST& gmsSST, const vector<char>& failed) {
    int myRank = gmsSST.get_local_index();
    int min = gmsSST[myRank].nAcked;
    for(size_t n = 0; n < failed.size(); n++) {
        if(!failed[n] && gmsSST[n].nAcked < min) {
            min = gmsSST[n].nAcked;
        }
    }

    return min;
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::deliver_in_order(const View<dispatcherType>& Vc, int Leader) {
    // Ragged cleanup is finished, deliver in the implied order
    std::vector<long long int> max_received_indices(Vc.num_members);
    std::string deliveryOrder(" ");
    for(int n = 0; n < Vc.num_members; n++) {
        deliveryOrder += std::to_string(Vc.members[Vc.my_rank]) +
                         std::string(":0..") +
                         std::to_string((*Vc.gmsSST)[Leader].globalMin[n]) +
                         std::string(" ");
        max_received_indices[n] = (*Vc.gmsSST)[Leader].globalMin[n];
    }
    util::debug_log().log_event("Delivering ragged-edge messages in order: " +
                                deliveryOrder);
    //    std::cout << "Delivery Order (View " << Vc.vid << ") {" <<
    //    deliveryOrder << std::string("}") << std::endl;
    Vc.derecho_group->deliver_messages_upto(max_received_indices);
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::ragged_edge_cleanup(View<dispatcherType>& Vc) {
    util::debug_log().log_event(
        "Running RaggedEdgeCleanup");
    if(Vc.IAmLeader()) {
        leader_ragged_edge_cleanup(Vc);
    } else {
        follower_ragged_edge_cleanup(Vc);
    }
    util::debug_log().log_event("Done with RaggedEdgeCleanup");
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::leader_ragged_edge_cleanup(View<dispatcherType>& Vc) {
    int myRank = Vc.my_rank;
    int Leader = Vc.rank_of_leader();  // We don't want this to change under our feet
    bool found = false;
    for(int n = 0; n < Vc.num_members && !found; n++) {
        if((*Vc.gmsSST)[n].globalMinReady) {
            gmssst::set((*Vc.gmsSST)[myRank].globalMin,
                        (*Vc.gmsSST)[n].globalMin, Vc.num_members);
            found = true;
        }
    }

    if(!found) {
        for(int n = 0; n < Vc.num_members; n++) {
            int min = (*Vc.gmsSST)[myRank].nReceived[n];
            for(int r = 0; r < Vc.num_members; r++) {
                if(/*!Vc.failed[r] && */ min > (*Vc.gmsSST)[r].nReceived[n]) {
                    min = (*Vc.gmsSST)[r].nReceived[n];
                }
            }

            gmssst::set((*Vc.gmsSST)[myRank].globalMin[n], min);
        }
    }

    util::debug_log().log_event("Leader finished computing globalMin");
    gmssst::set((*Vc.gmsSST)[myRank].globalMinReady, true);
    Vc.gmsSST->put();

    deliver_in_order(Vc, Leader);
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::follower_ragged_edge_cleanup(View<dispatcherType>& Vc) {
    int myRank = Vc.my_rank;
    // Learn the leader's data and push it before acting upon it
    util::debug_log().log_event("Received leader's globalMin; echoing it");
    int Leader = Vc.rank_of_leader();
    gmssst::set((*Vc.gmsSST)[myRank].globalMin, (*Vc.gmsSST)[Leader].globalMin,
                Vc.num_members);
    gmssst::set((*Vc.gmsSST)[myRank].globalMinReady, true);
    Vc.gmsSST->put();
    deliver_in_order(Vc, Leader);
}

/* ------------------------------------------------------------------------- */

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::report_failure(const node_id_t who) {
    int r = curr_view->rank_of(who);
    log_event(std::stringstream() << "Node ID " << who << " failure reported; marking suspected[" << r << "]");
    cout << "Node ID " << who << " failure reported; marking suspected[" << r << "]" << endl;
    (*curr_view->gmsSST)[curr_view->my_rank].suspected[r] = true;
    int cnt = 0;
    for(r = 0; r < MAX_MEMBERS; r++) {
        if((*curr_view->gmsSST)[curr_view->my_rank].suspected[r]) {
            ++cnt;
        }
    }

    if(cnt >= (curr_view->num_members + 1) / 2) {
        throw derecho_exception(
            "Potential partitioning event: this node is no longer in the majority and must shut down!");
    }
    curr_view->gmsSST->put();
    std::cout << "Exiting from remote_failure" << std::endl;
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::leave() {
    lock_guard_t lock(view_mutex);
    log_event("Cleanly leaving the group.");
    curr_view->derecho_group->wedge();
    curr_view->gmsSST->delete_all_predicates();
    (*curr_view->gmsSST)[curr_view->my_rank].suspected[curr_view->my_rank] = true;
    curr_view->gmsSST->put();
    thread_shutdown = true;
}

template <typename dispatcherType>
char* ManagedGroup<dispatcherType>::get_sendbuffer_ptr(unsigned long long int payload_size, int pause_sending_turns, bool cooked_send) {
    lock_guard_t lock(view_mutex);
    return curr_view->derecho_group->get_position(payload_size, pause_sending_turns, cooked_send);
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::send() {
    std::unique_lock<std::mutex> lock(view_mutex);
    while(true) {
        if(curr_view->derecho_group->send()) break;
        view_change_cv.wait(lock);
    }
}

template <typename dispatcherType>
template <typename IdClass, unsigned long long tag, typename... Args>
void ManagedGroup<dispatcherType>::orderedSend(const vector<node_id_t>& nodes,
                                               Args&&... args) {
    char* buf;
    while(!(buf = get_sendbuffer_ptr(0, 0, true))) {
    };

    std::unique_lock<std::mutex> lock(view_mutex);
    curr_view->derecho_group->template orderedSend<IdClass, tag, Args...>(
        nodes, buf, std::forward<Args>(args)...);
}

template <typename dispatcherType>
template <typename IdClass, unsigned long long tag, typename... Args>
void ManagedGroup<dispatcherType>::orderedSend(Args&&... args) {
    char* buf;
    while(!(buf = get_sendbuffer_ptr(0, 0, true))) {
    };

    std::unique_lock<std::mutex> lock(view_mutex);
    curr_view->derecho_group->template orderedSend<IdClass, tag, Args...>(buf,
                                                                          std::forward<Args>(args)...);
}

template <typename dispatcherType>
template <typename IdClass, unsigned long long tag, typename... Args>
auto ManagedGroup<dispatcherType>::orderedQuery(const vector<node_id_t>& nodes,
                                                Args&&... args) {
    char* buf;
    while(!(buf = get_sendbuffer_ptr(0, 0, true))) {
    };

    std::unique_lock<std::mutex> lock(view_mutex);
    return curr_view->derecho_group->template orderedQuery<IdClass, tag, Args...>(
        nodes, buf, std::forward<Args>(args)...);
}

template <typename dispatcherType>
template <typename IdClass, unsigned long long tag, typename... Args>
auto ManagedGroup<dispatcherType>::orderedQuery(Args&&... args) {
    char* buf;
    while(!(buf = get_sendbuffer_ptr(0, 0, true))) {
    };

    std::unique_lock<std::mutex> lock(view_mutex);
    return curr_view->derecho_group->template orderedQuery<IdClass, tag, Args...>(buf,
                                                                                  std::forward<Args>(args)...);
}

template <typename dispatcherType>
template <typename IdClass, unsigned long long tag, typename... Args>
void ManagedGroup<dispatcherType>::p2pSend(node_id_t dest_node, Args&&... args) {
    curr_view->derecho_group->template p2pSend<IdClass, tag, Args...>(
        dest_node, std::forward<Args>(args)...);
}

template <typename dispatcherType>
template <typename IdClass, unsigned long long tag, typename... Args>
auto ManagedGroup<dispatcherType>::p2pQuery(node_id_t dest_node, Args&&... args) {
    return curr_view->derecho_group->template p2pQuery<IdClass, tag, Args...>(
        dest_node, std::forward<Args>(args)...);
}

template <typename dispatcherType>
std::vector<node_id_t> ManagedGroup<dispatcherType>::get_members() {
    lock_guard_t lock(view_mutex);
    return curr_view->members;
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::barrier_sync() {
    lock_guard_t lock(view_mutex);
    curr_view->gmsSST->sync_with_members();
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::debug_print_status() const {
    cout << "curr_view = " << curr_view->ToString() << endl;
}

template <typename dispatcherType>
void ManagedGroup<dispatcherType>::print_log(std::ostream& output_dest) const {
    for(size_t i = 0; i < util::debug_log().curr_event; ++i) {
        output_dest << util::debug_log().times[i] << "," << util::debug_log().events[i]
                    << "," << std::string("ABCDEFGHIJKLMNOPQRSTUVWXYZ")[curr_view->members[curr_view->my_rank]]
                    << endl;
    }
}

template <typename dispatcherType>
std::map<node_id_t, ip_addr> ManagedGroup<dispatcherType>::get_member_ips_map(
    std::vector<node_id_t>& members, std::vector<ip_addr>& member_ips, std::vector<char> failed) {
    std::map<node_id_t, ip_addr> member_ips_map;
    size_t num_members = members.size();
    for(uint i = 0; i < num_members; ++i) {
        if(!failed[i]) {
            member_ips_map[members[i]] = member_ips[i];
        }
    }
    return member_ips_map;
}

} /* namespace derecho */
