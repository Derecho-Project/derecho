#pragma once

#include <algorithm>
#include <cassert>
#include <chrono>
#include <limits>
#include <thread>

#include "derecho_group.h"
#include "logger.h"

namespace derecho {

using std::chrono::milliseconds;

/**
 * Helper function to find the index of an element in a container.
 */
template <class T, class U>
size_t index_of(T container, U elem) {
    size_t n = 0;
    for(auto it = begin(container); it != end(container); ++it) {
        if(*it == elem) return n;

        n++;
    }
    return container.size();
}

/**
 *
 * @param _members A list of node IDs of members in this group
 * @param my_node_id The rank (ID) of this node in the group
 * @param _sst The SST this group will use; created by the GMS (membership
 * service) for this group.
 * @param _free_message_buffers Message buffers to use for RDMC sending/receiving
 * (there must be one for each sender in the group)
 * @param _max_payload_size The size of the largest possible message that will
 * be sent in this group, in bytes
 * @param _callbacks A set of functions to call when messages have reached
 * various levels of stability
 * @param _block_size The block size to use for RDMC
 * @param _window_size The window size (number of outstanding messages that can
 * be in progress at once before blocking sends) to use when sending a stream
 * of messages to the group; default is 3
 * @param timeout_ms The time that this node will wait for a sender in the group
 * to send its message before concluding that the sender has failed; default is 1ms
 * @param _type The type of RDMC algorithm to use; default is BINOMIAL_SEND
 * @param filename If provided, the name of the file in which to save persistent
 * copies of all messages received. If an empty filename is given (the default),
 * the node runs in non-persistent mode and no persistence callbacks will be
 * issued.
 */
template <typename dispatchersType>
DerechoGroup<dispatchersType>::DerechoGroup(
    std::vector<node_id_t> _members, node_id_t my_node_id,
    std::shared_ptr<DerechoSST> _sst,
    std::vector<MessageBuffer>& _free_message_buffers,
    dispatchersType _dispatchers,
    CallbackSet callbacks,
    const DerechoParams derecho_params,
    std::map<node_id_t, std::string> ip_addrs,
    std::vector<char> already_failed)
        : members(_members),
          num_members(members.size()),
          member_index(index_of(members, my_node_id)),
          block_size(derecho_params.block_size),
          max_msg_size(compute_max_msg_size(derecho_params.max_payload_size, derecho_params.block_size)),
          type(derecho_params.type),
          window_size(derecho_params.window_size),
          callbacks(callbacks),
          dispatchers(std::move(_dispatchers)),
          connections(my_node_id, ip_addrs, derecho_params.rpc_port),
          rdmc_group_num_offset(0),
          sender_timeout(derecho_params.timeout_ms),
          sst(_sst) {
    assert(window_size >= 1);

    if(!derecho_params.filename.empty()) {
        file_writer = std::make_unique<FileWriter>(make_file_written_callback(),
                                                   derecho_params.filename);
    }

    free_message_buffers.swap(_free_message_buffers);
    while(free_message_buffers.size() < window_size * num_members) {
        free_message_buffers.emplace_back(max_msg_size);
    }

    total_message_buffers = free_message_buffers.size();

    p2pBuffer = std::unique_ptr<char[]>(new char[derecho_params.max_payload_size]);
    deliveryBuffer = std::unique_ptr<char[]>(new char[derecho_params.max_payload_size]);

    initialize_sst_row();
    bool no_member_failed = true;
    if(already_failed.size()) {
        for(int i = 0; i < num_members; ++i) {
            if(already_failed[i]) {
                no_member_failed = false;
                break;
            }
        }
    }
    if(!already_failed.size() || no_member_failed) {
        // if groups are created successfully, rdmc_groups_created will be set
        // to true
        rdmc_groups_created = create_rdmc_groups();
    }
    register_predicates();
    sender_thread = std::thread(&DerechoGroup::send_loop, this);
    timeout_thread = std::thread(&DerechoGroup::check_failures_loop, this);
    rpc_thread = std::thread(&DerechoGroup::rpc_process_loop, this);
    //    cout << "DerechoGroup: Registered predicates and started thread" <<
    //    std::endl;
}

template <typename dispatchersType>
DerechoGroup<dispatchersType>::DerechoGroup(
    std::vector<node_id_t> _members, node_id_t my_node_id,
    std::shared_ptr<DerechoSST> _sst,
    DerechoGroup&& old_group, std::map<node_id_t, std::string> ip_addrs,
    std::vector<char> already_failed, uint32_t rpc_port)
        : members(_members),
          num_members(members.size()),
          member_index(index_of(members, my_node_id)),
          block_size(old_group.block_size),
          max_msg_size(old_group.max_msg_size),
          type(old_group.type),
          window_size(old_group.window_size),
          callbacks(old_group.callbacks),
          dispatchers(std::move(old_group.dispatchers)),
          connections(my_node_id, ip_addrs, rpc_port),
          toFulfillQueue(std::move(old_group.toFulfillQueue)),
          fulfilledList(std::move(old_group.fulfilledList)),
          rdmc_group_num_offset(old_group.rdmc_group_num_offset +
                                old_group.num_members),
          total_message_buffers(old_group.total_message_buffers),
          sender_timeout(old_group.sender_timeout),
          sst(_sst) {
    // Make sure rdmc_group_num_offset didn't overflow.
    assert(old_group.rdmc_group_num_offset <=
           std::numeric_limits<uint16_t>::max() - old_group.num_members -
               num_members);

    // Just in case
    old_group.wedge();

    // Convience function that takes a msg from the old group and
    // produces one suitable for this group.
    auto convert_msg = [this](Message& msg) {
        msg.sender_rank = member_index;
        msg.index = future_message_index++;

        header* h = (header*)msg.message_buffer.buffer.get();
        future_message_index += h->pause_sending_turns;

        return std::move(msg);
    };

    // Reclaim MessageBuffers from the old group, and supplement them with
    // additional if the group has grown.
    std::lock_guard<std::mutex> lock(old_group.msg_state_mtx);
    free_message_buffers.swap(old_group.free_message_buffers);
    while(total_message_buffers < window_size * num_members) {
        free_message_buffers.emplace_back(max_msg_size);
        total_message_buffers++;
    }

    for(auto& msg : old_group.current_receives) {
        free_message_buffers.push_back(std::move(msg.second.message_buffer));
    }
    old_group.current_receives.clear();
    p2pBuffer = std::move(old_group.p2pBuffer);
    deliveryBuffer = std::move(old_group.deliveryBuffer);

    // Assume that any locally stable messages failed. If we were the sender
    // than re-attempt, otherwise discard. TODO: Presumably the ragged edge
    // cleanup will want the chance to deliver some of these.
    for(auto& p : old_group.locally_stable_messages) {
        if(p.second.size == 0) {
            continue;
        }

        if(p.second.sender_rank == old_group.member_index) {
            pending_sends.push(convert_msg(p.second));
        } else {
            free_message_buffers.push_back(std::move(p.second.message_buffer));
        }
    }
    old_group.locally_stable_messages.clear();

    // Any messages that were being sent should be re-attempted.
    if(old_group.current_send) {
        pending_sends.push(convert_msg(*old_group.current_send));
    }
    while(!old_group.pending_sends.empty()) {
        pending_sends.push(convert_msg(old_group.pending_sends.front()));
        old_group.pending_sends.pop();
    }
    if(old_group.next_send) {
        next_send = convert_msg(*old_group.next_send);
    }

    // If the old group was using persistence, we should transfer its state to the new group
    file_writer = std::move(old_group.file_writer);
    if(file_writer) {
        file_writer->set_message_written_upcall(make_file_written_callback());
    }
    for(auto& entry : old_group.non_persistent_messages) {
        non_persistent_messages.emplace(entry.first,
                                        convert_msg(entry.second));
    }
    old_group.non_persistent_messages.clear();
    initialize_sst_row();
    bool no_member_failed = true;
    if(already_failed.size()) {
        for(int i = 0; i < num_members; ++i) {
            if(already_failed[i]) {
                no_member_failed = false;
                break;
            }
        }
    }
    if(!already_failed.size() || no_member_failed) {
        // if groups are created successfully, rdmc_groups_created will be set
        // to true
        rdmc_groups_created = create_rdmc_groups();
    }
    register_predicates();
    sender_thread = std::thread(&DerechoGroup::send_loop, this);
    timeout_thread = std::thread(&DerechoGroup::check_failures_loop, this);
    rpc_thread = std::thread(&DerechoGroup::rpc_process_loop, this);
    //    cout << "DerechoGroup: Registered predicates and started thread" <<
    //    std::endl;
}

template <typename handlersType>
std::function<void(persistence::message)> DerechoGroup<handlersType>::make_file_written_callback() {
    return [this](persistence::message m) {
        //m.sender is an ID, not a rank
        int sender_rank;
        for(sender_rank = 0; sender_rank < num_members; ++sender_rank) {
            if(members[sender_rank] == m.sender) break;
        }
        callbacks.local_persistence_callback(sender_rank, m.index, m.data,
                                             m.length);

        // m.data points to the char[] buffer in a MessageBuffer, so we need to find
        // the msg corresponding to m and put its MessageBuffer on free_message_buffers
        auto sequence_number = m.index * num_members + sender_rank;
        {
            std::lock_guard<std::mutex> lock(msg_state_mtx);
            auto find_result = non_persistent_messages.find(sequence_number);
            assert(find_result != non_persistent_messages.end());
            Message &m_msg = find_result->second;
            free_message_buffers.push_back(std::move(m_msg.message_buffer));
            non_persistent_messages.erase(find_result);
            sst->persisted_num[member_index] = sequence_number;
            sst->put();
        }

    };
}

template <typename dispatchersType>
bool DerechoGroup<dispatchersType>::create_rdmc_groups() {
    // rotated list of members - used for creating n internal RDMC groups
    std::vector<uint32_t> rotated_members(num_members);

    std::cout << "The members are" << std::endl;
    for(int i = 0; i < num_members; ++i) {
        std::cout << members[i] << " ";
    }
    std::cout << std::endl;

    // create num_members groups one at a time
    for(int groupnum = 0; groupnum < num_members; ++groupnum) {
        /* members[groupnum] is the sender for group `groupnum`
         * for now, we simply rotate the members vector to supply to create_group
         * even though any arrangement of receivers in the members vector is possible
         */
        // allocate buffer for the group
        // std::unique_ptr<char[]> buffer(new char[max_msg_size*window_size]);
        // buffers.push_back(std::move(buffer));
        // create a memory region encapsulating the buffer
        // std::shared_ptr<rdma::memory_region> mr =
        // std::make_shared<rdma::memory_region>(buffers[groupnum].get(),max_msg_size*window_size);
        // mrs.push_back(mr);
        for(int j = 0; j < num_members; ++j) {
            rotated_members[j] = members[(groupnum + j) % num_members];
        }
        // When RDMC receives a message, it should store it in
        // locally_stable_messages and update the received count
        auto rdmc_receive_handler = [this, groupnum](char* data, size_t size) {
            assert(this->sst);
            util::debug_log().log_event(std::stringstream() << "Locally received message from sender " << groupnum << ": index = " << (sst->nReceived[member_index][groupnum] + 1));
            std::lock_guard<std::mutex> lock(msg_state_mtx);
            header *h = (header *)data;
            sst->nReceived[member_index][groupnum]++;

            long long int index = sst->nReceived[member_index][groupnum];
            long long int sequence_number = index * num_members + groupnum;

            // Move message from current_receives to locally_stable_messages.
            if(groupnum == member_index) {
                assert(current_send);
                locally_stable_messages[sequence_number] =
                    std::move(*current_send);
                current_send = std::experimental::nullopt;
            } else {
                auto it = current_receives.find(sequence_number);
                assert(it != current_receives.end());
                auto& message = it->second;
                locally_stable_messages.emplace(sequence_number, std::move(message));
                current_receives.erase(it);
            }
            // Add empty messages to locally_stable_messages for each turn that the sender is skipping.
            for(unsigned int j = 0; j < h->pause_sending_turns; ++j) {
                index++;
                sequence_number += num_members;
                sst->nReceived[member_index][groupnum]++;
                locally_stable_messages[sequence_number] = {groupnum, index, 0, 0};
            }

            std::atomic_signal_fence(std::memory_order_acq_rel);
            auto* min_ptr = std::min_element(&sst->nReceived[member_index][0],
                                 &sst->nReceived[member_index][num_members]);
            int min_index = std::distance(&sst->nReceived[member_index][0], min_ptr);
            auto new_seq_num = (*min_ptr + 1) * num_members + min_index - 1;
            if(new_seq_num > sst->seq_num[member_index]) {
                util::debug_log().log_event(std::stringstream() << "Updating seq_num to "  << new_seq_num);
                sst->seq_num[member_index] = new_seq_num;
                std::atomic_signal_fence(std::memory_order_acq_rel);
                sst->put();
            } else {
                sst->put();
            }
        };
        // Capture rdmc_receive_handler by copy! The reference to it won't be valid
        // after this constructor ends!
        auto receive_handler_plus_notify =
            [this, rdmc_receive_handler](char* data, size_t size) {
                rdmc_receive_handler(data, size);
                // signal background writer thread
                sender_cv.notify_all();
            };
        // groupnum is the group number
        // receive destination checks if the message will exceed the buffer length
        // at current position in which case it returns the beginning position
        if(groupnum == member_index) {
            // In the group in which this node is the sender, we need to signal the writer thread
            // to continue when we see that one of our messages was delivered.
            if(!rdmc::create_group(
                   groupnum + rdmc_group_num_offset, rotated_members, block_size, type,
                   [this, groupnum](size_t length) -> rdmc::receive_destination {
                       assert(false);
                       return {nullptr, 0};
                   },
                   receive_handler_plus_notify,
                   [](boost::optional<uint32_t>) {})) {
                return false;
            }
        } else {
            if(!rdmc::create_group(
                   groupnum + rdmc_group_num_offset, rotated_members, block_size, type,
                   [this, groupnum](size_t length) -> rdmc::receive_destination {
                       std::lock_guard<std::mutex> lock(msg_state_mtx);
                       assert(!free_message_buffers.empty());

                       Message msg;
                       msg.sender_rank = groupnum;
                       msg.index = sst->nReceived[member_index][groupnum] + 1;
                       msg.size = length;
                       msg.message_buffer = std::move(free_message_buffers.back());
                       free_message_buffers.pop_back();

                       rdmc::receive_destination ret{msg.message_buffer.mr, 0};
                       auto sequence_number = msg.index * num_members + groupnum;
                       current_receives[sequence_number] = std::move(msg);

                       assert(ret.mr->buffer != nullptr);
                       return ret;
                   },
                   rdmc_receive_handler, [](boost::optional<uint32_t>) {})) {
                return false;
            }
        }
    }
    return true;
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::initialize_sst_row() {
    for(int i = 0; i < num_members; ++i) {
        for(int j = 0; j < num_members; ++j) {
            sst->nReceived[i][j] = -1;
        }
        sst->seq_num[i] = -1;
        sst->stable_num[i] = -1;
        sst->delivered_num[i] = -1;
        sst->persisted_num[i] = -1;
    }
    sst->put();
    sst->sync_with_members();
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::deliver_message(Message& msg) {
    if(msg.size > 0) {
        char* buf = msg.message_buffer.buffer.get();
        header* h = (header*)(buf);
        // cooked send
        if(h->cooked_send) {
            buf += h->header_size;
            auto payload_size = msg.size - h->header_size;
            // extract the destination vector
            size_t dest_size = ((size_t*)buf)[0];
            buf += sizeof(size_t);
            payload_size -= sizeof(size_t);
            bool in_dest = false;
            for(size_t i = 0; i < dest_size; ++i) {
                auto n = ((node_id_t*)buf)[0];
                buf += sizeof(node_id_t);
                payload_size -= sizeof(node_id_t);
                if(n == members[member_index]) {
                    in_dest = true;
                }
            }
            if(in_dest || dest_size == 0) {
                auto max_payload_size = max_msg_size - sizeof(header);
                size_t reply_size = 0;
                dispatchers.handle_receive(
                    buf, payload_size, [this, &reply_size, &max_payload_size](size_t size) -> char* {
                        reply_size = size;
                        if(reply_size <= max_payload_size) {
                            return deliveryBuffer.get();
                        } else {
                            return nullptr;
                        }
                    });
                if(reply_size > 0) {
                    node_id_t id = members[msg.sender_rank];
                    if(id == members[member_index]) {
                        dispatchers.handle_receive(
                            deliveryBuffer.get(), reply_size,
                            [](size_t size) -> char* { assert(false); });
                        if(dest_size == 0) {
                            std::lock_guard<std::mutex> lock(pending_results_mutex);
                            toFulfillQueue.front()->fulfill_map(members);
                            fulfilledList.push_back(std::move(toFulfillQueue.front()));
                            toFulfillQueue.pop();
                        }
                    } else {
                        connections.write(id, deliveryBuffer.get(),
                                          reply_size);
                    }
                }
            }
        }
        // raw send
        else {
            callbacks.global_stability_callback(msg.sender_rank, msg.index,
                                                buf + h->header_size, msg.size);
        }
        if(file_writer) {
            // msg.sender_rank is the 0-indexed rank within this group, but
            // persistence::message needs the sender's globally unique ID
            persistence::message msg_for_filewriter{buf + h->header_size,
                                                    msg.size, (uint32_t)sst->vid[member_index],
                                                    members[msg.sender_rank], (uint64_t)msg.index,
                                                    h->cooked_send};
            auto sequence_number = msg.index * num_members + msg.sender_rank;
            non_persistent_messages.emplace(sequence_number, std::move(msg));
            file_writer->write_message(msg_for_filewriter);
        } else {
            free_message_buffers.push_back(std::move(msg.message_buffer));
        }
    }
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::deliver_messages_upto(
    const std::vector<long long int>& max_indices_for_senders) {
    assert(max_indices_for_senders.size() == (size_t)num_members);
    std::lock_guard<std::mutex> lock(msg_state_mtx);
    auto curr_seq_num = sst->delivered_num[member_index];
    auto max_seq_num = curr_seq_num;
    for(int sender = 0; sender < (int)max_indices_for_senders.size();
        sender++) {
        max_seq_num =
            std::max(max_seq_num,
                     max_indices_for_senders[sender] * num_members + sender);
    }
    for(auto seq_num = curr_seq_num; seq_num <= max_seq_num; seq_num++) {
        auto msg_ptr = locally_stable_messages.find(seq_num);
        if(msg_ptr != locally_stable_messages.end()) {
            deliver_message(msg_ptr->second);
            locally_stable_messages.erase(msg_ptr);
        }
    }
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::register_predicates() {
    auto stability_pred = [this](
        const DerechoSST& sst) { return true; };
    auto stability_trig =
        [this](DerechoSST& sst) {
            // compute the min of the seq_num
            long long int min_seq_num = sst.seq_num[0];
            for(int i = 0; i < num_members; ++i) {
                if(sst.seq_num[i] < min_seq_num) {
                    min_seq_num = sst.seq_num[i];
                }
            }
            if(min_seq_num > sst.stable_num[member_index]) {
                util::debug_log().log_event(std::stringstream()
                                            << "Updating stable_num to "
                                            << min_seq_num);
                sst.stable_num[member_index] = min_seq_num;
                sst.put();
            }
        };
    stability_pred_handle = sst->predicates.insert(
        stability_pred, stability_trig, sst::PredicateType::RECURRENT);

    auto delivery_pred = [this](
        const DerechoSST& sst) { return true; };
    auto delivery_trig = [this](
        DerechoSST& sst) {
        std::lock_guard<std::mutex> lock(msg_state_mtx);
        // compute the min of the stable_num
        long long int min_stable_num = sst.stable_num[0];
        for(int i = 0; i < num_members; ++i) {
            if(sst.stable_num[i] < min_stable_num) {
                min_stable_num = sst.stable_num[i];
            }
        }

        if(!locally_stable_messages.empty()) {
            long long int least_undelivered_seq_num =
                locally_stable_messages.begin()->first;
            if(least_undelivered_seq_num <= min_stable_num) {
                util::debug_log().log_event(std::stringstream() << "Can deliver a locally stable message: min_stable_num=" << min_stable_num << " and least_undelivered_seq_num=" << least_undelivered_seq_num);
                Message& msg = locally_stable_messages.begin()->second;
                deliver_message(msg);
                sst.delivered_num[member_index] = least_undelivered_seq_num;
                //                sst.put (offsetof (DerechoRow<N>,
                //                delivered_num), sizeof
                //                (least_undelivered_seq_num));
                sst.put();
                locally_stable_messages.erase(locally_stable_messages.begin());
            }
        }
    };
    delivery_pred_handle = sst->predicates.insert(delivery_pred, delivery_trig, sst::PredicateType::RECURRENT);

    auto sender_pred = [this](const DerechoSST& sst) {
        long long int seq_num = next_message_to_deliver * num_members + member_index;
        for (int i = 0; i < num_members; ++i) {
            if (sst.delivered_num[i] < seq_num || (file_writer && sst.persisted_num[i] < seq_num)) {
                return false;
            }
        }
        return true;
    };
    auto sender_trig = [this](DerechoSST& sst) {
        sender_cv.notify_all();
        next_message_to_deliver++;
    };
    sender_pred_handle = sst->predicates.insert(sender_pred, sender_trig,
                                                sst::PredicateType::RECURRENT);
}

template <typename dispatchersType>
DerechoGroup<dispatchersType>::~DerechoGroup() {
    wedge();
    if(timeout_thread.joinable()) {
        timeout_thread.join();
    }
    if(rpc_thread.joinable()) {
        rpc_thread.join();
    }
}

template <typename dispatchersType>
long long unsigned int DerechoGroup<dispatchersType>::compute_max_msg_size(
    const long long unsigned int max_payload_size,
    const long long unsigned int block_size) {
    auto max_msg_size = max_payload_size + sizeof(header);
    if(max_msg_size % block_size != 0) {
        max_msg_size = (max_msg_size / block_size + 1) * block_size;
    }
    return max_msg_size;
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::wedge() {
    bool thread_shutdown_existing = thread_shutdown.exchange(true);
    if(thread_shutdown_existing) {  // Wedge has already been called
        return;
    }

    sst->predicates.remove(stability_pred_handle);
    sst->predicates.remove(delivery_pred_handle);
    sst->predicates.remove(sender_pred_handle);

    for(int i = 0; i < num_members; ++i) {
        rdmc::destroy_group(i + rdmc_group_num_offset);
    }

    if(rpc_thread.joinable()) {
        rpc_thread.join();
    }
    connections.destroy();

    sender_cv.notify_all();
    if(sender_thread.joinable()) {
        sender_thread.join();
    }
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::send_loop() {
    auto should_send = [&]() {
        if(!rdmc_groups_created) {
            return false;
        }
        if(pending_sends.empty()) {
            return false;
        }
        Message &msg = pending_sends.front();
        if(sst->nReceived[member_index][member_index] < msg.index - 1) {
            return false;
        }

        for (int i = 0; i < num_members; ++i) {
            if (sst->delivered_num[i] < (msg.index - window_size) * num_members + member_index
                    || (file_writer && sst->persisted_num[i] < (msg.index - window_size) * num_members + member_index)) {
                return false;
            }
        }

        return true;
    };
    auto should_wake = [&]() { return thread_shutdown || should_send(); };
    try {
        std::unique_lock<std::mutex> lock(msg_state_mtx);
        while(!thread_shutdown) {
            sender_cv.wait(lock, should_wake);
            if(!thread_shutdown) {
                current_send = std::move(pending_sends.front());
                util::debug_log().log_event(std::stringstream() << "Calling send on message " << current_send->index
                                                                << " from sender " << current_send->sender_rank);
                if(!rdmc::send(member_index + rdmc_group_num_offset,
                               current_send->message_buffer.mr, 0,
                               current_send->size)) {
                    throw std::runtime_error("rdmc::send returned false");
                }
                pending_sends.pop();
            }
        }
        std::cout << "DerechoGroup send thread shutting down" << std::endl;
    } catch(const std::exception& e) {
        std::cout << "DerechoGroup send thread had an exception: " << e.what() << std::endl;
    }
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::check_failures_loop() {
    while(!thread_shutdown) {
        std::this_thread::sleep_for(milliseconds(sender_timeout));
        if(sst) sst->put();
    }
}

template <typename dispatchersType>
bool DerechoGroup<dispatchersType>::send() {
    std::lock_guard<std::mutex> lock(msg_state_mtx);
    if(thread_shutdown || !rdmc_groups_created) {
        return false;
    }
    assert(next_send);
    pending_sends.push(std::move(*next_send));
    next_send = std::experimental::nullopt;
    sender_cv.notify_all();
    return true;
}

template <typename dispatchersType>
char* DerechoGroup<dispatchersType>::get_position(
    long long unsigned int payload_size,
    int pause_sending_turns, bool cooked_send) {
    // if rdmc groups were not created because of failures, return NULL
    if(!rdmc_groups_created) {
        return NULL;
    }
    long long unsigned int msg_size = payload_size + sizeof(header);
    // payload_size is 0 when max_msg_size is desired, useful for ordered send/query
    if(!payload_size) {
        msg_size = max_msg_size;
    }
    if(msg_size > max_msg_size) {
        std::cout << "Can't send messages of size larger than the maximum message "
                     "size which is equal to "
                  << max_msg_size << std::endl;
        return nullptr;
    }
    for(int i = 0; i < num_members; ++i) {
        if(sst->delivered_num[i] <
           (future_message_index - window_size) * num_members + member_index) {
            return nullptr;
        }
    }

    std::unique_lock<std::mutex> lock(msg_state_mtx);
    if(thread_shutdown) return nullptr;
    if(free_message_buffers.empty()) return nullptr;

    // Create new Message
    Message msg;
    msg.sender_rank = member_index;
    msg.index = future_message_index;
    msg.size = msg_size;
    msg.message_buffer = std::move(free_message_buffers.back());
    free_message_buffers.pop_back();

    // Fill header
    char* buf = msg.message_buffer.buffer.get();
    ((header*)buf)->header_size = sizeof(header);
    ((header*)buf)->pause_sending_turns = pause_sending_turns;
    ((header*)buf)->cooked_send = cooked_send;

    next_send = std::move(msg);
    future_message_index += pause_sending_turns + 1;

    return buf + sizeof(header);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
auto DerechoGroup<dispatchersType>::derechoCallerSend(
    const std::vector<node_id_t>& nodes, char* buf, Args&&... args) {
    auto max_payload_size = max_msg_size - sizeof(header);
    // use nodes
    ((size_t*)buf)[0] = nodes.size();
    buf += sizeof(size_t);
    max_payload_size -= sizeof(size_t);
    for(auto& n : nodes) {
        ((node_id_t*)buf)[0] = n;
        buf += sizeof(node_id_t);
        max_payload_size -= sizeof(node_id_t);
    }

    auto return_pair = dispatchers.template Send<IdClass, tag>(
        [&buf, &max_payload_size](size_t size) -> char* {
            if(size <= max_payload_size) {
                return buf;
            } else {
                return nullptr;
            }
        },
        std::forward<Args>(args)...);
    while(!send()) {
    }
    auto P = createPending(return_pair.pending);

    std::lock_guard<std::mutex> lock(pending_results_mutex);
    if(nodes.size()) {
        P->fulfill_map(nodes);
        fulfilledList.push_back(std::move(P));
    } else {
        toFulfillQueue.push(std::move(P));
    }
    return std::move(return_pair.results);
}

// this should be called from the GMS, as this takes care of locks on mutexes
// view_change_mutex and msg_state_mutex
template <typename dispatchersType>
template <typename IDClass, unsigned long long tag, typename... Args>
void DerechoGroup<dispatchersType>::orderedSend(const std::vector<node_id_t>& nodes,
                                                char* buf, Args&&... args) {
    derechoCallerSend<IDClass, tag>(nodes, buf, std::forward<Args>(args)...);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
void DerechoGroup<dispatchersType>::orderedSend(char* buf, Args&&... args) {
    // empty nodes means that the destination is the entire group
    orderedSend<IdClass, tag>({}, buf, std::forward<Args>(args)...);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
auto DerechoGroup<dispatchersType>::orderedQuery(const std::vector<node_id_t>& nodes,
                                                 char* buf, Args&&... args) {
    return derechoCallerSend<IdClass, tag>(nodes, buf, std::forward<Args>(args)...);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
auto DerechoGroup<dispatchersType>::orderedQuery(char* buf, Args&&... args) {
    return orderedQuery<IdClass, tag>({}, buf, std::forward<Args>(args)...);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
auto DerechoGroup<dispatchersType>::tcpSend(node_id_t dest_node,
                                            Args&&... args) {
    assert(dest_node != members[member_index]);
    // use dest_node

    size_t size;
    auto max_payload_size = max_msg_size - sizeof(header);
    auto return_pair = dispatchers.template Send<IdClass, tag>(
        [this, &max_payload_size, &size](size_t _size) -> char* {
            size = _size;
            if(size <= max_payload_size) {
                return p2pBuffer.get();
            } else {
                return nullptr;
            }
        },
        std::forward<Args>(args)...);
    connections.write(dest_node, p2pBuffer.get(), size);
    auto P = createPending(return_pair.pending);
    P->fulfill_map({dest_node});

    std::lock_guard<std::mutex> lock(pending_results_mutex);
    fulfilledList.push_back(std::move(P));
    return std::move(return_pair.results);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
void DerechoGroup<dispatchersType>::p2pSend(node_id_t dest_node,
                                            Args&&... args) {
    tcpSend<IdClass, tag>(dest_node, std::forward<Args>(args)...);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
auto DerechoGroup<dispatchersType>::p2pQuery(node_id_t dest_node,
                                             Args&&... args) {
    return tcpSend<IdClass, tag>(dest_node, std::forward<Args>(args)...);
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::send_objects(tcp::socket& new_member_socket) {
    dispatchers.send_objects(new_member_socket);
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::rpc_process_loop() {
    using namespace ::rpc::remote_invocation_utilities;
    const auto header_size = header_space();
    auto max_payload_size = max_msg_size - sizeof(header);
    std::unique_ptr<char[]> rpcBuffer =
        std::unique_ptr<char[]>(new char[max_payload_size]);
    while(!thread_shutdown) {
        auto other_id = connections.probe_all();
        if(other_id < 0) {
            continue;
        }
        connections.read(other_id, rpcBuffer.get(), header_size);
        std::size_t payload_size;
        Opcode indx;
        Node_id received_from;
        retrieve_header(nullptr, rpcBuffer.get(), payload_size,
                        indx, received_from);
        connections.read(other_id, rpcBuffer.get() + header_size,
                         payload_size);
        size_t reply_size = 0;
        dispatchers.handle_receive(
            indx, received_from, rpcBuffer.get() + header_size, payload_size,
            [&rpcBuffer, &max_payload_size,
             &reply_size](size_t _size) -> char* {
                reply_size = _size;
                if(reply_size <= max_payload_size) {
                    return rpcBuffer.get();
                } else {
                    return nullptr;
                }
            });
        if(reply_size > 0) {
            connections.write(received_from.id, rpcBuffer.get(),
                              reply_size);
        }
    }
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::set_exceptions_for_removed_nodes(
    std::vector<node_id_t> removed_members) {
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    for(auto& pending : fulfilledList) {
        for(auto removed_id : removed_members) {
            pending->set_exception_for_removed_node(removed_id);
        }
    }
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::debug_print() {
    std::cout << "In DerechoGroup SST has " << sst->get_num_rows()
              << " rows; member_index is " << member_index << std::endl;
    std::cout << "Printing SST" << std::endl;
    for(int i = 0; i < num_members; ++i) {
        std::cout << sst->seq_num[i] << " " << sst->stable_num[i] << " " << sst->delivered_num[i] << std::endl;
    }
    std::cout << std::endl;

    std::cout << "Printing last_received_messages" << std::endl;
    for(int i = 0; i < num_members; ++i) {
        std::cout << sst->nReceived[member_index][i] << " " << std::endl;
    }
    std::cout << std::endl;
}

}  // namespace derecho
