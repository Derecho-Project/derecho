
#include "group_send.h"

#include <cassert>
#include <climits>
#include <cmath>
#include <iostream>
#include <sys/mman.h>

using namespace std;
using namespace rdma;
using namespace rdmc;

decltype(group::message_types) group::message_types;

group::group(uint16_t _group_number, size_t _block_size,
             vector<uint32_t> _members, uint32_t _member_index,
             incoming_message_callback_t upcall, completion_callback_t callback)
    : members(_members),
      first_block_buffer(nullptr),
      completion_callback(callback),
      incoming_message_upcall(upcall),
      group_number(_group_number),
      block_size(_block_size),
      num_members(members.size()),
      member_index(_member_index) {
    if(member_index != 0) {
        first_block_buffer = unique_ptr<char[]>(new char[block_size]);
        memset(first_block_buffer.get(), 0, block_size);

        first_block_mr =
            make_unique<memory_region>(first_block_buffer.get(), block_size);
    }
}
group::~group() { unique_lock<mutex> lock(monitor); }
void group::receive_block(uint32_t send_imm, size_t received_block_size) {
    unique_lock<mutex> lock(monitor);

    assert(member_index > 0);

    if(receive_step == 0) {
        num_blocks = parse_immediate(send_imm).total_blocks;
        first_block_number =
            min(get_first_block()->block_number, num_blocks - 1);
        message_size = num_blocks * block_size;
        if(num_blocks == 1) {
            message_size = received_block_size;
        }

        assert(*first_block_number == parse_immediate(send_imm).block_number);

        //////////////////////////////////////////////////////
        auto destination = incoming_message_upcall(message_size);
        mr_offset = destination.offset;
        mr = destination.mr;

        assert(mr->size >= mr_offset + message_size);
        //////////////////////////////////////////////////////

        num_received_blocks = 1;
        received_blocks = vector<bool>(num_blocks);
        received_blocks[*first_block_number] = true;

        LOG_EVENT(group_number, message_number, *first_block_number,
                  "initialized_internal_datastructures");

        assert(receive_step == 0);
        auto transfer = get_incoming_transfer(receive_step);
        while((!transfer || transfer->block_number == *first_block_number) &&
              receive_step < get_total_steps()) {
            transfer = get_incoming_transfer(++receive_step);
        }

        // cout << "receive_step = " << receive_step
        //      << " transfer->block_number = "
        //      << transfer->block_number
        //      << " first_block_number = " << *first_block_number
        //      << " total_steps = " << get_total_steps() << endl;

        LOG_EVENT(group_number, message_number, *first_block_number,
                  "found_next_transfer");

        if(transfer) {
            LOG_EVENT(group_number, message_number, transfer->block_number,
                      "posting_recv");
            // printf("Posting recv #%d (receive_step = %d,
            // *first_block_number =
            // %d, total_steps = %d)\n",
            //        (int)transfer->block_number, (int)receive_step,
            // (int)*first_block_number, (int)get_total_steps());
            post_recv(*transfer);
            incoming_block = transfer->block_number;
            send_ready_for_block(transfer->target);
            // cout << "Issued Ready For Block AAAAAAAA (receive_step = "
            //      << receive_step << ", target = " << transfer->target << ")"
            //      << endl;

            for(auto r = receive_step + 1; r < get_total_steps(); r++) {
                auto t = get_incoming_transfer(r);
                if(t) {
                    // cout << "posting block for step " << (int)r
                    //      << " (block #" << (*t).block_number << ")" << endl;
                    post_recv(*t);
                    break;
                }
            }
        }

        LOG_EVENT(group_number, message_number, *first_block_number,
                  "calling_send_next_block");

        send_next_block();

        LOG_EVENT(group_number, message_number, *first_block_number,
                  "returned_from_send_next_block");

        if(!sending && send_step == get_total_steps() &&
           num_received_blocks == num_blocks) {
            complete_message();
        }
    } else {
        //        assert(tag.index() <= tag.message_size());
        size_t block_number = incoming_block;
        if(block_number != parse_immediate(send_imm).block_number) {
            printf("Expected block #%d but got #%d on step %d\n",
                   (int)block_number,
                   (int)parse_immediate(send_imm).block_number,
                   (int)receive_step);
            fflush(stdout);
        }
        assert(block_number == parse_immediate(send_imm).block_number);

        if(block_number == num_blocks - 1) {
            message_size = (num_blocks - 1) * block_size + received_block_size;
        } else {
            assert(received_block_size == block_size);
        }

        received_blocks[block_number] = true;

        LOG_EVENT(group_number, message_number, block_number, "received_block");

        // Figure out the next block to receive.
        optional<block_transfer> transfer;
        while(!transfer && receive_step + 1 < get_total_steps()) {
            transfer = get_incoming_transfer(++receive_step);
        }

        // Post a receive for it.
        if(transfer) {
            incoming_block = transfer->block_number;
            send_ready_for_block(transfer->target);
            // cout << "Issued Ready For Block BBBBBBBB (receive_step = "
            //      << receive_step << ", target = " << transfer->target
            //      << ", total_steps = " << get_total_steps() << ")" << endl;
            for(auto r = receive_step + 1; r < get_total_steps(); r++) {
                auto t = get_incoming_transfer(r);
                if(t) {
                    post_recv(*t);
                    break;
                }
            }
        }

        // If we just finished receiving a block and we weren't
        // previously sending, then try to send now.
        if(!sending) {
            send_next_block();
        }
        // If we just received the last block and aren't still sending then
        // issue a completion callback
        if(++num_received_blocks == num_blocks && !sending &&
           send_step == get_total_steps()) {
            complete_message();
        }
    }
}
void group::receive_ready_for_block(uint32_t step, uint32_t sender) {
    unique_lock<mutex> lock(monitor);

    auto it = rfb_queue_pairs.find(sender);
    assert(it != rfb_queue_pairs.end());
    it->second.post_empty_recv(form_tag(group_number, sender),
                               message_types.ready_for_block);

    receivers_ready.insert(sender);

    if(!sending && mr) {
        send_next_block();
    }
}
void group::complete_block_send() {
    unique_lock<mutex> lock(monitor);

    LOG_EVENT(group_number, message_number, outgoing_block,
              "finished_sending_block");

    send_next_block();

    // If we just send the last block, and were already done
    // receiving, then signal completion and prepare for the next
    // message.
    if(!sending && send_step == get_total_steps() &&
       (member_index == 0 || num_received_blocks == num_blocks)) {
        complete_message();
    }
}
void group::send_message(shared_ptr<memory_region> message_mr, size_t offset,
                         size_t length) {
    LOG_EVENT(group_number, -1, -1, "send()");

    unique_lock<mutex> lock(monitor);

    if(length == 0) throw rdmc::invalid_args();
    if(offset + length > message_mr->size) throw rdmc::invalid_args();
    if(member_index > 0) throw rdmc::nonroot_sender();

    // Queueing sends is not supported
    if(receive_step > 0) throw rdmc::group_busy();
    if(send_step > 0) throw rdmc::group_busy();

    mr = message_mr;
    mr_offset = offset;
    message_size = length;
    num_blocks = (message_size - 1) / block_size + 1;
    if(num_blocks > std::numeric_limits<uint16_t>::max())
        throw rdmc::invalid_args();
    // printf("message_size = %lu, block_size = %lu, num_blocks = %lu\n",
    //        message_size, block_size, num_blocks);
    LOG_EVENT(group_number, message_number, -1, "send_message");

    send_next_block();
    // No need to worry about completion here. We must send at least
    // one block, so we can't be done already.
}
bool group::init() {
    unique_lock<mutex> lock(monitor);

    try {
        form_connections();
    } catch(rdma::qp_creation_failure) {
        return false;
    }

    if(member_index > 0) {
        auto transfer = get_first_block();
        first_block_number = transfer->block_number;
        post_recv(*transfer);
        incoming_block = transfer->block_number;
        send_ready_for_block(transfer->target);
        // puts("Issued Ready For Block CCCCCCCCC");
    }
    return true;
}
void group::send_next_block() {
    sending = false;
    if(send_step == get_total_steps()) {
        return;
    }
    auto transfer = get_outgoing_transfer(send_step);
    while(!transfer) {
        if(++send_step == get_total_steps()) return;

        transfer = get_outgoing_transfer(send_step);
    }

    size_t target = transfer->target;
    size_t block_number = transfer->block_number;
    //    size_t forged_block_number = transfer->forged_block_number;

    if(member_index > 0 && !received_blocks[block_number]) return;

    if(receivers_ready.count(transfer->target) == 0) {
        LOG_EVENT(group_number, message_number, block_number,
                  "receiver_not_ready");
        return;
    }

    receivers_ready.erase(transfer->target);
    sending = true;
    ++send_step;

    // printf("sending block #%d to node #%d on step %d\n", (int)block_number,
    // 	   (int)target, (int)send_step-1);
    // fflush(stdout);
    auto it = queue_pairs.find(target);
    assert(it != queue_pairs.end());

    if(first_block_number && block_number == *first_block_number) {
        CHECK(it->second.post_send(*first_block_mr, 0, block_size,
                                   form_tag(group_number, target),
                                   form_immediate(num_blocks, block_number),
                                   message_types.data_block));
    } else {
        size_t offset = block_number * block_size;
        size_t nbytes = min(block_size, message_size - offset);
        CHECK(it->second.post_send(*mr, mr_offset + offset, nbytes,
                                   form_tag(group_number, target),
                                   form_immediate(num_blocks, block_number),
                                   message_types.data_block));
    }
    outgoing_block = block_number;
    LOG_EVENT(group_number, message_number, block_number,
              "started_sending_block");
}
void group::complete_message() {
    // remap first_block into buffer
    if(member_index > 0 && first_block_number) {
        LOG_EVENT(group_number, message_number, *first_block_number,
                  "starting_remap_first_block");
        // if(block_size > (128 << 10) && (block_size % 4096 == 0)) {
        //     char *tmp_buffer =
        //         (char *)mmap(NULL, block_size, PROT_READ | PROT_WRITE,
        //                      MAP_ANON | MAP_PRIVATE, -1, 0);

        //     mremap(buffer + block_size * (*first_block_number), block_size,
        //            block_size, MREMAP_FIXED | MREMAP_MAYMOVE, tmp_buffer);

        //     mremap(first_block_buffer, block_size, block_size,
        //            MREMAP_FIXED | MREMAP_MAYMOVE,
        //            buffer + block_size * (*first_block_number));
        //     first_block_buffer = tmp_buffer;
        // } else {
        memcpy(mr->buffer + mr_offset + block_size * (*first_block_number),
               first_block_buffer.get(), block_size);
        // }
        LOG_EVENT(group_number, message_number, *first_block_number,
                  "finished_remap_first_block");
    }
    completion_callback(mr->buffer + mr_offset, message_size);

    ++message_number;
    sending = false;
    send_step = 0;
    receive_step = 0;
    mr.reset();
    // if(first_block_buffer == nullptr && member_index > 0){
    //     first_block_buffer = (char*)mmap(NULL, block_size,
    // PROT_READ|PROT_WRITE,
    //                                      MAP_ANON|MAP_PRIVATE, -1, 0);
    //     memset(first_block_buffer, 1, block_size);
    //     memset(first_block_buffer, 0, block_size);
    // }
    first_block_number = boost::none;

    if(member_index != 0) {
        num_received_blocks = 0;
        received_blocks.clear();
        auto transfer = get_first_block();
        assert(transfer);
        first_block_number = transfer->block_number;
        post_recv(*transfer);
        incoming_block = transfer->block_number;
        send_ready_for_block(transfer->target);
        // cout << "Issued Ready For Block DDDDDDD (target = " <<
        // transfer->target
        //      << ")" << endl;
    }
}
void group::post_recv(block_transfer transfer) {
    auto it = queue_pairs.find(transfer.target);
    assert(it != queue_pairs.end());

    // printf("Posting receive buffer for block #%d from node #%d\n",
    //        (int)transfer.block_number, (int)transfer.target);
    // fflush(stdout);

    if(first_block_number && transfer.block_number == *first_block_number) {
        CHECK(it->second.post_recv(*first_block_mr, 0, block_size,
                                   form_tag(group_number, transfer.target),
                                   message_types.data_block));
    } else {
        size_t offset = block_size * transfer.block_number;
        size_t length = min(block_size, (size_t)(message_size - offset));

        if(length > 0) {
            CHECK(it->second.post_recv(*mr, mr_offset + offset, length,
                                       form_tag(group_number, transfer.target),
                                       message_types.data_block));
        }
    }
    LOG_EVENT(group_number, message_number, transfer.block_number,
              "posted_receive_buffer");
}
void group::connect(size_t neighbor) {
    queue_pairs.emplace(neighbor, queue_pair(members[neighbor]));

    auto post_recv = [this, neighbor](rdma::queue_pair* qp) {
        qp->post_empty_recv(form_tag(group_number, neighbor),
                            message_types.ready_for_block);
    };

    rfb_queue_pairs.emplace(neighbor, queue_pair(members[neighbor], post_recv));
}

void group::send_ready_for_block(uint32_t neighbor) {
    auto it = rfb_queue_pairs.find(neighbor);
    assert(it != rfb_queue_pairs.end());
    it->second.post_empty_send(form_tag(group_number, neighbor), 0,
                               message_types.ready_for_block);
}

void chain_group::form_connections() {
    // establish connection with member_index-1 and member_index+1, if they
    // exist
    if(member_index == 0) {
        if(num_members == 1) {
            return;
        }
        // with only node with rank 1
        connect(1);
    } else if(member_index == num_members - 1) {
        connect(num_members - 2);
    } else {
        connect(member_index - 1);
        connect(member_index + 1);
    }
}
size_t chain_group::get_total_steps() const {
    size_t total_steps = num_blocks + num_members - 2;
    return total_steps;
}
optional<group::block_transfer> chain_group::get_outgoing_transfer(
    size_t step) const {
    size_t block_number = step - member_index;

    if(member_index > step || block_number >= num_blocks ||
       member_index == num_members - 1) {
        return boost::none;
    }

    return block_transfer{(uint32_t)(member_index + 1), block_number};
}
optional<group::block_transfer> chain_group::get_incoming_transfer(
    size_t step) const {
    size_t block_number = (step + 1) - member_index;
    if(member_index > step + 1 || block_number >= num_blocks ||
       member_index == 0) {
        return boost::none;
    }
    return block_transfer{(uint32_t)(member_index - 1), block_number};
}
optional<group::block_transfer> chain_group::get_first_block() const {
    if(member_index == 0) return boost::none;
    return block_transfer{(uint32_t)(member_index - 1), 0};
}

void sequential_group::form_connections() {
    // if sender, connect to every receiver
    if(member_index == 0) {
        for(auto i = 1u; i < num_members; ++i) {
            connect(i);
        }
    }
    // if receiver, connect only to sender
    else {
        connect(0);
    }
}
size_t sequential_group::get_total_steps() const {
    return num_blocks * (num_members - 1);
}
optional<group::block_transfer> sequential_group::get_outgoing_transfer(
    size_t step) const {
    if(member_index > 0 || step >= num_blocks * (num_members - 1)) {
        return boost::none;
    }

    size_t block_number = step % num_blocks;
    return block_transfer{(uint32_t)(1 + step / num_blocks), block_number};
}
optional<group::block_transfer> sequential_group::get_incoming_transfer(
    size_t step) const {
    if(1 + step / num_blocks != member_index) {
        return boost::none;
    }

    size_t block_number = step % num_blocks;
    return block_transfer{(uint32_t)0, block_number};
}
optional<group::block_transfer> sequential_group::get_first_block() const {
    if(member_index == 0) return boost::none;
    return block_transfer{0, 0};
}

void tree_group::form_connections() {
    for(uint32_t i = 0; i < 32; i++) {
        if(member_index != 0 && (2ull << i) > member_index) {
            connect(member_index - (1 << i));
            break;
        }
    }

    for(uint32_t i = 0; i < 32 && num_members - member_index > (1u << i); i++) {
        if((1u << i) > member_index) {
            connect(member_index + (1 << i));
        }
    }
}
size_t tree_group::get_total_steps() const {
    unsigned int log2_num_members = ceil(log2(num_members));
    return num_blocks * log2_num_members;
}
optional<group::block_transfer> tree_group::get_outgoing_transfer(
    size_t step) const {
    size_t stage = step / num_blocks;
    if(step >= get_total_steps() || (1u << stage) <= member_index ||
       (1u << stage) >= num_members - member_index) {
        return boost::none;
    } else {
        return block_transfer{member_index + (1u << stage),
                              step - stage * num_blocks};
    }
}
optional<group::block_transfer> tree_group::get_incoming_transfer(
    size_t step) const {
    size_t stage = step / num_blocks;
    if(step < get_total_steps() && (1u << stage) <= member_index &&
       member_index < (2u << stage)) {
        return block_transfer{member_index - (1u << stage),
                              step - stage * num_blocks};
    } else {
        return boost::none;
    }
}
optional<group::block_transfer> tree_group::get_first_block() const {
    if(member_index == 0) return boost::none;

    for(uint32_t i = 0; i < 32; i++) {
        if((2ull << i) > member_index)
            return block_transfer{member_index - (1 << i), 0};
    }
    assert(false);
}
binomial_group::binomial_group(uint16_t group_number, size_t block_size,
                               vector<uint32_t> members, uint32_t member_index,
                               incoming_message_callback_t upcall,
                               completion_callback_t callback)
    : group(group_number, block_size, members, member_index, upcall, callback),
      log2_num_members(floor(log2(num_members))) /*,
      vertex(member_index & ((1 << log2_num_members) - 1))*/ {
    // size_t vertex_twin = vertex | (1 << log2_num_members);
    // if(vertex_twin < num_members) {
    //     assert(false && "NPOT group size not yet supported");
    //     twin = (member_index == vertex) ? vertex_twin : vertex;
    // }

    // for(unsigned int index = 0; index < log2_num_members; index++) {
    //     if(vertex < (2ull << index)) {
    //         vertex_first_block_step = index;
    //     }
    // }
}

void binomial_group::form_connections() {
    // size_t num_neighbors = floor(log2(num_members));

    uint32_t twin = (member_index < (1u << log2_num_members))
                        ? member_index + (1 << log2_num_members) - 1
                        : member_index + 1 - (1 << log2_num_members);

    uint32_t vertex = min(member_index, twin);

    if(member_index > 0 && twin < num_members) connect(twin);

    for(size_t i = 0; i < log2_num_members; ++i) {
        // connect to num_members^ (1 << i)
        uint32_t neighbor = vertex ^ (1 << i);
        uint32_t neighbor_twin = neighbor + (1 << log2_num_members) - 1;

        connect(neighbor);
        if(neighbor > 0 && neighbor_twin < num_members) connect(neighbor_twin);
    }
}

size_t binomial_group::get_total_steps() const {
    if(1u << log2_num_members == num_members)
        return num_blocks + log2_num_members - 1;

    return num_blocks + log2_num_members;
}
optional<group::block_transfer> binomial_group::get_vertex_outgoing_transfer(
    uint32_t sender, size_t send_step, uint32_t num_members,
    unsigned int log2_num_members, size_t num_blocks, size_t total_steps) {
    /*
   * During a typical step, the rotated rank will indicate:
   *
   *   0000...0001 -> block = send_step
   *   1000...0001 -> block = send_step - 1
   *   x100...0001 -> block = send_step - 2
   *   xx10...0001 -> block = send_step - 3
   *
   *   xxxx...xx11 -> block = send_step - log2_num_members + 1
   *   xxxx...xxx0 -> block = send_step - log2_num_members
   */

    size_t rank_mask =
        (~((size_t)0)) >> (sizeof(size_t) * CHAR_BIT - log2_num_members);

    size_t step_index = send_step % log2_num_members;
    uint32_t neighbor = sender ^ (1 << step_index);
    if(send_step >= total_steps || neighbor == 0 ||
       (send_step == total_steps - 1 && num_members > 1u << log2_num_members)) {
        //        printf("send_step = %d, neighbor = %d, log2(...) = %f\n",
        // (int)send_step, (int)neighbor, log2(member_index|neighbor));
        //        fflush(stdout);
        return boost::none;
    }

    size_t rotated_rank =
        ((neighbor | (neighbor << log2_num_members)) >> step_index) & rank_mask;
    //    printf("send_step = %d, rotated_rank = %x\n", (int)send_step,
    // (int)rotated_rank);
    //    fflush(stdout);

    if((rotated_rank & 1) == 0) {
        if(send_step < log2_num_members) {
            //            printf("send_step < log2_num_members\n");
            //            fflush(stdout);
            return boost::none;
        }
        return block_transfer{neighbor, send_step - log2_num_members};
    }

    for(unsigned int index = 1; index < log2_num_members; index++) {
        if(rotated_rank & (1 << index)) {
            if(send_step + index < log2_num_members) {
                return boost::none;
            }
            size_t block_number =
                min(send_step + index - log2_num_members, num_blocks - 1);
            return block_transfer{neighbor, block_number};
        }
    }

    size_t block_number = min(send_step, num_blocks - 1);
    return block_transfer{neighbor, block_number};
}
optional<group::block_transfer> binomial_group::get_vertex_incoming_transfer(
    uint32_t vertex, size_t send_step, uint32_t num_members,
    unsigned int log2_num_members, size_t num_blocks, size_t total_steps) {
    size_t step_index = send_step % log2_num_members;
    uint32_t neighbor = vertex ^ (1 << step_index);

    auto transfer =
        get_vertex_outgoing_transfer(neighbor, send_step, num_members,
                                     log2_num_members, num_blocks, total_steps);
    if(!transfer) return boost::none;
    return block_transfer{neighbor, transfer->block_number};
}
optional<group::block_transfer> binomial_group::get_outgoing_transfer(
    uint32_t node, size_t step, uint32_t num_members,
    unsigned int log2_num_members, size_t num_blocks, size_t total_steps) {
    uint32_t vertex = (node < (1u << log2_num_members))
                          ? node
                          : node + 1 - (1 << log2_num_members);
    uint32_t intervertex_receiver = get_intervertex_receiver(
        vertex, step, num_members, log2_num_members, num_blocks, total_steps);

    if(step >= total_steps) {
        return boost::none;
    } else if(step == total_steps - 1 && num_blocks == 1 &&
              num_members > (1u << log2_num_members)) {
        uint32_t intervertex_receiver =
            get_intervertex_receiver(vertex, step, num_members,
                                     log2_num_members, num_blocks, total_steps);

        uint32_t target =
            get_intervertex_receiver(vertex ^ 1, step, num_members,
                                     log2_num_members, num_blocks, total_steps);

        bool node_has_twin =
            node != 0 && vertex + (1 << log2_num_members) - 1 < num_members;
        bool target_has_twin =
            target != 0 &&
            (target >= (1u << log2_num_members) ||
             target + (1u << log2_num_members) - 1 < num_members);

        if((node_has_twin && node == intervertex_receiver) || node == 1 ||
           !target_has_twin)
            return boost::none;
        else
            return block_transfer{target, 0};
    } else if(node == intervertex_receiver && vertex != 0 &&
              vertex + (1u << log2_num_members) - 1 < num_members) {
        auto block =
            get_intravertex_block(vertex, step, num_members, log2_num_members,
                                  num_blocks, total_steps);

        if(!block) return boost::none;
        uint32_t twin = (node < (1u << log2_num_members))
                            ? node + (1 << log2_num_members) - 1
                            : node + 1 - (1 << log2_num_members);
        return block_transfer{twin, *block};
    } else {
        if(step == total_steps - 1 && num_members > 1u << log2_num_members) {
            if((vertex + (1u << log2_num_members) - 1) >= num_members ||
               vertex == 0)
                return boost::none;

            uint32_t twin = (node < (1u << log2_num_members))
                                ? node + (1 << log2_num_members) - 1
                                : node + 1 - (1 << log2_num_members);

            size_t s = step;
            uint32_t r = twin;
            while(r == twin) {
                assert(s > 0);
                r = get_intervertex_receiver(vertex, --s, num_members,
                                             log2_num_members, num_blocks,
                                             total_steps);
            }
            auto transfer = get_vertex_incoming_transfer(
                vertex, s, num_members, log2_num_members, num_blocks,
                total_steps);

            assert(transfer);
            transfer->target = twin;
            return transfer;
        }

        auto transfer = get_vertex_outgoing_transfer(vertex, step, num_members,
                                                     log2_num_members,
                                                     num_blocks, total_steps);

        if(transfer) {
            transfer->target = get_intervertex_receiver(
                transfer->target, step, num_members, log2_num_members,
                num_blocks, total_steps);
        }
        return transfer;
    }
}
optional<group::block_transfer> binomial_group::get_incoming_transfer(
    uint32_t node, size_t step, uint32_t num_members,
    unsigned int log2_num_members, size_t num_blocks, size_t total_steps) {
    uint32_t vertex = (node < (1u << log2_num_members))
                          ? node
                          : node + 1 - (1 << log2_num_members);
    uint32_t intervertex_receiver = get_intervertex_receiver(
        vertex, step, num_members, log2_num_members, num_blocks, total_steps);

    if(step >= total_steps) {
        return boost::none;
    } else if(step == total_steps - 1 && num_blocks == 1 &&
              num_members > (1u << log2_num_members)) {
        uint32_t target =
            get_intervertex_receiver(vertex ^ 1, step, num_members,
                                     log2_num_members, num_blocks, total_steps);

        bool node_has_twin =
            node != 0 && vertex + (1 << log2_num_members) - 1 < num_members;

        if(target >= (1u << log2_num_members))
            target = target + 1 - (1 << log2_num_members);
        else if(target != 0 &&
                target + (1 << log2_num_members) - 1 < num_members)
            target = target + (1 << log2_num_members) - 1;

        if(!node_has_twin || node != intervertex_receiver)
            return boost::none;
        else {
            return block_transfer{target, 0};
        }
    } else if(node != intervertex_receiver) {
        auto block =
            get_intravertex_block(vertex, step, num_members, log2_num_members,
                                  num_blocks, total_steps);

        if(!block) return boost::none;
        uint32_t twin = (node < (1u << log2_num_members))
                            ? node + (1 << log2_num_members) - 1
                            : node + 1 - (1 << log2_num_members);
        return block_transfer{twin, *block};
    } else {
        if(step == total_steps - 1 && num_members > 1u << log2_num_members) {
            if((vertex + (1u << log2_num_members) - 1) >= num_members ||
               vertex == 0)
                return boost::none;

            uint32_t twin = (node < (1u << log2_num_members))
                                ? node + (1 << log2_num_members) - 1
                                : node + 1 - (1 << log2_num_members);

            auto transfer =
                get_outgoing_transfer(twin, step, num_members, log2_num_members,
                                      num_blocks, total_steps);

            assert(transfer);

            transfer->target = twin;
            return transfer;
        }

        auto transfer = get_vertex_incoming_transfer(vertex, step, num_members,
                                                     log2_num_members,
                                                     num_blocks, total_steps);
        if(transfer) {
            uint32_t neighbor = get_intervertex_receiver(
                transfer->target, step, num_members, log2_num_members,
                num_blocks, total_steps);

            if(neighbor == 0)
                transfer->target = neighbor;
            else if(neighbor >= (1u << log2_num_members))
                transfer->target = neighbor + 1 - (1u << log2_num_members);
            else if(neighbor + (1u << log2_num_members) - 1 < num_members)
                transfer->target = neighbor + (1u << log2_num_members) - 1;
            else
                transfer->target = neighbor;
        }
        return transfer;
    }
}
uint32_t binomial_group::get_intervertex_receiver(uint32_t vertex, size_t step,
                                                  uint32_t num_members,
                                                  unsigned int log2_num_members,
                                                  size_t num_blocks,
                                                  size_t total_steps) {
    // If the vertex only has one node, then no intravertex transfer can take
    // place.
    if((vertex + (1u << log2_num_members) - 1) >= num_members || vertex == 0)
        return vertex;

    size_t weight = 0;
    for(int i = 0; i < 32; i++) {
        if(vertex & (1 << i)) weight++;
    }

    // Compute the number of times the intravertex sender has flipped since the
    // start of the message. We do not count the current step, because that
    // will not affect the transfer going on now.
    auto total_flips = [&](size_t step) {
        size_t flips = (step / log2_num_members) * weight;
        for(unsigned int i = 0; i < step % log2_num_members; i++) {
            if(vertex & (1 << i)) flips++;
        }
        return flips;
    };

    if(total_flips(step) % 2 == 1) {
        return vertex + (1u << log2_num_members) - 1;
    }
    return vertex;
}
optional<size_t> binomial_group::get_intravertex_block(
    uint32_t vertex, size_t step, uint32_t num_members,
    unsigned int log2_num_members, size_t num_blocks, size_t total_steps) {
    // If the vertex only has one node, then no intravertex transfer can take
    // place.
    if((vertex + (1u << log2_num_members) - 1) >= num_members || vertex == 0)
        return boost::none;

    size_t weight = 0;
    for(int i = 0; i < 32; i++) {
        if(vertex & (1 << i)) weight++;
    }

    // Compute the number of times the intravertex sender has flipped since the
    // start of the message. We do not count the current step, because that
    // will not affect the transfer going on now.
    auto total_flips = [&](size_t step) {
        size_t flips = ((step) / log2_num_members) * weight;
        for(unsigned int i = 0; i < (step) % log2_num_members; i++) {
            if(vertex & (1 << i)) flips++;
        }
        return flips;
    };

    size_t flips = total_flips(step);

    // The first block received triggers a flip. If we haven't gotten it yet,
    // then clearly there can't be an intravertex transfer.
    if(flips == 0) {
        return boost::none;
    }

    // uint32_t target = vertex;
    // if(flips % 2 == 0) {
    //     target += (1 << log2_num_members) - 1;
    // }

    size_t prev_receive_block_step = step - 1;
    if(flips != total_flips(step - 1)) {
        if(flips <= 1) return boost::none;

        while(total_flips(prev_receive_block_step) != flips - 2) {
            --prev_receive_block_step;
        }
    }

    auto last = get_vertex_incoming_transfer(vertex, prev_receive_block_step,
                                             num_members, log2_num_members,
                                             num_blocks, total_steps);

    if(!last) return boost::none;
    return last->block_number;
}

optional<group::block_transfer> binomial_group::get_outgoing_transfer(
    size_t step) const {
    return get_outgoing_transfer(member_index, step, num_members,
                                 log2_num_members, num_blocks,
                                 get_total_steps());
}
optional<group::block_transfer> binomial_group::get_incoming_transfer(
    size_t step) const {
    return get_incoming_transfer(member_index, step, num_members,
                                 log2_num_members, num_blocks,
                                 get_total_steps());
}

optional<group::block_transfer> binomial_group::get_first_block() const {
    if(member_index == 0) return boost::none;

    size_t simulated_total_steps = num_members == 1u << log2_num_members
                                       ? 1024 + log2_num_members - 1
                                       : 1024 + log2_num_members;

    size_t step = 0;
    optional<block_transfer> transfer;
    while(!transfer) {
        transfer = get_incoming_transfer(member_index, step++, num_members,
                                         log2_num_members, 1024,
                                         simulated_total_steps);
        assert(step < simulated_total_steps);
    }

    return transfer;
}
