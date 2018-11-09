
#include "schedule.h"

#include <cassert>
#include <climits>

using std::min;
using std::optional;

#ifndef NDEBUG
#define assert_always(x...) assert(x)
#else
#define assert_always(x...) if(!x){abort();}
#endif

vector<uint32_t> chain_schedule::get_connections() const {
    // establish connection with member_index-1 and member_index+1, if they
    // exist
    if(member_index == 0) {
        if(num_members == 1) {
            return {};
        }
        // with only node with rank 1
        return {1};
    } else if(member_index == num_members - 1) {
        return {num_members - 2};
    } else {
        return {member_index - 1, member_index + 1};
    }
}
size_t chain_schedule::get_total_steps(size_t num_blocks) const {
    size_t total_steps = num_blocks + num_members - 2;
    return total_steps;
}
optional<schedule::block_transfer> chain_schedule::get_outgoing_transfer(size_t num_blocks, size_t step) const {
    size_t block_number = step - member_index;

    if(member_index > step || block_number >= num_blocks || member_index == num_members - 1) {
        return std::nullopt;
    }

    return block_transfer{(uint32_t)(member_index + 1), block_number};
}
optional<schedule::block_transfer> chain_schedule::get_incoming_transfer(size_t num_blocks, size_t step) const {
    size_t block_number = (step + 1) - member_index;
    if(member_index > step + 1 || block_number >= num_blocks || member_index == 0) {
        return std::nullopt;
    }
    return block_transfer{(uint32_t)(member_index - 1), block_number};
}
optional<schedule::block_transfer> chain_schedule::get_first_block(size_t num_blocks) const {
    if(member_index == 0) return std::nullopt;
    return block_transfer{(uint32_t)(member_index - 1), 0};
}

vector<uint32_t> sequential_schedule::get_connections() const {
    // if sender, connect to every receiver
    if(member_index == 0) {
        vector<uint32_t> ret;
        for(auto i = 1u; i < num_members; ++i) {
            ret.push_back(i);
        }
        return ret;
    }
    // if receiver, connect only to sender
    else {
        return {0};
    }
}
size_t sequential_schedule::get_total_steps(size_t num_blocks) const {
    return num_blocks * (num_members - 1);
}
optional<schedule::block_transfer> sequential_schedule::get_outgoing_transfer(size_t num_blocks, size_t step) const {
    if(member_index > 0 || step >= num_blocks * (num_members - 1)) {
        return std::nullopt;
    }

    size_t block_number = step % num_blocks;
    return block_transfer{(uint32_t)(1 + step / num_blocks), block_number};
}
optional<schedule::block_transfer> sequential_schedule::get_incoming_transfer(size_t num_blocks, size_t step) const {
    if(1 + step / num_blocks != member_index) {
        return std::nullopt;
    }

    size_t block_number = step % num_blocks;
    return block_transfer{(uint32_t)0, block_number};
}
optional<schedule::block_transfer> sequential_schedule::get_first_block(size_t num_blocks) const {
    if(member_index == 0) return std::nullopt;
    return block_transfer{0, 0};
}

vector<uint32_t> tree_schedule::get_connections() const {
    vector<uint32_t> ret;
    for(uint32_t i = 0; i < 32; i++) {
        if(member_index != 0 && (2ull << i) > member_index) {
            ret.push_back(member_index - (1 << i));
            break;
        }
    }

    for(uint32_t i = 0; i < 32 && num_members - member_index > (1u << i); i++) {
        if((1u << i) > member_index) {
            ret.push_back(member_index + (1 << i));
        }
    }
    return ret;
}
size_t tree_schedule::get_total_steps(size_t num_blocks) const {
    unsigned int log2_num_members = ceil(log2(num_members));
    return num_blocks * log2_num_members;
}
optional<schedule::block_transfer> tree_schedule::get_outgoing_transfer(size_t num_blocks, size_t step) const {
    size_t stage = step / num_blocks;
    if(step >= get_total_steps(num_blocks) || (1u << stage) <= member_index || (1u << stage) >= num_members - member_index) {
        return std::nullopt;
    } else {
        return block_transfer{member_index + (1u << stage),
                              step - stage * num_blocks};
    }
}
optional<schedule::block_transfer> tree_schedule::get_incoming_transfer(size_t num_blocks, size_t step) const {
    size_t stage = step / num_blocks;
    if(step < get_total_steps(num_blocks) && (1u << stage) <= member_index && member_index < (2u << stage)) {
        return block_transfer{member_index - (1u << stage),
                              step - stage * num_blocks};
    } else {
        return std::nullopt;
    }
}
optional<schedule::block_transfer> tree_schedule::get_first_block(size_t num_blocks) const {
    if(member_index == 0) return std::nullopt;

    for(uint32_t i = 0; i < 32; i++) {
        if((2ull << i) > member_index)
            return block_transfer{member_index - (1 << i), 0};
    }
    assert_always(false);
}
vector<uint32_t> binomial_schedule::get_connections() const {
    vector<uint32_t> ret;

    uint32_t twin = (member_index < (1u << log2_num_members))
                            ? member_index + (1 << log2_num_members) - 1
                            : member_index + 1 - (1 << log2_num_members);

    uint32_t vertex = min(member_index, twin);

    if(member_index > 0 && twin < num_members) ret.push_back(twin);

    for(size_t i = 0; i < log2_num_members; ++i) {
        // connect to num_members^ (1 << i)
        uint32_t neighbor = vertex ^ (1 << i);
        uint32_t neighbor_twin = neighbor + (1 << log2_num_members) - 1;

        ret.push_back(neighbor);
        if(neighbor > 0 && neighbor_twin < num_members) {
            ret.push_back(neighbor_twin);
        }
    }
    return ret;
}

size_t binomial_schedule::get_total_steps(size_t num_blocks) const {
    if(1u << log2_num_members == num_members)
        return num_blocks + log2_num_members - 1;

    return num_blocks + log2_num_members;
}
optional<schedule::block_transfer> binomial_schedule::get_vertex_outgoing_transfer(
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

    size_t rank_mask = (~((size_t)0)) >> (sizeof(size_t) * CHAR_BIT - log2_num_members);

    size_t step_index = send_step % log2_num_members;
    uint32_t neighbor = sender ^ (1 << step_index);
    if(send_step >= total_steps || neighbor == 0 || (send_step == total_steps - 1 && num_members > 1u << log2_num_members)) {
        //        printf("send_step = %d, neighbor = %d, log2(...) = %f\n",
        // (int)send_step, (int)neighbor, log2(member_index|neighbor));
        //        fflush(stdout);
        return std::nullopt;
    }

    size_t rotated_rank = ((neighbor | (neighbor << log2_num_members)) >> step_index) & rank_mask;
    //    printf("send_step = %d, rotated_rank = %x\n", (int)send_step,
    // (int)rotated_rank);
    //    fflush(stdout);

    if((rotated_rank & 1) == 0) {
        if(send_step < log2_num_members) {
            //            printf("send_step < log2_num_members\n");
            //            fflush(stdout);
            return std::nullopt;
        }
        return block_transfer{neighbor, send_step - log2_num_members};
    }

    for(unsigned int index = 1; index < log2_num_members; index++) {
        if(rotated_rank & (1 << index)) {
            if(send_step + index < log2_num_members) {
                return std::nullopt;
            }
            size_t block_number = min(send_step + index - log2_num_members, num_blocks - 1);
            return block_transfer{neighbor, block_number};
        }
    }

    size_t block_number = min(send_step, num_blocks - 1);
    return block_transfer{neighbor, block_number};
}
optional<schedule::block_transfer> binomial_schedule::get_vertex_incoming_transfer(
        uint32_t vertex, size_t send_step, uint32_t num_members,
        unsigned int log2_num_members, size_t num_blocks, size_t total_steps) {
    size_t step_index = send_step % log2_num_members;
    uint32_t neighbor = vertex ^ (1 << step_index);

    auto transfer = get_vertex_outgoing_transfer(neighbor, send_step, num_members,
                                                 log2_num_members, num_blocks, total_steps);
    if(!transfer) return std::nullopt;
    return block_transfer{neighbor, transfer->block_number};
}
optional<schedule::block_transfer> binomial_schedule::get_outgoing_transfer(
        uint32_t node, size_t step, uint32_t num_members,
        unsigned int log2_num_members, size_t num_blocks, size_t total_steps) {
    uint32_t vertex = (node < (1u << log2_num_members))
                              ? node
                              : node + 1 - (1 << log2_num_members);
    uint32_t intervertex_receiver = get_intervertex_receiver(
            vertex, step, num_members, log2_num_members, num_blocks, total_steps);

    if(step >= total_steps) {
        return std::nullopt;
    } else if(step == total_steps - 1 && num_blocks == 1 && num_members > (1u << log2_num_members)) {
        uint32_t intervertex_receiver = get_intervertex_receiver(vertex, step, num_members,
                                                                 log2_num_members, num_blocks, total_steps);

        uint32_t target = get_intervertex_receiver(vertex ^ 1, step, num_members,
                                                   log2_num_members, num_blocks, total_steps);

        bool node_has_twin = node != 0 && vertex + (1 << log2_num_members) - 1 < num_members;
        bool target_has_twin = target != 0 && (target >= (1u << log2_num_members) || target + (1u << log2_num_members) - 1 < num_members);

        if((node_has_twin && node == intervertex_receiver) || node == 1 || !target_has_twin)
            return std::nullopt;
        else
            return block_transfer{target, 0};
    } else if(node == intervertex_receiver && vertex != 0 && vertex + (1u << log2_num_members) - 1 < num_members) {
        auto block = get_intravertex_block(vertex, step, num_members, log2_num_members,
                                           num_blocks, total_steps);

        if(!block) return std::nullopt;
        uint32_t twin = (node < (1u << log2_num_members))
                                ? node + (1 << log2_num_members) - 1
                                : node + 1 - (1 << log2_num_members);
        return block_transfer{twin, *block};
    } else {
        if(step == total_steps - 1 && num_members > 1u << log2_num_members) {
            if((vertex + (1u << log2_num_members) - 1) >= num_members || vertex == 0)
                return std::nullopt;

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
optional<schedule::block_transfer> binomial_schedule::get_incoming_transfer(
        uint32_t node, size_t step, uint32_t num_members,
        unsigned int log2_num_members, size_t num_blocks, size_t total_steps) {
    uint32_t vertex = (node < (1u << log2_num_members))
                              ? node
                              : node + 1 - (1 << log2_num_members);
    uint32_t intervertex_receiver = get_intervertex_receiver(
            vertex, step, num_members, log2_num_members, num_blocks, total_steps);

    if(step >= total_steps) {
        return std::nullopt;
    } else if(step == total_steps - 1 && num_blocks == 1 && num_members > (1u << log2_num_members)) {
        uint32_t target = get_intervertex_receiver(vertex ^ 1, step, num_members,
                                                   log2_num_members, num_blocks, total_steps);

        bool node_has_twin = node != 0 && vertex + (1 << log2_num_members) - 1 < num_members;

        if(target >= (1u << log2_num_members))
            target = target + 1 - (1 << log2_num_members);
        else if(target != 0 && target + (1 << log2_num_members) - 1 < num_members)
            target = target + (1 << log2_num_members) - 1;

        if(!node_has_twin || node != intervertex_receiver)
            return std::nullopt;
        else {
            return block_transfer{target, 0};
        }
    } else if(node != intervertex_receiver) {
        auto block = get_intravertex_block(vertex, step, num_members, log2_num_members,
                                           num_blocks, total_steps);

        if(!block) return std::nullopt;
        uint32_t twin = (node < (1u << log2_num_members))
                                ? node + (1 << log2_num_members) - 1
                                : node + 1 - (1 << log2_num_members);
        return block_transfer{twin, *block};
    } else {
        if(step == total_steps - 1 && num_members > 1u << log2_num_members) {
            if((vertex + (1u << log2_num_members) - 1) >= num_members || vertex == 0)
                return std::nullopt;

            uint32_t twin = (node < (1u << log2_num_members))
                                    ? node + (1 << log2_num_members) - 1
                                    : node + 1 - (1 << log2_num_members);

            auto transfer = get_outgoing_transfer(twin, step, num_members, log2_num_members,
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
uint32_t binomial_schedule::get_intervertex_receiver(uint32_t vertex, size_t step,
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
optional<size_t> binomial_schedule::get_intravertex_block(
        uint32_t vertex, size_t step, uint32_t num_members,
        unsigned int log2_num_members, size_t num_blocks, size_t total_steps) {
    // If the vertex only has one node, then no intravertex transfer can take
    // place.
    if((vertex + (1u << log2_num_members) - 1) >= num_members || vertex == 0)
        return std::nullopt;

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
        return std::nullopt;
    }

    // uint32_t target = vertex;
    // if(flips % 2 == 0) {
    //     target += (1 << log2_num_members) - 1;
    // }

    size_t prev_receive_block_step = step - 1;
    if(flips != total_flips(step - 1)) {
        if(flips <= 1) return std::nullopt;

        while(total_flips(prev_receive_block_step) != flips - 2) {
            --prev_receive_block_step;
        }
    }

    auto last = get_vertex_incoming_transfer(vertex, prev_receive_block_step,
                                             num_members, log2_num_members,
                                             num_blocks, total_steps);

    if(!last) return std::nullopt;
    return last->block_number;
}

optional<schedule::block_transfer> binomial_schedule::get_outgoing_transfer(size_t num_blocks, size_t step) const {
    return get_outgoing_transfer(member_index, step, num_members,
                                 log2_num_members, num_blocks,
                                 get_total_steps(num_blocks));
}
optional<schedule::block_transfer> binomial_schedule::get_incoming_transfer(size_t num_blocks, size_t step) const {
    return get_incoming_transfer(member_index, step, num_members,
                                 log2_num_members, num_blocks,
                                 get_total_steps(num_blocks));
}

optional<schedule::block_transfer> binomial_schedule::get_first_block(size_t num_blocks) const {
    if(member_index == 0) return std::nullopt;

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
