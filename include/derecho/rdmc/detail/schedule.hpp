#ifndef SCHEDULE_HPP
#define SCHEDULE_HPP

#include <cmath>
#include <optional>
#include <vector>

using std::vector;
using std::optional;

class schedule {
protected:
    const uint32_t num_members = 0;
    const uint32_t member_index = 0;

public:
    schedule(uint32_t members, uint32_t index)
            : num_members(members),
              member_index(index) {}
    virtual ~schedule() = default;

    struct block_transfer {
        uint32_t target;
        size_t block_number;
    };

    virtual vector<uint32_t> get_connections() const = 0;
    virtual optional<block_transfer> get_outgoing_transfer(size_t num_blocks, size_t send_step) const = 0;
    virtual optional<block_transfer> get_incoming_transfer(size_t num_blocks, size_t receive_step) const = 0;
    virtual optional<block_transfer> get_first_block(size_t num_blocks) const = 0;
    virtual size_t get_total_steps(size_t num_blocks) const = 0;
};

class chain_schedule : public schedule {
public:
    using schedule::schedule;
    vector<uint32_t> get_connections() const;
    optional<block_transfer> get_outgoing_transfer(size_t num_blocks, size_t send_step) const;
    optional<block_transfer> get_incoming_transfer(size_t num_blocks, size_t receive_step) const;
    optional<block_transfer> get_first_block(size_t num_blocks) const;
    size_t get_total_steps(size_t num_blocks) const;
};

class sequential_schedule : public schedule {
public:
    using schedule::schedule;
    vector<uint32_t> get_connections() const;
    optional<block_transfer> get_outgoing_transfer(size_t num_blocks, size_t send_step) const;
    optional<block_transfer> get_incoming_transfer(size_t num_blocks, size_t receive_step) const;
    optional<block_transfer> get_first_block(size_t num_blocks) const;
    size_t get_total_steps(size_t num_blocks) const;
};

class tree_schedule : public schedule {
public:
    using schedule::schedule;
    vector<uint32_t> get_connections() const;
    optional<block_transfer> get_outgoing_transfer(size_t num_blocks, size_t send_step) const;
    optional<block_transfer> get_incoming_transfer(size_t num_blocks, size_t receive_step) const;
    optional<block_transfer> get_first_block(size_t num_blocks) const;
    size_t get_total_steps(size_t num_blocks) const;
};

class binomial_schedule : public schedule {
private:
    // Base to logarithm of the group size, rounded down.
    const unsigned int log2_num_members;

    optional<block_transfer> get_vertex_outgoing_transfer(size_t send_step);
    optional<block_transfer> get_vertex_incoming_transfer(size_t receive_step);

public:
    binomial_schedule(uint32_t members, uint32_t index)
            : schedule(members, index),
              log2_num_members(floor(log2(num_members))) {}

    static optional<block_transfer> get_vertex_outgoing_transfer(
            uint32_t vertex, size_t step, uint32_t num_members,
            unsigned int log2_num_members, size_t num_blocks, size_t total_steps);
    static optional<block_transfer> get_vertex_incoming_transfer(
            uint32_t vertex, size_t step, uint32_t num_members,
            unsigned int log2_num_members, size_t num_blocks, size_t total_steps);
    static optional<block_transfer> get_outgoing_transfer(
            uint32_t node, size_t step, uint32_t num_members,
            unsigned int log2_num_members, size_t num_blocks, size_t total_steps);
    static optional<block_transfer> get_incoming_transfer(
            uint32_t node, size_t step, uint32_t num_members,
            unsigned int log2_num_members, size_t num_blocks, size_t total_steps);
    static optional<size_t> get_intravertex_block(uint32_t vertex, size_t step,
                                                  uint32_t num_members,
                                                  unsigned int log2_num_members,
                                                  size_t num_blocks,
                                                  size_t total_steps);
    static uint32_t get_intervertex_receiver(uint32_t vertex, size_t step,
                                             uint32_t num_members,
                                             unsigned int log2_num_members,
                                             size_t num_blocks,
                                             size_t total_steps);

    vector<uint32_t> get_connections() const;
    optional<block_transfer> get_outgoing_transfer(size_t num_blocks, size_t send_step) const;
    optional<block_transfer> get_incoming_transfer(size_t num_blocks, size_t receive_step) const;
    optional<block_transfer> get_first_block(size_t num_blocks) const;
    size_t get_total_steps(size_t num_blocks) const;
};

#endif /* SCHEDULE_HPP */
