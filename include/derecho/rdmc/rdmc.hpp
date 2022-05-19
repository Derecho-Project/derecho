
#ifndef RDMC_HPP
#define RDMC_HPP

#ifdef USE_VERBS_API
    #include "detail/verbs_helper.hpp"
#else
    #include "detail/lf_helper.hpp"
#endif

#include <array>
#include <cstdint>
#include <optional>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace rdmc {

class exception {};
class connection_broken : public exception {};
class invalid_args : public exception {};
class nonroot_sender : public exception {};
class group_busy : public exception {};

enum send_algorithm {
    BINOMIAL_SEND = 1,
    CHAIN_SEND = 2,
    SEQUENTIAL_SEND = 3,
    TREE_SEND = 4
};

struct receive_destination {
    std::shared_ptr<rdma::memory_region> mr;
    size_t offset;
};

typedef std::function<receive_destination(size_t size)>
        incoming_message_callback_t;
typedef std::function<void(uint8_t* buffer, size_t size)> completion_callback_t;
typedef std::function<void(std::optional<uint32_t> suspected_victim)>
        failure_callback_t;

bool initialize(const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>>& addresses,
                uint32_t node_rank) __attribute__((warn_unused_result));
void add_address(uint32_t index, const std::pair<ip_addr_t, uint16_t>& address);
void shutdown();

/**
 * Creates a new RDMC group.
 * @param group_number The group's unique identifier.
 * @param members A vector of node IDs representing the members of this group.
 * The order of this vector will be used as the rank order of the members.
 * @param block_size The size, in bytes, of blocks to use when sending in this
 * group.
 * @param algorithm Which RDMC send algorithm to use in this group.
 * @param incoming_receive The function to call when there is a new incoming
 * message in this group; it must provide a destination to receive the message
 * into.
 * @param send_callback The function to call when RDMC completes receiving a
 * message in this group
 * @param failure_callback The function to call when RDMC detects a failure in
 * this group. It will be called with the suspected failed node's ID.
 * @return True if group creation succeeds, false if it fails.
 */
bool create_group(uint16_t group_number, std::vector<uint32_t> members,
                  size_t block_size, send_algorithm algorithm,
                  incoming_message_callback_t incoming_receive,
                  completion_callback_t send_callback,
                  failure_callback_t failure_callback)
        __attribute__((warn_unused_result));
void destroy_group(uint16_t group_number);

bool send(uint16_t group_number, std::shared_ptr<rdma::memory_region> mr,
          size_t offset, size_t length) __attribute__((warn_unused_result));

// Convenience function to obtain the addresses of other nodes that might be
// part of group communication.
// void query_addresses(std::map<uint32_t, std::string>& addresses,
//                      uint32_t& node_rank);

class barrier_group {
    // Queue Pairs and associated remote memory regions used for performing a
    // barrier.
#ifdef USE_VERBS_API
    std::vector<rdma::queue_pair> queue_pairs;
#else
    std::vector<rdma::endpoint> endpoints;
#endif
    std::vector<rdma::remote_memory_region> remote_memory_regions;

    // Additional queue pairs which will handle incoming writes (but which this
    // node does not need to interact with directly).
#ifdef USE_VERBS_API
    std::vector<rdma::queue_pair> extra_queue_pairs;
#else
    std::vector<rdma::endpoint> extra_endpoints;
#endif

    // RDMA memory region used for doing the barrier
    std::array<volatile int64_t, 32> steps;
    std::unique_ptr<rdma::memory_region> steps_mr;

    // Current barrier number, and a memory region to issue writes from.
    volatile int64_t number = -1;
    std::unique_ptr<rdma::memory_region> number_mr;

    // Number of steps per barrier.
    unsigned int total_steps;

    // Lock to ensure that only one barrier is in flight at a time.
    std::mutex lock;

    // Index of this node in the list of members
    uint32_t member_index;
    uint32_t group_size;

public:
    barrier_group(std::vector<uint32_t> members);
    void barrier_wait();
};
};  // namespace rdmc

#endif /* RDMC_HPP */
