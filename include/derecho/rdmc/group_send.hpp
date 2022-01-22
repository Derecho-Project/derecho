#ifndef GROUP_SEND_HPP
#define GROUP_SEND_HPP

#include <derecho/rdmc/rdmc.hpp>
#include "detail/schedule.hpp"

#ifdef USE_VERBS_API
    #include "detail/verbs_helper.hpp"
#else
    #include "detail/lf_helper.hpp"
#endif

#include <optional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

using rdmc::completion_callback_t;
using rdmc::incoming_message_callback_t;
using std::map;
using std::unique_ptr;
using std::vector;
using std::optional;

class group {
protected:
    const vector<uint32_t> members;  // first element is the sender
    const uint16_t group_number;
    const size_t block_size;
    const uint32_t num_members;
    const uint32_t member_index;  // our index in the members list

    const unique_ptr<schedule> transfer_schedule;

    std::mutex monitor;

    std::shared_ptr<rdma::memory_region> mr;
    size_t mr_offset;
    size_t message_size;
    size_t num_blocks;

    completion_callback_t completion_callback;
    incoming_message_callback_t incoming_message_upcall;

    group(uint16_t group_number, size_t block_size,
          vector<uint32_t> members, uint32_t member_index,
          incoming_message_callback_t upcall,
          completion_callback_t callback,
          unique_ptr<schedule> transfer_schedule);

public:
    virtual ~group();

    virtual void receive_block(uint32_t send_imm, size_t size) = 0;
    virtual void receive_ready_for_block(uint32_t step, uint32_t sender) = 0;
    virtual void complete_block_send() = 0;
    virtual void send_message(std::shared_ptr<rdma::memory_region> message_mr,
                              size_t offset, size_t length)
            = 0;
};

class polling_group : public group {
private:
    // Set of receivers who are ready to receive the next block from us.
    std::set<uint32_t> receivers_ready;

    unique_ptr<rdma::memory_region> first_block_mr;
    optional<size_t> first_block_number;
    unique_ptr<uint8_t[]> first_block_buffer;

    size_t incoming_block;
    size_t message_number = 0;

    size_t outgoing_block;
    bool sending = false;  // Whether a block send is in progress
    size_t send_step = 0;  // Number of blocks sent/stalls so far

    // Total number of blocks received and the number of chunks
    // received for ecah block, respectively.
    size_t num_received_blocks = 0;
    size_t receive_step = 0;
    vector<bool> received_blocks;

    // maps from member_indices to the queue pairs
#ifdef USE_VERBS_API
    map<size_t, rdma::queue_pair> queue_pairs;
    map<size_t, rdma::queue_pair> rfb_queue_pairs;
#else
    map<size_t, rdma::endpoint> endpoints;
    map<size_t, rdma::endpoint> rfb_endpoints;
#endif
    static struct {
        rdma::message_type data_block;
        rdma::message_type ready_for_block;
    } message_types;

public:
    static void initialize_message_types();

    polling_group(uint16_t group_number, size_t block_size,
                  vector<uint32_t> members, uint32_t member_index,
                  incoming_message_callback_t upcall,
                  completion_callback_t callback,
                  unique_ptr<schedule> transfer_schedule);

    virtual void receive_block(uint32_t send_imm, size_t size);
    virtual void receive_ready_for_block(uint32_t step, uint32_t sender);
    virtual void complete_block_send();

    virtual void send_message(std::shared_ptr<rdma::memory_region> message_mr,
                              size_t offset, size_t length);

private:
    void post_recv(schedule::block_transfer transfer);
    void send_next_block();
    void complete_message();
    void prepare_for_next_message();
    void send_ready_for_block(uint32_t neighbor);
    void connect(uint32_t neighbor);
};

#endif /* GROUP_SEND_HPP */
