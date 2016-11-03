
#ifndef GROUP_SEND_H
#define GROUP_SEND_H

#include "schedule.h"
#include "message.h"
#include "rdmc.h"
#include "util.h"
#include "verbs_helper.h"

#include <boost/optional.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <vector>

using boost::optional;
using std::vector;
using std::map;
using std::unique_ptr;
using rdmc::incoming_message_callback_t;
using rdmc::completion_callback_t;

class group {
private:
    const vector<uint32_t> members;  // first element is the sender

    // Set of receivers who are ready to receive the next block from us.
    std::set<uint32_t> receivers_ready;

    std::mutex monitor;

    unique_ptr<rdma::memory_region> first_block_mr;
    optional<size_t> first_block_number;
    unique_ptr<char[]> first_block_buffer;

    std::shared_ptr<rdma::memory_region> mr;
    size_t mr_offset;
    size_t message_size;
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

    completion_callback_t completion_callback;
    incoming_message_callback_t incoming_message_upcall;

    const uint16_t group_number;
    const size_t block_size;
    const uint32_t num_members;
    const uint32_t member_index;  // our index in the members list
    size_t num_blocks;

    // maps from member_indices to the queue pairs
    map<size_t, rdma::queue_pair> queue_pairs;
    map<size_t, rdma::queue_pair> rfb_queue_pairs;

	unique_ptr<schedule> transfer_schedule;
	
public:
    static struct {
        rdma::message_type data_block;
        rdma::message_type ready_for_block;
    } message_types;

    group(uint16_t group_number, size_t block_size, vector<uint32_t> members,
          uint32_t member_index, incoming_message_callback_t upcall,
          completion_callback_t callback,
		  unique_ptr<schedule> transfer_schedule);
    virtual ~group();

    void receive_block(uint32_t send_imm, size_t size);
    void receive_ready_for_block(uint32_t step, uint32_t sender);
    void complete_block_send();

    void send_message(std::shared_ptr<rdma::memory_region> message_mr,
                      size_t offset, size_t length);

    // // This function is necessary because we can't issue a virtual function call
    // // from a constructor, but we need to perform them in order to know which
    // // queue pair to post the first_block_buffer on, and which nodes we must
    // // connect to.
    // bool init();

private:
    void post_recv(schedule::block_transfer transfer);
    void send_next_block();
    void complete_message();
    void prepare_for_next_message();
    void send_ready_for_block(uint32_t neighbor);
    void connect(uint32_t neighbor);
};

#endif /* GROUP_SEND_H */
