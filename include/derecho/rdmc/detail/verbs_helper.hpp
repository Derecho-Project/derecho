#ifndef VERBS_HELPER_HPP
#define VERBS_HELPER_HPP

#include <cstdint>
#include <optional>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <derecho/core/derecho_type_definitions.hpp>

struct ibv_mr;
struct ibv_qp;
struct ibv_cq;

/**
 * Contains functions and classes for low-level RDMA operations, such as setting
 * up memory regions and queue pairs. This provides a more C++-friendly
 * interface
 * to the IB Verbs library.
 */
namespace rdma {
// Various classes of exceptions
class exception {};
class invalid_args : public exception {};
class connection_broken : public exception {};
class creation_failure : public exception {};
class mr_creation_failure : public creation_failure {};
class cq_creation_failure : public creation_failure {};
class qp_creation_failure : public creation_failure {};
class message_types_exhausted : public exception {};
class unsupported_feature : public exception {};

/**
 * A C++ wrapper for the IB Verbs ibv_mr struct. Registers a memory region for
 * the provided buffer on construction, and deregisters it on destruction.
 * Instances of this class can only be created after global Verbs initialization
 * has been run, since it depends on the global Verbs resources.
 */
class memory_region {
    std::unique_ptr<ibv_mr, std::function<void(ibv_mr*)>> mr;
    std::unique_ptr<char[]> allocated_buffer;

    memory_region(size_t size, bool contiguous);
    friend class queue_pair;
    friend class task;

public:
    memory_region(size_t size);
    memory_region(char* buffer, size_t size);
    uint32_t get_rkey() const;

    char* const buffer;
    const size_t size;
};

class remote_memory_region {
public:
    remote_memory_region(uint64_t remote_address, size_t length,
                         uint32_t remote_key)
            : buffer(remote_address), size(length), rkey(remote_key) {}

    const uint64_t buffer;
    const size_t size;
    const uint32_t rkey;
};

class completion_queue {
    std::unique_ptr<ibv_cq, std::function<void(ibv_cq*)>> cq;
    friend class managed_queue_pair;
    friend class task;

public:
    explicit completion_queue(bool cross_channel);
};

typedef std::function<void(uint64_t tag, uint32_t immediate, size_t length)>
        completion_handler;

class message_type {
public:
    typedef uint8_t tag_type;
    static constexpr unsigned int shift_bits = 64 - 8 * sizeof(tag_type);

private:
    std::optional<tag_type> tag;
    message_type(tag_type t) : tag(t) {}

    friend class queue_pair;
    friend class task;

public:
    message_type(const std::string& name, completion_handler send_handler,
                 completion_handler recv_handler,
                 completion_handler write_handler = nullptr);
    message_type() {}

    static message_type ignored();
};

/**
 * A C++ wrapper for the IB Verbs ibv_qp struct and its associated functions.
 * Instances of this class can only be created after global Verbs initialization
 * has been run, since it depends on global Verbs resources.
 */
class queue_pair {
protected:
    std::unique_ptr<ibv_qp, std::function<void(ibv_qp*)>> qp;
    explicit queue_pair() {}

    friend class task;

public:
    ~queue_pair();
    explicit queue_pair(size_t remote_index);
    queue_pair(size_t remote_index,
               std::function<void(queue_pair*)> post_recvs);
    queue_pair(queue_pair&&) = default;
    bool post_send(const memory_region& mr, size_t offset, size_t length,
                   uint64_t wr_id, uint32_t immediate,
                   const message_type& type);
    bool post_recv(const memory_region& mr, size_t offset, size_t length,
                   uint64_t wr_id, const message_type& type);

    bool post_empty_send(uint64_t wr_id, uint32_t immediate,
                         const message_type& type);
    bool post_empty_recv(uint64_t wr_id, const message_type& type);

    bool post_write(const memory_region& mr, size_t offset, size_t length,
                    uint64_t wr_id, remote_memory_region remote_mr,
                    size_t remote_offset, const message_type& type,
                    bool signaled = false, bool send_inline = false);
};

class managed_queue_pair : public queue_pair {
public:
    completion_queue scq, rcq;
    managed_queue_pair(size_t remote_index,
                       std::function<void(managed_queue_pair*)> post_recvs);
};

class manager_queue_pair : public queue_pair {
public:
    explicit manager_queue_pair();
};

class task {
protected:
    struct task_impl;
    std::unique_ptr<task_impl> impl;
    std::shared_ptr<manager_queue_pair> mqp;

public:
    task(std::shared_ptr<manager_queue_pair> manager_qp);
    virtual ~task();

    void append_wait(const completion_queue& cq, int count, bool signaled,
                     bool last, uint64_t wr_id, const message_type& type);
    void append_enable_send(const managed_queue_pair& qp, int count);
    void append_send(const managed_queue_pair& qp, const memory_region& mr,
                     size_t offset, size_t length, uint32_t immediate);
    void append_recv(const managed_queue_pair& qp, const memory_region& mr,
                     size_t offset, size_t length);
    bool post() __attribute__((warn_unused_result));
};

struct feature_set {
    bool contiguous_memory;
    bool cross_channel;
};
feature_set get_supported_features();

namespace impl {
bool verbs_initialize(const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>>& ip_addrs_and_ports,
                      uint32_t node_rank);
// bool verbs_add_connection(uint32_t index, const std::string& address,
//                           uint32_t node_rank);
bool verbs_add_connection(uint32_t new_id, const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port);
bool verbs_remove_connection(uint32_t node_id);
void verbs_destroy();
// int poll_for_completions(int num, ibv_wc* wcs,
//                          std::atomic<bool>& shutdown_flag);

// This function exchanges memory regions with all other connected nodes which
// enables us to do one-sided RDMA operations between them. Due to its nature,
// the function requires that it is called simultaneously on all nodes and that
// only one execution is active at any time.
std::map<uint32_t, remote_memory_region> verbs_exchange_memory_regions(
        const std::vector<uint32_t>& members, uint32_t node_rank,
        const memory_region& mr);

bool set_interrupt_mode(bool enabled);
bool set_contiguous_memory_mode(bool enabled);

} /* namespace impl */
} /* namespace rdma */
#endif /* VERBS_HELPER_HPP */
