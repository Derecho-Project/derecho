#ifndef LF_HELPER_H
#define LF_HELPER_H

#include <cstdint>
#include <experimental/optional>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <rdma/fabric.h>

#define LF_VERSION FI_VERSION(1,5)

struct fid_mr;
struct fid_ep;
struct fid_cq;

/**
 * Contains functions and classes for low-level RDMA operations, such as setting
 * up memory regions and queue pairs. This provides a more C++-friendly interfaace
 * to the libfabric library.
 */
namespace rdma {
class exception {};
class invalid_args : public exception {};
class configuration_failure : public exception {};
class connection_broken : public exception {};
class creation_failure : public exception {};
class mr_creation_failure : public creation_failure {};
class ep_creation_failure : public creation_failure {};
class cq_creation_failure : public creation_failure {};
class message_types_exhausted : public exception {};
class unsupported_feature : public exception {};

/**
 * A wrapper for fi_close. The previous way this was done used a lambda and
 * std::function, but that incurs the overhead of type-erasure
 */
//template<typename fi_struct_type>
//struct close { 
//  void operator() (fi_struct_type* fi_struct) const {fi_close(fi_struct->fid);} 
//};

/**
 * A C++ wrapper for the libfabric fid_mr struct. Registers a memory region for 
 * the provided buffer on construction, and deregisters it on destruction.
 */
class memory_region {
    /** Smart pointer for managing the registered memory region */
    std::unique_ptr<fid_mr, std::function<void(fid_mr *)>> mr;
    /** Smart pointer for managing the buffer the mr uses */
    std::unique_ptr<char[]> allocated_buffer;

    friend class endpoint;
    friend class task;

public:
    /**
     * Constructor
     * Creates a buffer of the specified size and then calls the second 
     * constructor with the new buffer as an argument.
     *
     * @param size The size in bytes of the buffer to be associated with
     *     the memory region.
     */ 
    memory_region(size_t size);
    /**
     * Constructor
     * Registers a memory region using the specified buffer and size. 
     *
     * @param buffer The allocated memory that will be registered.
     * @param size The size in bytes of the buffer to be associated with
     *      the memory region.
     */ 
    memory_region(char* buffer, size_t size);
    /**
     * get_key
     * Returns the key associated with the registered memory region, which
     * is used to access the region.
     */ 
    uint64_t get_key() const;

    char* const buffer;
    const size_t size;
};

class remote_memory_region {
public:
    /**
     * Constructor
     * Takes in parameters representing a remote memory region.
     *
     * @param remote_address The address of the remote buffer.
     * @param length The size of the remote buffer in bytes.
     * @param remote_key The key used to refer to the buffer
     *     for remote accesses.
     */
    remote_memory_region(uint64_t remote_address, size_t length,
                         uint64_t remote_key)
        : buffer(remote_address), size(length), rkey(remote_key) {}

    const uint64_t buffer;
    const size_t size;
    const uint64_t rkey;
};

/**
 * A C++ wrapper for the libfabric fid_cq struct and its associated functions.
 */
class completion_queue {
    /** Smart pointer for managing the completion queue */
    std::unique_ptr<fid_cq, std::function<void(fid_cq *)>> cq;

    friend class managed_endpoint;
    friend class task;

public:
    /**
     * Constructor
     * Uses the libfabrics API to open a completion queue
     */
    explicit completion_queue();
};

typedef std::function<void(uint64_t tag, uint32_t immediate, size_t length)> 
        completion_handler;

class message_type {
public:
    typedef uint8_t tag_type;
    static constexpr unsigned int shift_bits = 64 - 8 * sizeof(tag_type);

private:
    std::experimental::optional<tag_type> tag;
    message_type(tag_type t) : tag(t) {}

    friend class endpoint;
    friend class task;

public:
    message_type(const std::string& name, completion_handler send_handler,
                 completion_handler recv_handler,
                 completion_handler write_handler = nullptr);
    message_type() {}

    static message_type ignored();
};

/**
 * A C++ wrapper for the libfabric fid_ep struct and its associated functions.
 */
class endpoint {
protected:
    /** Smart pointer for managing the endpoint */
    std::unique_ptr<fid_ep, std::function<void(fid_ep *)>> ep;
    std::unique_ptr<fid_eq, std::function<void(fid_eq *)>> eq;

    explicit endpoint() {}

    friend class task;
public:
    virtual ~endpoint();
    
    /**
     * Constructor
     * Calls the second constructor with an empty lambda as the second argument.
     *
     * @param remote_index The id of the remote node.
     */ 
    explicit endpoint(size_t remote_index, bool is_lf_server);
     /**
     * Constructor
     * Initializes members and then calls endpoint::connect.
     *
     * @param remote_index The index of the remote node in the group.
     * @param is_lf_server This parameter decide local role in connection.
     *     If is_lf_server is true, it waits on PEP for connection from remote
     *     side. Otherwise, it initiate a connection to remote side.
     * @param post_recvs A lambda that is called at the end of initializing the
     *     endpoints on the client and remote sides to avoid race conditions 
     *     between post_send() and post_recv().
     */    
    endpoint(size_t remote_index, bool is_lf_server,
             std::function<void(endpoint*)> post_recvs);
    /**
     * Constructor 
     * Default move constructor
     */ 
    endpoint(endpoint&&) = default;
    /**
     * init
     * Creates an endpoint, and then initializes/enables it
     *
     * @param fi A struct containing information about the current 
     *     fabric services.
     */ 
    int init(struct fi_info *fi);
    /**
     * connect
     * Uses the initialized endpoint to connect to a remote node
     *
     * @param remote_index The index of the remote node in the group. 
     * @param is_lf_server This parameter decide local role in connection.
     *     If is_lf_server is true, it waits on PEP for connection from remote
     *     side. Otherwise, it initiate a connection to remote side.
     * @param post_recvs A lambda that is called at the end of initializing the
     *     endpoints on the client and remote sides to avoid race conditions 
     *     between post_send() and post_recv().
     */ 
    void connect(size_t remote_index, bool is_lf_server,
                 std::function<void(endpoint *)> post_recvs);

    /**
     * post_send
     * Uses the libfabrics API to post a buffer to an endpoint.
     *
     * @param mr The wrapper around the memory region that is being sent.
     * @param offset The offset into the buffer managed by mr.
     * @param size The size (in bytes) of the buffer being sent.
     * @param wr_id A parameter used to differentiate types of messages.
     * @param immediate A parameter used only for send operations.
     * @param message_type 
     */
    bool post_send(const memory_region& mr, size_t offset, 
                   size_t size, uint64_t wr_id, uint32_t immediate, 
                   const message_type& type);
    /**
     * post_recv
     * Uses the libfabrics API to post a buffer to the recv queue of an endpoint.
     *
     * @param mr The wrapper around the memory region that is being posted.
     * @param offset The offset into the buffer managed by mr.
     * @param size The size (in bytes) of the buffer.
     * @param wr_id A parameter used to differentiate types of messages.
     * @param message_type  
     */
    bool post_recv(const memory_region& mr, size_t offset, 
                   size_t size, uint64_t wr_id, 
                   const message_type& type);

    bool post_empty_send(uint64_t wr_id, uint32_t immediate,
                         const message_type& type);
    bool post_empty_recv(uint64_t wr_id, const message_type& type);

    bool post_write(const memory_region& mr, size_t offset, size_t size,
                    uint64_t wr_id, remote_memory_region remote_mr,
                    size_t remote_offset, const message_type& type,
                    bool signaled = false, bool send_inline = false);

    fi_addr_t remote_fi_addr;
};

class managed_endpoint : public endpoint {
public:
    completion_queue scq, rcq;
    /** TODO Implement the constructor */
    managed_endpoint(size_t remote_index,
                       std::function<void(managed_endpoint*)> post_recvs) {}
};

class manager_endpoint : public endpoint {
public:
    /** TODO: Implement the constructor */
    explicit manager_endpoint() {}
};

class task {
protected:
    struct task_impl;
    std::unique_ptr<task_impl> impl;
    std::shared_ptr<manager_endpoint> mep;

public:
    task(std::shared_ptr<manager_endpoint> manager_ep);
    virtual ~task();

    void append_wait(const completion_queue& cq, int count, bool signaled,
                     bool last, uint64_t wr_id, const message_type& type);
    void append_enable_send(const managed_endpoint& ep, int count);
    void append_send(const managed_endpoint& ep, const memory_region& mr, 
                     size_t offset, size_t length, uint32_t immediate);
    void append_recv(const managed_endpoint& ep, const memory_region& mr, 
                     size_t offset, size_t length);
    bool post() __attribute__((warn_unused_result));
};

namespace impl {
bool lf_initialize(const std::map<uint32_t, std::string>& node_addresses,
                   uint32_t node_rank);
bool lf_add_connection(uint32_t new_id, const std::string new_ip_addr);
bool lf_destroy();

std::map<uint32_t, remote_memory_region> lf_exchange_memory_regions(
         const std::vector<uint32_t>& members, uint32_t node_rank,
         const memory_region& mr);

bool set_interrupt_mode(bool enabled);
} /* namespace impl */
} /* namespace rdma */

#endif /* LF_HELPER_H */
