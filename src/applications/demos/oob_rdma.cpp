/**
 * @file oob_rdma.cpp
 *
 * This test creates one subgroup demonstrating OOB mechanism 
 * - between external clients and a group member, and
 * - between derecho members
 */

#include <oob_config.h>
#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>
#include <getopt.h>

#ifdef CUDA_FOUND
#include <cuda.h>

#define ASSERTDRV(stmt) \
    do { \
        CUresult result = (stmt); \
        if (result != CUDA_SUCCESS) { \
            const char* _err_name; \
            cuGetErrorName(result, &_err_name); \
            std::cerr << "CUDA Driver API error:" << _err_name << std::endl; \
        } \
        assert (CUDA_SUCCESS == result); \
    } while (false)

__attribute__ ((visibility ("hidden")))
struct global_ctxt {
    CUdevice    device;
    CUcontext   context;
    CUdeviceptr dev_ptr;
} cuda_ctxt;
#endif

struct OOBRDMADSM : public mutils::RemoteDeserializationContext {
    void*   oob_mr_ptr;
    size_t  oob_mr_size;
};

class OOBRDMA : public mutils::ByteRepresentable,
                public derecho::GroupReference {
private:
    uint32_t sudbgroup_index;
    void*   oob_mr_ptr;
    size_t  oob_mr_size;
public:
    /**
     * Naming issue: here the subject of 'put' is not the (remote) OOBRDMA object itself but the client side thread
     * holding the OOBRDMA object.
     *
     * put data (At @addr with @rkey of size @size)
     * @param caller_addr   the address on the caller side
     * @param rkey          the sender memory region's remote access key
     * @param size          the size of the data
     *
     * @return the address on the callee side where the data put.
     */
    uint64_t put(const uint64_t& caller_addr, const uint64_t rkey, const uint64_t size) const;

    /**
     * Naming issue: here the subject of 'get' is not the (remote) OOBRDMA object itself but the client side thread
     * holding the OOBRDMA object.
     *
     * get data (At @addr with @rkey of size @size)
     * @param callee_addr   the address on the callee side
     * @param caller_addr   the address on the caller side
     * @param rkey          the sender memory region's remote access key
     * @param size          the size of the data
     *
     * @return true for success, otherwise false.
     */ 
    bool get(const uint64_t& callee_addr, const uint64_t& caller_addr, const uint64_t rkey, const uint64_t size) const;

    /**
     * Naming issue: here the subject of 'send' is not the (remote) OOBRDMA object itself but the client side thread
     * holding the OOBRDMA object.
     *
     * send data (and asking the remote OOBRDMA object to receive it)
     * @param size          the size of the data just sent (and the OOBRDMA object need to receive this much)
     *
     * @return the address of remote buffer.
     */
    uint64_t send(const uint64_t size) const;

    /**
     * Naming issue: here the subject of 'send' is not the (remote) OOBRDMA object itself but the client side thread
     * holding the OOBRDMA object.
     *
     * recv data (and asking the remote OOBRDMA object to send it)
     * @param callee_addr   the address of the data on the caller node (and the remote OOBRDMA object need to send from
     *                      there.)
     * @param size          the size of the data to recv (and the OOBRDMA object need to send this much)
     *
     * @return true for success, otherwise false.
     */
    bool recv(const uint64_t& callee_addr, const uint64_t size) const;

    // constructors
    OOBRDMA(void* _oob_mr_ptr, size_t _oob_mr_size) : 
        oob_mr_ptr(_oob_mr_ptr),
        oob_mr_size(_oob_mr_size) {}

    // serialization supports
    virtual std::size_t to_bytes(uint8_t*) const override { return 0; }
    virtual void post_object(const std::function<void(uint8_t const* const, std::size_t)>&) const override {}
    virtual std::size_t bytes_size() const override { return 0; }
    static std::unique_ptr<OOBRDMA> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf) {
        if ( !dsm->registered<OOBRDMADSM>() ) {
            throw derecho::derecho_exception("OOBRDMA::from_bytes(): No OOBRDMADSM registered!");
        }
        return std::make_unique<OOBRDMA>(dsm->mgr<OOBRDMADSM>().oob_mr_ptr,dsm->mgr<OOBRDMADSM>().oob_mr_size);
    }
    static mutils::context_ptr<OOBRDMA> from_bytes_noalloc(mutils::DeserializationManager* dsm, uint8_t const* buf) {
        return mutils::context_ptr<OOBRDMA>(from_bytes(dsm, buf).release());
    }

    void ensure_registered(mutils::DeserializationManager&) {}

    REGISTER_RPC_FUNCTIONS(OOBRDMA,P2P_TARGETS(put,get,send,recv))
};

uint64_t OOBRDMA::put(const uint64_t& caller_addr, const uint64_t rkey, const uint64_t size) const {
    // STEP 1 - validate the memory size
    if (size > oob_mr_size) {
        std::cerr << "Cannot put " << size << " bytes of data, it's more than my memory region limit:" << oob_mr_size << std::endl;
        return 0ull;
    }
    // STEP 2 - get a random address
    uint64_t callee_addr = reinterpret_cast<uint64_t>(oob_mr_ptr) + (static_cast<uint64_t>(rand()%((oob_mr_size - size) >> 12))<<12);
    // STEP 3 - do RDMA read to get the OOB data.
    auto& subgroup_handle = group->template get_subgroup<OOBRDMA>(this->subgroup_index);
    struct iovec iov;
    iov.iov_base    = reinterpret_cast<void*>(callee_addr);
    iov.iov_len     = static_cast<size_t>(size);

    subgroup_handle.oob_remote_read(group->get_rpc_caller_id(),&iov,1,caller_addr,rkey,size);
    subgroup_handle.wait_for_oob_op(group->get_rpc_caller_id(),OOB_OP_READ,1000);

    return callee_addr;
}

bool OOBRDMA::get(const uint64_t& callee_addr, const uint64_t& caller_addr, const uint64_t rkey, const uint64_t size) const {
    // STEP 1 - validate the memory size
    if ((callee_addr < reinterpret_cast<uint64_t>(oob_mr_ptr)) || 
        ((callee_addr+size) > reinterpret_cast<uint64_t>(oob_mr_ptr) + oob_mr_size)) {
        std::cerr << "callee address:0x" << std::hex << callee_addr << " or size " << size << " is invalid." << std::dec << std::endl;
        return false;
    }
    // STEP 2 - do RDMA write to send the OOB data
    auto& subgroup_handle = group->template get_subgroup<OOBRDMA>(this->subgroup_index);
    struct iovec iov;
    iov.iov_base    = reinterpret_cast<void*>(callee_addr);
    iov.iov_len     = static_cast<size_t>(size);
    subgroup_handle.oob_remote_write(group->get_rpc_caller_id(),&iov,1,caller_addr,rkey,size);
    subgroup_handle.wait_for_oob_op(group->get_rpc_caller_id(),OOB_OP_WRITE,1000);
    return true;
}

uint64_t OOBRDMA::send(const uint64_t size) const {
    // STEP 1 - validate the memory size
    if (size > oob_mr_size) {
        std::cerr << "Cannot put " << size << " bytes of data, it's more than my memory region limit:" << oob_mr_size << std::endl;
        return 0ull;
    }
    // STEP 2 - get a random address
    uint64_t callee_addr = reinterpret_cast<uint64_t>(oob_mr_ptr) + (static_cast<uint64_t>(rand()%((oob_mr_size - size) >> 12))<<12);
    // STEP 3 - do RDMA recv to get the OOB data.
    auto& subgroup_handle = group->template get_subgroup<OOBRDMA>(this->subgroup_index);
    struct iovec iov;
    iov.iov_base    = reinterpret_cast<void*>(callee_addr);
    iov.iov_len     = static_cast<size_t>(size);
    subgroup_handle.oob_recv(group->get_rpc_caller_id(),&iov,1);
    subgroup_handle.wait_for_oob_op(group->get_rpc_caller_id(),OOB_OP_RECV,1000);
    return callee_addr;
}

bool OOBRDMA::recv(const uint64_t& callee_addr, const uint64_t size) const {
    // STEP 1 - validate the memory size
    if ((callee_addr < reinterpret_cast<uint64_t>(oob_mr_ptr)) ||
        ((callee_addr+size) > reinterpret_cast<uint64_t>(oob_mr_ptr) + oob_mr_size)) {
        std::cerr << "callee address:0x" << std::hex << callee_addr << " or size " << size << " is invalid." << std::dec << std::endl;
        return false;
    }
    // STEP 2 - do RDMA send
    auto& subgroup_handle = group->template get_subgroup<OOBRDMA>(this->subgroup_index);
    struct iovec iov;
    iov.iov_base    = reinterpret_cast<void*>(callee_addr); 
    iov.iov_len     = static_cast<size_t>(size);
    subgroup_handle.oob_send(group->get_rpc_caller_id(),&iov,1);
    subgroup_handle.wait_for_oob_op(group->get_rpc_caller_id(),OOB_OP_SEND,1000);
    return true;
}

static void print_data (void* addr,size_t size) {
    std::cout << "data@0x" << std::hex << reinterpret_cast<uint64_t>(addr) << " [ " << std::endl;

    for (uint32_t cidx=0;cidx<16;cidx++) {
        std::cout << reinterpret_cast<char*>(addr)[cidx] << " ";
    }
    std::cout << std::endl;
    std::cout << "..." << std::endl;
    for (uint32_t cidx=size - 16;cidx<size;cidx++) {
        std::cout << reinterpret_cast<char*>(addr)[cidx] << " ";
    }

    std::cout << std::dec << std::endl;
    std::cout << "]" << std::endl;
}

template <typename SubgroupRefT>
void do_send_recv_test(SubgroupRefT& subgroup_handle,
                       node_id_t nid,
                       void* send_buffer_laddr,
                       void* recv_buffer_laddr,
                       size_t oob_data_size
#ifdef CUDA_FOUND
                       , bool use_gpu_mem
#endif//CUDA_FOUND
                       ) {
    std::cout << "Implementation of OOBRDMA::send() and OOBRDMA::recv() does not work \n"
                 "with Infiniband verbs environment. This is an known issue with this \n"
                 "test case. We omit it for now. TODO: fix it. \n"
              << std::endl;
    /***************************************************************
    std::cout << "Testing node-" << nid << std::endl;

    // 1 - test send
#ifdef CUDA_FOUND
    if (use_gpu_mem) {
        char sendbuf[oob_data_size]={'B'};
        char recvbuf[oob_data_size]={'b'};
        ASSERTDRV(cuMemcpyHtoD(
            reinterpret_cast<CUdeviceptr>(send_buffer_laddr),
            sendbuf,oob_data_size));
        ASSERTDRV(cuMemcpyHtoD(
            reinterpret_cast<CUdeviceptr>(recv_buffer_laddr),
            recvbuf,oob_data_size));
        std::cout << "contents of the send buffer" << std::endl;
        std::cout << "===========================" << std::endl;
        print_data(sendbuf,oob_data_size);
        std::cout << "contents of the recv buffer" << std::endl;
        std::cout << "===========================" << std::endl;
        print_data(recvbuf,oob_data_size);
    } else {
#endif//CUDA_FOUND
        memset(send_buffer_laddr, 'B', oob_data_size);
        memset(recv_buffer_laddr, 'b', oob_data_size);
        std::cout << "contents of the send buffer" << std::endl;
        std::cout << "===========================" << std::endl;
        print_data(send_buffer_laddr,oob_data_size);
        std::cout << "contents of the recv buffer" << std::endl;
        std::cout << "===========================" << std::endl;
        print_data(recv_buffer_laddr,oob_data_size);
#ifdef CUDA_FOUND
    }
#endif//CUDA_FOUND
    // 2 - do send
    // 2.1 - post p2p_send
    auto send_results = subgroup_handle.template p2p_send<RPC_NAME(send)>(nid,oob_data_size);
    // 2.2 - send oob data
    struct iovec iov;
    uint64_t remote_addr;
    iov.iov_base = send_buffer_laddr;
    iov.iov_len = oob_data_size;
    subgroup_handle.oob_send(nid,&iov,1);
    // 2.3 - wait for oob send
    subgroup_handle.wait_for_oob_op(nid,OOB_OP_SEND,1000);
    // 2.4 - wait for p2p reply
    remote_addr = send_results.get().get(nid);
    std::cout << "Data sent to remote address @" << std::hex << remote_addr << std::dec << std::endl;

    // 3 - do recv
    iov.iov_base = recv_buffer_laddr;
    iov.iov_len = oob_data_size;
    // 3.1 - post oob buffer for receive
    subgroup_handle.oob_recv(nid,&iov,1);
    // 3.2 - post p2p_send 
    auto recv_results = subgroup_handle.template p2p_send<RPC_NAME(recv)>(nid,remote_addr,oob_data_size);
    // 3.3 - wait until oob received.
    subgroup_handle.wait_for_oob_op(nid,OOB_OP_RECV,1000);
    // 3.4 - wait for p2p reply
    bool recv_res = recv_results.get().get(nid);
    if (!recv_res) {
        std::cerr << "Data receive OOB operation failed." << std::endl;
        return;
    }
    std::cout << "Data received to local address @" << std::hex << recv_buffer_laddr << std::dec << std::endl;

    // 4 - show data:
#ifdef CUDA_FOUND
    if (use_gpu_mem) {
        char sendbuf[oob_data_size];
        char recvbuf[oob_data_size];
        ASSERTDRV(cuMemcpyDtoH(sendbuf,
            reinterpret_cast<CUdeviceptr>(send_buffer_laddr),
            oob_data_size));
        ASSERTDRV(cuMemcpyDtoH(recvbuf,
            reinterpret_cast<CUdeviceptr>(recv_buffer_laddr),
            oob_data_size));
        std::cout << "contents of the send buffer" << std::endl;
        std::cout << "===========================" << std::endl;
        print_data(sendbuf,oob_data_size);
        std::cout << "contents of the recv buffer" << std::endl;
        std::cout << "===========================" << std::endl;
        print_data(recvbuf,oob_data_size);
    } else {
#endif
        std::cout << "contents of the send buffer" << std::endl;
        std::cout << "===========================" << std::endl;
        print_data(send_buffer_laddr,oob_data_size);
        std::cout << "contents of the recv buffer" << std::endl;
        std::cout << "===========================" << std::endl;
        print_data(recv_buffer_laddr,oob_data_size);
#ifdef CUDA_FOUND
    }
#endif
    ***************************************************************/
}

template <typename P2PCaller>
void do_test (P2PCaller& p2p_caller, node_id_t nid, uint64_t rkey, void* put_buffer_laddr, void* get_buffer_laddr,
    size_t oob_data_size
#ifdef CUDA_FOUND
    , bool use_gpu_mem
#endif
    ) {
    std::cout << "Testing node-" << nid << std::endl;

    // 1 - test one-sided OOB
#ifdef CUDA_FOUND
    if (use_gpu_mem) {
        char putbuf[oob_data_size] = {'A'};
        char getbuf[oob_data_size] = {'a'};
        ASSERTDRV(cuMemcpyHtoD(
                    reinterpret_cast<CUdeviceptr>(put_buffer_laddr),
                    reinterpret_cast<const void*>(putbuf),
                    oob_data_size));
        ASSERTDRV(cuMemcpyHtoD(
                    reinterpret_cast<CUdeviceptr>(get_buffer_laddr),
                    reinterpret_cast<const void*>(getbuf),
                    oob_data_size));
        std::cout << "contents of the put buffer" << std::endl;
        std::cout << "==========================" << std::endl;
        print_data(putbuf,oob_data_size);
        std::cout << "contents of the get buffer" << std::endl;
        std::cout << "==========================" << std::endl;
        print_data(getbuf,oob_data_size);
    } else {
#endif
        memset(put_buffer_laddr, 'A', oob_data_size);
        memset(get_buffer_laddr, 'a', oob_data_size);
        std::cout << "contents of the put buffer" << std::endl;
        std::cout << "==========================" << std::endl;
        print_data(put_buffer_laddr,oob_data_size);
        std::cout << "contents of the get buffer" << std::endl;
        std::cout << "==========================" << std::endl;
        print_data(get_buffer_laddr,oob_data_size);
#ifdef CUDA_FOUND
    }
#endif
    // do put
    uint64_t remote_addr;
    {
        std::cout << "One-Side Put with oob to node-" << nid << std::endl;
        auto results = p2p_caller.template p2p_send<RPC_NAME(put)>(nid,reinterpret_cast<uint64_t>(put_buffer_laddr),rkey,oob_data_size);
        std::cout << "Wait for return" << std::endl;
        remote_addr = results.get().get(nid);
        std::cout << "Data put to remote address @" << std::hex << remote_addr << std::dec << std::endl;
    }
    // do get
    {
        std::cout << "One-Side Get with oob from node-" << nid << std::endl;
        auto results = p2p_caller.template p2p_send<RPC_NAME(get)>(nid,remote_addr,reinterpret_cast<uint64_t>(get_buffer_laddr),rkey,oob_data_size);
        std::cout << "Wait for return" << std::endl;
        results.get().get(nid);
        std::cout << "Data get from remote address @" << std::hex << remote_addr 
                  << " to local address @" << reinterpret_cast<uint64_t>(get_buffer_laddr) << std::endl;
    }
    // print 16 bytes of contents
#ifdef CUDA_FOUND
    if (use_gpu_mem) {
        char putbuf[oob_data_size];
        char getbuf[oob_data_size];
        ASSERTDRV(cuMemcpyDtoH(
                    reinterpret_cast<void*>(putbuf),
                    reinterpret_cast<CUdeviceptr>(put_buffer_laddr),
                    oob_data_size));
        ASSERTDRV(cuMemcpyDtoH(
                    reinterpret_cast<void*>(getbuf),
                    reinterpret_cast<CUdeviceptr>(get_buffer_laddr),
                    oob_data_size));
        std::cout << "contents of the put buffer" << std::endl;
        std::cout << "==========================" << std::endl;
        print_data(putbuf,oob_data_size);
        std::cout << "contents of the get buffer" << std::endl;
        std::cout << "==========================" << std::endl;
        print_data(getbuf,oob_data_size);
    } else {
#endif
        std::cout << "contents of the put buffer" << std::endl;
        std::cout << "==========================" << std::endl;
        print_data(put_buffer_laddr,oob_data_size);
        std::cout << "contents of the get buffer" << std::endl;
        std::cout << "==========================" << std::endl;
        print_data(get_buffer_laddr,oob_data_size);
#ifdef CUDA_FOUND
    }
#endif
}

const char* help_string = 
"--server,-s        Run as server, otherwise, run as client by default.\n"
#ifdef CUDA_FOUND
"--gpu,-g           Using GPU memory, otherwise, use CPU memory by default.\n"
"--device,-d <device number>\n"
"                   CUDA device number, used along with --gpu,-g. Defaulted to 0\n"
#endif
"--buffer,-b <registered buffer size in megabytes>.\n"
"                   Buffer size registered for OOB. Defaulted to 1.\n"
"--size,-S <data size in bytes>\n"
"                   The OOB transfer data size in bytes. Defaulted to 256.\n"
"--count,-c <transfer count>\n"
"                   The number of transfers. Defaulted to 3.\n"
"--help,-h          Print this message.\n"
;

__attribute__ ((visibility ("hidden"))) void print_help(int argc, char** argv) {
    std::cout << "USAGE:" << argv[0] << "  [options]\n" << help_string << std::endl;
}

/**
 * oob_rdma server [oob memory in MB=1] [data size in Byte=256] [count=3]
 * oob_rdma client [oob memory in MB=1] [data size in Byte=256] [count=3]
 */
int main(int argc, char** argv) {

    static struct option long_options[] = {
        {"server",  no_argument,        0,  's'},
#ifdef CUDA_FOUND
        {"gpu",     no_argument,        0,  'g'},
        {"device",  required_argument,  0,  'd'},
#endif
        {"buffer",  required_argument,  0,  'b'},
        {"size",    required_argument,  0,  'S'},
        {"count",   required_argument,  0,  'c'},
        {"help",    no_argument,        0,  'h'},
        {0,0,0,0}
    };

    int c;

    // argument collectors
    bool        server_mode     = false;
#ifdef CUDA_FOUND
    bool        use_gpu_mem     = false;
    int32_t     cuda_device     = 0;
#endif
    size_t      oob_mr_size     = 1ul << 20;
    size_t      oob_data_size   = 256;
    size_t      count           = 3;

    while (true) {
        int option_index = 0;
        c = getopt_long(argc,argv,"sgd:b:S:c:h",long_options,&option_index);
        if (c == -1) break;
        switch(c) {
        case 's':
            server_mode = true;
            break;
#ifdef CUDA_FOUND
        case 'g':
            use_gpu_mem = true;
            break;
        case 'd':
            cuda_device = std::stoi(optarg);
            break;
#endif
        case 'b':
            oob_mr_size = (std::stoul(optarg)<<20);
            break;
        case 'S':
            oob_data_size   = std::stoul(optarg);
            break;
        case 'c':
            count   = std::stoul(optarg);
            break;
        case 'h':
            print_help(argc,argv);
            return 0;
        case '?':
            break;
        default:
            std::cout << "Unknown return value from getopt_long:" << c << std::endl;
        }
    }

    // initialize GPU context
#ifdef CUDA_FOUND
    if (use_gpu_mem) {
        ASSERTDRV(cuInit(0));
        int n_devices = 0;
        ASSERTDRV(cuDeviceGetCount(&n_devices));
        if (!(n_devices > cuda_device)) {
            std::cerr << "Unknown CUDA device:" << cuda_device << ". We found only " << n_devices << std::endl;
            return -1;
        }
        // initialize cuda_ctxt; 
        ASSERTDRV(cuDeviceGet(&cuda_ctxt.device, cuda_device));
        ASSERTDRV(cuDevicePrimaryCtxRetain(&cuda_ctxt.context, cuda_ctxt.device));
        ASSERTDRV(cuCtxSetCurrent(cuda_ctxt.context));
    }
#endif

    // allocate memory
    void* oob_mr_ptr = nullptr;
#ifdef CUDA_FOUND
    if (use_gpu_mem) {
        ASSERTDRV(cuMemAlloc(reinterpret_cast<CUdeviceptr*>(&oob_mr_ptr),oob_mr_size));
    } else {
#endif
        oob_mr_ptr = aligned_alloc(4096,oob_mr_size);
        if (!oob_mr_ptr) {
            std::cerr << "Failed to allocate oob memory with aligned_alloc(). errno=" << errno << std::endl;
            return -1;
        }
#ifdef CUDA_FOUND
    }
#endif
    derecho::memory_attribute_t attr;
#ifdef CUDA_FOUND
    if (use_gpu_mem) {
        attr.type = derecho::memory_attribute_t::CUDA;
        attr.device.cuda = cuda_ctxt.device;
    } else {
#endif
        attr.type = derecho::memory_attribute_t::SYSTEM;
#ifdef CUDA_FOUND
    }
#endif

    // Deserialization Context
    OOBRDMADSM dsm;
    dsm.oob_mr_ptr = oob_mr_ptr;
    dsm.oob_mr_size = oob_mr_size;

    if (server_mode) {
        // Read configurations from the command line options as well as the default config file
        derecho::Conf::initialize(argc, argv);
    
        // Define subgroup member ship using the default subgroup allocator function.
        // When constructed using make_subgroup_allocator with no arguments, this will check the config file
        // for either the json_layout or json_layout_file options, and use whichever one is present to define
        // the mapping from types to subgroup allocation parameters.
        derecho::SubgroupInfo subgroup_function{derecho::make_subgroup_allocator<OOBRDMA>()};
    
        // oobrdma_factory
        auto oobrdma_factory = [&oob_mr_ptr,&oob_mr_size](persistent::PersistentRegistry*, derecho::subgroup_id_t) {
            return std::make_unique<OOBRDMA>(oob_mr_ptr,oob_mr_size);
        };
    
        // group
        derecho::Group<OOBRDMA> group(derecho::UserMessageCallbacks{}, subgroup_function, 
                                      {&dsm},
                                      std::vector<derecho::view_upcall_t>{}, oobrdma_factory);
    
        std::cout << "Finished constructing/joining Group." << std::endl;
        memset(oob_mr_ptr,'A',oob_mr_size);
        group.register_oob_memory_ex(oob_mr_ptr, oob_mr_size, attr);
        std::cout << oob_mr_size << " bytes of OOB Memory registered" << std::endl;

        std::cout << "Press Enter to shutdown gracefully." << std::endl;

        // test OOB put/get to a random peer
        if (group.get_my_rank() == 0) {
            uint64_t rkey           = group.get_oob_memory_key(oob_mr_ptr);
            void* put_buffer_laddr  = oob_mr_ptr;
            void* get_buffer_laddr  = reinterpret_cast<void*>(((reinterpret_cast<uint64_t>(oob_mr_ptr) + oob_data_size + 4095)>>12)<<12);

            for(const auto& member: group.get_members()) {
                if (member == group.get_my_id()) {
                    continue;
                }
                // TEST - one-sided OOB
                do_test(group.get_subgroup<OOBRDMA>(),member,rkey,put_buffer_laddr,get_buffer_laddr,oob_data_size
#ifdef CUDA_FOUND
                    ,use_gpu_mem
#endif
                );

                // TEST - two-sided OOB
                do_send_recv_test(group.get_subgroup<OOBRDMA>(),member,put_buffer_laddr,get_buffer_laddr,oob_data_size
#ifdef CUDA_FOUND
                    ,use_gpu_mem
#endif
                );
            }
        }

        // wait here
        std::cin.get();

        group.deregister_oob_memory(oob_mr_ptr);
        std::cout << oob_mr_size << " bytes of OOB Memory deregistered" << std::endl;
        group.barrier_sync();
        group.leave();
    } else {
        // create an external client
        derecho::ExternalGroupClient<OOBRDMA> external_group;
        derecho::ExternalClientCaller<OOBRDMA, decltype(external_group)>& external_caller = external_group.get_subgroup_caller<OOBRDMA>();
        std::cout << "External caller created." << std::endl;

        external_group.register_oob_memory_ex(oob_mr_ptr, oob_mr_size, attr);
        std::cout << oob_mr_size << " bytes of OOB Memory registered" << std::endl;

        {
            uint64_t rkey           = external_group.get_oob_memory_key(oob_mr_ptr);
            void* put_buffer_laddr  = oob_mr_ptr;
            void* get_buffer_laddr  = reinterpret_cast<void*>(((reinterpret_cast<uint64_t>(oob_mr_ptr) + oob_data_size + 4095)>>12)<<12);

            for (uint32_t i=1;i<=count;i++) {
                node_id_t nid = i%external_group.get_members().size();
                do_test(external_caller,nid,rkey,put_buffer_laddr,get_buffer_laddr,oob_data_size
#ifdef CUDA_FOUND
                    ,use_gpu_mem
#endif
                );
            }
        }

        external_group.deregister_oob_memory(oob_mr_ptr);
        std::cout << oob_mr_size << "bytes of OOB Memory unregistered" << std::endl;
    }

    // release memory
#ifdef CUDA_FOUND
    if (use_gpu_mem) {
        cuMemFree(reinterpret_cast<CUdeviceptr>(oob_mr_ptr));
    } else {
#endif
        free(oob_mr_ptr);
#ifdef CUDA_FOUND
    }

    // TODO: release gpu context.
    if (use_gpu_mem) {
        ASSERTDRV(cuDevicePrimaryCtxRelease(cuda_ctxt.device));
    }
#endif
    return 0;
}
