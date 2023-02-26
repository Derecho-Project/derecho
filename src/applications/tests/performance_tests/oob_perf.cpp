/**
 * @file oob_rdma.cpp
 *
 * This test creates one subgroup demonstrating OOB mechanism 
 * - between external clients and a group member, and
 * - between derecho members
 */

#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>
#include <derecho/utils/time.h>

#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

#include "bytes_object.hpp"

using test::Bytes;

struct OOBRDMADSM : public mutils::RemoteDeserializationContext {
    void*   oob_mr_ptr;
    size_t  oob_mr_size;
    size_t  inband_data_size;
};

class OOBRDMA : public mutils::ByteRepresentable,
                public derecho::GroupReference {
private:
    uint32_t sudbgroup_index;
    void*   oob_mr_ptr;
    size_t  oob_mr_size;
    std::unique_ptr<Bytes>  bytes;
public:
    /**
     * put data (At @addr with @rkey of size @size)
     * @param caller_addr   the address on the caller side
     * @param rkey          the sender memory region's remote access key
     * @param size          the size of the data
     *
     * @return the address on the callee side where the data put.
     */
    uint64_t put(const uint64_t& caller_addr, const uint64_t rkey, const uint64_t size) const;

    /**
     * put data
     * @param bytes         the bytes to send
     *
     * @return 0
     */
    uint64_t inband_put(const Bytes& bytes) const;

    /** get data (At @addr with @rkey of size @size)
     * @param callee_addr   the address on the callee side
     * @param caller_addr   the address on the caller side
     * @param rkey          the sender memory region's remote access key
     * @param size          the size of the data
     *
     * @return true for success, otherwise false.
     */ 
    bool get(const uint64_t& callee_addr, const uint64_t& caller_addr, const uint64_t rkey, const uint64_t size) const;

    /**
     * get data
     * @return bytes.
     */
    Bytes inband_get() const;

    // constructors
    OOBRDMA(void* _oob_mr_ptr, size_t _oob_mr_size, size_t inband_data_size) : 
        oob_mr_ptr(_oob_mr_ptr),
        oob_mr_size(_oob_mr_size) {
        uint8_t *buffer = new uint8_t[inband_data_size];
        bytes = std::make_unique<Bytes>(buffer,inband_data_size);
        delete[] buffer;
    }

    // serialization supports
    virtual std::size_t to_bytes(uint8_t*) const override { return 0; }
    virtual void post_object(const std::function<void(uint8_t const* const, std::size_t)>&) const override {}
    virtual std::size_t bytes_size() const override { return 0; }
    static std::unique_ptr<OOBRDMA> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf) {
        if ( !dsm->registered<OOBRDMADSM>() ) {
            throw derecho::derecho_exception("OOBRDMA::from_bytes(): No OOBRDMADSM registered!");
        }
        return std::make_unique<OOBRDMA>(dsm->mgr<OOBRDMADSM>().oob_mr_ptr,
                                         dsm->mgr<OOBRDMADSM>().oob_mr_size,
                                         dsm->mgr<OOBRDMADSM>().inband_data_size);
    }
    static mutils::context_ptr<OOBRDMA> from_bytes_noalloc(mutils::DeserializationManager* dsm, uint8_t const* buf) {
        return mutils::context_ptr<OOBRDMA>(from_bytes(dsm, buf).release());
    }

    void ensure_registered(mutils::DeserializationManager&) {}

    REGISTER_RPC_FUNCTIONS(OOBRDMA,P2P_TARGETS(put,inband_put,get,inband_get))
};

uint64_t OOBRDMA::put(const uint64_t& caller_addr, const uint64_t rkey, const uint64_t size) const {
    // STEP 1 - validate the memory size
    if (size > oob_mr_size) {
        std::cerr << "Cannot put " << size << " bytes of data, it's more than my memory region limit:" << oob_mr_size << std::endl;
        return 0ull;
    }
    // STEP 2 - get a random address
    uint64_t callee_addr = reinterpret_cast<uint64_t>(oob_mr_ptr);
    // STEP 3 - do RDMA read to get the OOB data.
    auto& subgroup_handle = group->template get_subgroup<OOBRDMA>(this->subgroup_index);
    struct iovec iov;
    iov.iov_base    = reinterpret_cast<void*>(callee_addr);
    iov.iov_len     = static_cast<size_t>(size);

    subgroup_handle.oob_remote_read(group->get_rpc_caller_id(),&iov,1,caller_addr,rkey,size);

    return callee_addr;
}

uint64_t OOBRDMA::inband_put(const Bytes &bytes) const {
    //do nothing
    return 0;
}

Bytes OOBRDMA::inband_get() const {
    return *this->bytes;
}


bool OOBRDMA::get(const uint64_t& callee_addr, const uint64_t& caller_addr, const uint64_t rkey, const uint64_t size) const {
    // STEP 1 - validate the memory size
    if( (callee_addr < reinterpret_cast<uint64_t>(oob_mr_ptr)) || 
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
    return true;
}

template <typename P2PCaller>
void perf_test (
        P2PCaller& p2p_caller,
        node_id_t nid, 
        uint64_t rkey, 
        void* put_buffer_laddr, 
        void* get_buffer_laddr, 
        size_t oob_data_size, 
        size_t duration_sec,
        size_t nround = 1,
        bool   inband = false) {
    std::cout << "put_buffer_addr=" << put_buffer_laddr << std::endl;
    std::cout << "get_buffer_addr=" << get_buffer_laddr << std::endl;
    memset(put_buffer_laddr, 'A', oob_data_size);
    memset(get_buffer_laddr, 'a', oob_data_size);
#define MAX_OPS     256000
    uint64_t* ts_log = new uint64_t[MAX_OPS*duration_sec];
    const std::size_t rpc_header_size = sizeof(std::size_t) + sizeof(std::size_t) +
                                        derecho::remote_invocation_utilities::header_space();
    const uint64_t max_rep_size = derecho::getConfUInt64(CONF_DERECHO_MAX_P2P_REPLY_PAYLOAD_SIZE) - rpc_header_size;
    const uint64_t max_req_size   = derecho::getConfUInt64(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE) - rpc_header_size;
    if (max_rep_size < oob_data_size) {
        throw derecho::derecho_exception("max_reply_size (" + std::to_string(max_rep_size) + ") is smaller than data size(" + std::to_string(oob_data_size));
    }
    if (max_req_size < oob_data_size) {
        throw derecho::derecho_exception("max_request_size (" + std::to_string(max_req_size) + ") is smaller than data size(" + std::to_string(oob_data_size));
    }
    uint8_t* buf = new uint8_t[oob_data_size];
    Bytes bytes(buf,oob_data_size);
    delete[] buf;

    // STEP 1 Warmup: put 10 times
    uint64_t remote_addr;
    uint64_t cnt;
    for (cnt=0;cnt<10;cnt++) {
        if (inband) {
            auto results = p2p_caller.template p2p_send<RPC_NAME(inband_put)>(nid,bytes);
            results.get().get(nid);
        } else {
            auto results = p2p_caller.template p2p_send<RPC_NAME(put)>(nid,reinterpret_cast<uint64_t>(put_buffer_laddr),rkey,oob_data_size);
            remote_addr = results.get().get(nid);
        }
    }

    // STEP 2 Put performance
    uint64_t start_ns = get_time();
    uint64_t end_ns = start_ns + (duration_sec * 1e9);
    uint64_t cur_ns = start_ns;
    uint64_t num_put = 0;
    uint64_t num_get = 0;
    ts_log[0] = cur_ns;
    while(end_ns > cur_ns) {
        if (inband) {
            auto results = p2p_caller.template p2p_send<RPC_NAME(inband_put)>(nid,bytes);
            results.get().get(nid);
        } else {
            auto results = p2p_caller.template p2p_send<RPC_NAME(put)>(nid,reinterpret_cast<uint64_t>(put_buffer_laddr),rkey,oob_data_size);
            remote_addr = results.get().get(nid);
        }
        num_put ++;
        cur_ns = get_time();
        ts_log[num_put] = cur_ns;
    }
    std::ofstream put_log(std::to_string(oob_data_size) + "." + std::to_string(nround) + "_put.dat");
    for(uint64_t i=0;i<num_put;i++) {
        put_log << ts_log[i] << "\t" << ts_log[i+1] << std::endl;
    }
    put_log.close();

    // STEP 3 Get performance
    start_ns = get_time();
    cur_ns = start_ns;
    end_ns = start_ns + (duration_sec * 1e9);
    ts_log[0] = cur_ns;
    while(end_ns > cur_ns) {
        if (inband) {
            auto results = p2p_caller.template p2p_send<RPC_NAME(inband_get)>(nid);
            results.get().get(nid);
        } else {
            auto results = p2p_caller.template p2p_send<RPC_NAME(get)>(nid,remote_addr,reinterpret_cast<uint64_t>(get_buffer_laddr),rkey,oob_data_size);
            results.get().get(nid);
        }
        num_get ++;
        cur_ns = get_time();
        ts_log[num_get] = cur_ns;
    }
    std::ofstream get_log(std::to_string(oob_data_size) + "." + std::to_string(nround) + "_get.dat");
    for(uint64_t i=0;i<num_get;i++) {
        get_log << ts_log[i] << "\t" << ts_log[i+1] << std::endl;
    }
    get_log.close();
}

void print_help() {
    std::cout << "oob_perf [options]\n"
                 "--datasize=<the data size, default to 256>, -d\n"
                 "     The oob data size in bytes\n"
                 "--duration=<exp in sec, default to 5>, -D\n"
                 "     The duration of the experiment\n"
                 "--hugepage=<page size is MB>, -H\n"
                 "     using huge page OOB of the given size in MBytes. It could be 2 (MB) or 1024 (MB).\n"
                 "--count=<number of rounds, default to 1>, -c\n"
                 "     set number of rounds.\n"
                 "--inband\n"
                 "     use inband mode instead. By default, we use out-of-band(oob) mode.\n"
                 "--help, -h\n"
                 "     print this information"
              << std::endl;
}

int main(int argc, char** argv) {

    // STEP 1 - parse the argument
    static struct option perf_options[] = {
        {"datasize",    required_argument,  0,  'd'},
        {"duration",    required_argument,  0,  'D'},
        {"hugepage",    required_argument,  0,  'H'},
        {"count",       required_argument,  0,  'c'},
        {"inband",      no_argument,        0,  'i'},
        {"help",        no_argument,        0,  'h'},
        {0,0,0,0}
    };

    size_t oob_data_size = 256;
    size_t duration_sec  = 5;
    size_t hugepage_size = 0;
    size_t count         = 1;
    bool   inband        = false;
    while(true) {
        int c,option_index=0;
        c = getopt_long(argc,argv,"d:D:H:c:ih",perf_options,&option_index);
        if (c == -1) {
            break;
        }

        switch(c) {
            case 'd':
                oob_data_size = std::stol(optarg);
                break;
            case 'D':
                duration_sec  = std::stol(optarg);
            case 'H':
                hugepage_size = (std::stol(optarg) << 20);
                break;
            case 'c':
                count = std::stol(optarg);
                break;
            case 'i':
                inband = true;
                break;
            case 'h':
                print_help();
                return 0;
            case '?':
            default:
                std::cerr << "Unknown argument:" << argv[optind-1] << std::endl;
                return 1;
        }
    }

    size_t page_size            = static_cast<size_t>(getpagesize());
    size_t oob_mr_size          = ((2*oob_data_size + page_size - 1)/page_size)*page_size;

    std::cout << "Running OOB performance test with the following settings:\n"
              << "\tdatasize    = " << oob_data_size << " Bytes\n"
              << "\tmr_size     = " << oob_mr_size   << " Bytes\n"
              << "\tduration    = " << duration_sec << " Seconds\n";
    if (hugepage_size > 0) {
    std::cout << "\thugepage    = " << (hugepage_size >> 20) << " MBytes\n";

    std::cerr << "Hugepage support is to be implemented yet." << std::endl;
    return 1;
    } else {
    std::cout << "\thugepage    = N/A\n";
    }
    std::cout << "\tcount       = " << count << "\n"
              << "\tinband      = " << inband << "\n"
              << std::endl;


    // allocate memory
    void* oob_mr_ptr = mmap(nullptr,oob_mr_size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,0,0);

    if (oob_mr_ptr == MAP_FAILED) {
        std::cerr << "Failed to allocate oob memory region of " << oob_mr_size << " bytes" << std::endl;
        std::cerr << strerror(errno) << std::endl;
        return 1;
    }

    // Deserialization Context
    OOBRDMADSM dsm;
    dsm.oob_mr_ptr = oob_mr_ptr;
    dsm.oob_mr_size = oob_mr_size;
    dsm.inband_data_size = oob_data_size;

    {
        // Read configurations from the command line options as well as the default config file
        derecho::Conf::initialize(argc, argv);
    
        // Define subgroup member ship using the default subgroup allocator function.
        // When constructed using make_subgroup_allocator with no arguments, this will check the config file
        // for either the json_layout or json_layout_file options, and use whichever one is present to define
        // the mapping from types to subgroup allocation parameters.
        derecho::SubgroupInfo subgroup_function{derecho::make_subgroup_allocator<OOBRDMA>()};
    
        // oobrdma_factory
        auto oobrdma_factory = [&oob_mr_ptr,&oob_mr_size,&oob_data_size](persistent::PersistentRegistry*, derecho::subgroup_id_t) {
            return std::make_unique<OOBRDMA>(oob_mr_ptr,oob_mr_size,oob_data_size);
        };
    
        // group
        derecho::Group<OOBRDMA> group(derecho::UserMessageCallbacks{}, subgroup_function, 
                                      {&dsm},
                                      std::vector<derecho::view_upcall_t>{}, oobrdma_factory);
    
        std::cout << "Finished constructing/joining Group." << std::endl;
        memset(oob_mr_ptr,'A',oob_mr_size);
        group.register_oob_memory(oob_mr_ptr, oob_mr_size);
        std::cout << oob_mr_size << "bytes of OOB Memory registered" << std::endl;

        // test OOB put/get to a random peer
        if (group.get_my_rank() == 0) {
            uint64_t rkey           = group.get_oob_memory_key(oob_mr_ptr);
            void* put_buffer_laddr  = oob_mr_ptr;
            void* get_buffer_laddr  = reinterpret_cast<void*>(reinterpret_cast<uint64_t>(oob_mr_ptr) + oob_mr_size - oob_data_size);

            for(const auto& member: group.get_members()) {
                if (member == group.get_my_id()) {
                    continue;
                }
                // TEST
                for (size_t nr=1;nr<=count;nr++) {
                    perf_test(group.get_subgroup<OOBRDMA>(),member,rkey,put_buffer_laddr,get_buffer_laddr,oob_data_size,duration_sec,nr);
                }
            }
        }

        // wait here
        group.barrier_sync();
        group.unregister_oob_memory(oob_mr_ptr);
        std::cout << oob_mr_size << " bytes of OOB Memory unregistered" << std::endl;
        group.barrier_sync();
        group.leave();
    }

    if(munmap(oob_mr_ptr,oob_mr_size) != 0) {
        std::cout << "Failed to release oob memory:" << strerror(errno) << std::endl;
    }

    return 0;
}
