#include <iostream>
#include <map>
#include <memory>
#include <pthread.h>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <vector>

#include "block_size.h"
#include "derecho/derecho.h"
#include "initialize.h"
#include <mutils-serialization/SerializationSupport.hpp>
#include <mutils-serialization/context_ptr.hpp>
#include <persistent/Persistent.hpp>
#include <persistent/util.hpp>
#include <conf/conf.hpp>

using mutils::context_ptr;

//the payload is used to identify the user timestamp
typedef struct _payload {
    uint32_t node_rank;  // rank of the sender
    uint32_t msg_seqno;  // sequence of the message sent by the same sender
    uint64_t tv_sec;     // second
    uint64_t tv_nsec;    // nano second
} PayLoad;

//This class is modified from Matt's implementation
struct Bytes : public mutils::ByteRepresentable {
    char *bytes;
    std::size_t size;

    Bytes(const char *b, decltype(size) s)
            : size(s) {
        bytes = nullptr;
        if(s > 0) {
            bytes = new char[s];
            memcpy(bytes, b, s);
        }
    }
    Bytes(const Bytes &rhs)
            : Bytes(rhs.bytes, rhs.size) {
    }
    Bytes() {
        bytes = nullptr;
        size = 0;
    }
    virtual ~Bytes() {
        if(bytes != nullptr) {
            delete bytes;
        }
    }

    Bytes &operator=(Bytes &&other) {
        char *swp_bytes = other.bytes;
        std::size_t swp_size = other.size;
        other.bytes = bytes;
        other.size = size;
        bytes = swp_bytes;
        size = swp_size;
        return *this;
    }

    Bytes &operator=(const Bytes &other) {
        if(bytes != nullptr) {
            delete bytes;
        }
        size = other.size;
        if(size > 0) {
            bytes = new char[size];
            memcpy(bytes, other.bytes, size);
        } else {
            bytes = nullptr;
        }
        return *this;
    }

    std::size_t to_bytes(char *v) const {
        ((std::size_t *)(v))[0] = size;
        if(size > 0) {
            memcpy(v + sizeof(size), bytes, size);
        }
        return size + sizeof(size);
    }

    std::size_t bytes_size() const {
        return size + sizeof(size);
    }

    void post_object(const std::function<void(char const *const, std::size_t)> &f) const {
        f((char *)&size, sizeof(size));
        f(bytes, size);
    }

    void ensure_registered(mutils::DeserializationManager &) {}

    static std::unique_ptr<Bytes> from_bytes(mutils::DeserializationManager *, const char *const v) {
        return std::make_unique<Bytes>(v + sizeof(std::size_t), ((std::size_t *)(v))[0]);
    }

    static context_ptr<Bytes> from_bytes_noalloc(mutils::DeserializationManager *, const char *const v) {
        return context_ptr<Bytes>{new Bytes(v + sizeof(std::size_t), ((std::size_t *)(v))[0])};
    }

    static context_ptr<const Bytes> from_bytes_noalloc_const(mutils::DeserializationManager *, const char *const v) {
        return context_ptr<const Bytes>{new Bytes(v + sizeof(std::size_t), ((std::size_t *)(v))[0])};
    }
};

/**
 * Non-Persitent Object with vairable sizes
 */
class ByteArrayObject : public mutils::ByteRepresentable, public derecho::PersistsFields {
public:
    Persistent<Bytes> pers_bytes;
    //Persistent<Bytes,ST_MEM> vola_bytes;

    //void change_pers_bytes(const Bytes& bytes) {
    //  *pers_bytes = bytes;
    //}

    void change_pers_bytes(const Bytes &bytes) {
        *pers_bytes = bytes;
    }

    int query_const_int(uint64_t query_us) {
        return 100;
    }

    Bytes query_const_bytes(uint64_t query_us) {
        char bytes[1000000];
        Bytes b(bytes, 1000000);
        return b;
    }

    Bytes query_pers_bytes(uint64_t query_us) {
        HLC hlc{query_us, 0};
        try {
            return *pers_bytes.get(hlc);
        } catch(std::exception e) {
            std::cout << "query_pers_bytes failed:" << e.what() << std::endl;
        }
        return Bytes();
    }

    /** Named integers that will be used to tag the RPC methods */
    //  enum Functions { CHANGE_PERS_BYTES, CHANGE_VOLA_BYTES };
    enum Functions { CHANGE_PERS_BYTES,
                     QUERY_PERS_BYTES,
                     QUERY_CONST_INT,
                     QUERY_CONST_BYTES };

    static auto register_functions() {
        return std::make_tuple(
                //      derecho::rpc::tag<CHANGE_PERS_BYTES>(&ByteArrayObject::change_pers_bytes));
                derecho::rpc::tag<QUERY_PERS_BYTES>(&ByteArrayObject::query_pers_bytes),
                derecho::rpc::tag<QUERY_CONST_INT>(&ByteArrayObject::query_const_int),
                derecho::rpc::tag<QUERY_CONST_BYTES>(&ByteArrayObject::query_const_bytes),
                derecho::rpc::tag<CHANGE_PERS_BYTES>(&ByteArrayObject::change_pers_bytes));
    }

    //  DEFAULT_SERIALIZATION_SUPPORT(ByteArrayObject,pers_bytes,vola_bytes);
    DEFAULT_SERIALIZATION_SUPPORT(ByteArrayObject, pers_bytes);
    // constructor
    //  ByteArrayObject(Persistent<Bytes> & _p_bytes,Persistent<Bytes,ST_MEM> & _v_bytes):
    ByteArrayObject(Persistent<Bytes> &_p_bytes) :  //  ByteArrayObject(Persistent<Bytes> & _p_bytes):
                                                    //    pers_bytes(std::move(_p_bytes)) {
                                                   pers_bytes(std::move(_p_bytes)) {
    }
    // the default constructor
    ByteArrayObject(PersistentRegistry *pr) :  //    pers_bytes(nullptr,pr) {
                                              pers_bytes(nullptr, pr) {
    }
};

int main(int argc, char *argv[]) {
#ifndef NDEBUG
   spdlog::set_level(spdlog::level::trace);  
#endif
    if(argc != 7) {
        std::cout << "usage:" << argv[0] << "<shard_size> <num_of_shards> <ops_per_sec> <min_dur_sec> <msg_size> <query_cnt>" << std::endl;
        return -1;
    }
    int shard_size = atoi(argv[1]);
    int num_of_shards = atoi(argv[2]);
    // 1 for shards
    int num_of_nodes = (shard_size * num_of_shards + 1);
    int ops_per_sec = atoi(argv[3]);
    int min_dur_sec = atoi(argv[4]);
    int msg_size = atoi(argv[5]);
    int qcnt = atoi(argv[6]);
    uint64_t si_us = (1000000l / ops_per_sec);

    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;
    query_node_info(node_id, my_ip, leader_ip);
    long long unsigned int max_msg_size = msg_size;
    long long unsigned int block_size = get_block_size(msg_size);
    const long long unsigned int sst_max_msg_size = (max_msg_size < 17000 ? max_msg_size : 0);
    derecho::DerechoParams derecho_params{max_msg_size, sst_max_msg_size, block_size};

    derecho::CallbackSet callback_set{
            nullptr,  //we don't need the stability_callback here
            [&](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
            }};

    derecho::SubgroupInfo subgroup_info{
            {{std::type_index(typeid(ByteArrayObject)), [shard_size, num_of_shards, num_of_nodes](const derecho::View &curr_view, int &next_unassigned_rank) {
                  if(curr_view.num_members < num_of_nodes) {
                      std::cout << "not enough members yet:" << curr_view.num_members << " < " << num_of_nodes << std::endl;
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);

                  std::vector<uint32_t> members(shard_size);
                  for(int i = 0; i < num_of_shards; i++) {
                      for(int j = 0; j < shard_size; j++) {
                          members[j] = (uint32_t)i * shard_size + j;
                      }
                      subgroup_vector[0].emplace_back(curr_view.make_subview(members));
                  }

                  next_unassigned_rank = std::max(next_unassigned_rank, num_of_nodes - 1);
                  return subgroup_vector;
              }}},
            {std::type_index(typeid(ByteArrayObject))}};

    auto ba_factory = [](PersistentRegistry *pr) { return std::make_unique<ByteArrayObject>(pr); };

    std::unique_ptr<derecho::Group<ByteArrayObject>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<ByteArrayObject>>(
                node_id, my_ip, callback_set, subgroup_info, derecho_params,
                std::vector<derecho::view_upcall_t>{}, derecho::getConfInt32(CONF_DERECHO_GMS_PORT),
                ba_factory);
    } else {
        group = std::make_unique<derecho::Group<ByteArrayObject>>(
                node_id, my_ip, leader_ip, callback_set, subgroup_info,
                std::vector<derecho::view_upcall_t>{}, derecho::getConfInt32(CONF_DERECHO_GMS_PORT),
                ba_factory);
    }

    std::cout << "Finished constructing/joining Group" << std::endl;

    ///////////////////////////////////////////////////////////////////////////////
    // ordered send.
    if(node_id < (uint32_t)(num_of_nodes - 1)) {
        dbg_debug("begin to send message for {} seconds. Message size={}", min_dur_sec, msg_size);
        char *bbuf = new char[msg_size];
        bzero(bbuf, msg_size);
        Bytes bs(bbuf, msg_size);

        try {
            // start - start to send timestamp
            // last - timestamp of last send
            // cur - current timestamp
            struct timespec start, last, cur;
            int seqno = 0;
            clock_gettime(CLOCK_REALTIME, &start);
            cur = start;
            last = start;

#define DELTA_T_US(t1, t2) ((double)(((t2).tv_sec - (t1).tv_sec) * 1e6 + ((t2).tv_nsec - (t1).tv_nsec) * 1e-3))

            auto &handle = group->get_subgroup<ByteArrayObject>();
            while(DELTA_T_US(start, cur) / 1e6 < min_dur_sec) {
                do {
                    pthread_yield();
                    clock_gettime(CLOCK_REALTIME, &cur);
                } while(DELTA_T_US(last, cur) < (double)si_us);

                {
                    ((PayLoad *)bs.bytes)->node_rank = (uint32_t)node_id;
                    ((PayLoad *)bs.bytes)->msg_seqno = (uint32_t)seqno++;
                    ((PayLoad *)bs.bytes)->tv_sec = (uint64_t)cur.tv_sec;
                    ((PayLoad *)bs.bytes)->tv_nsec = (uint64_t)cur.tv_nsec;

                    handle.ordered_send<ByteArrayObject::CHANGE_PERS_BYTES>(bs);
                }
                last = cur;
            };

        } catch(uint64_t exp) {
            std::cout << "Exception caught:0x" << std::hex << exp << std::endl;
            return -1;
        }
    }
    ///////////////////////////////////////////////////////////////////////////////
    group->barrier_sync();
    usleep(min_dur_sec * 1e6);
    // query
    if(node_id == (uint32_t)(num_of_nodes - 1)) {
        struct timespec cur, tqs, tqm, tqm1, tqe;
        clock_gettime(CLOCK_REALTIME, &cur);
        uint64_t center_ts_us = (cur.tv_sec - min_dur_sec - min_dur_sec / 2) * 1e6;
        srandom((unsigned int)center_ts_us);
        while(qcnt--) {
            uint64_t query_ts_us = center_ts_us + random() % 2000000 - 1000000;
            clock_gettime(CLOCK_REALTIME, &tqs);
            auto shard_iterator = group->get_shard_iterator<ByteArrayObject>();
            // auto query_results_vec = shard_iterator.p2p_query<ByteArrayObject::QUERY_VOLA_BYTES>(query_ts_us);
            clock_gettime(CLOCK_REALTIME, &tqm1);
            auto query_results_vec = shard_iterator.p2p_query<ByteArrayObject::QUERY_PERS_BYTES>(query_ts_us);
            clock_gettime(CLOCK_REALTIME, &tqm);
            for(auto &query_result : query_results_vec) {
                auto &reply_map = query_result.get();
                PayLoad *pl = (PayLoad *)reply_map.begin()->second.get().bytes;
                volatile uint32_t seq = pl->msg_seqno;
                seq = seq;
                // volatile int x = reply_map.begin()->second.get();
                //dbg_trace("reply from shard {} received. message id = {}",cnt++,pl->msg_seqno);
            }
            clock_gettime(CLOCK_REALTIME, &tqe);
            dbg_trace("get all replies.");
            std::cout << "query " << DELTA_T_US(tqs, tqe) << " us send " << DELTA_T_US(tqs, tqm1) << " " << DELTA_T_US(tqm1, tqm) << " us" << std::endl;
        }
    }
    std::cout << std::flush;
    group->barrier_sync();
    exit(0);
}
