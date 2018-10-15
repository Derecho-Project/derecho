#include <iostream>
#include <map>
#include <memory>
#include <pthread.h>
#include <string>
#include <time.h>
#include <vector>

#include "block_size.h"
#include "bytes_object.h"
#include "derecho/derecho.h"
#include "initialize.h"
#include <mutils-serialization/SerializationSupport.hpp>
#include <persistent/Persistent.hpp>
#include <persistent/util.hpp>
#include "conf/conf.hpp"

using mutils::context_ptr;
using std::cout;
using std::endl;
using namespace persistent;
using derecho::Bytes;

//the payload is used to identify the user timestamp
typedef struct _payload {
    uint32_t node_rank;  // rank of the sender
    uint32_t msg_seqno;  // sequence of the message sent by the same sender
    uint64_t tv_sec;     // second
    uint64_t tv_nsec;    // nano second
} PayLoad;

/**
 * Non-Persitent Object with vairable sizes
 */
class ByteArrayObject : public mutils::ByteRepresentable, public derecho::PersistsFields {
public:
    //Persistent<Bytes> pers_bytes;
    Persistent<Bytes, ST_MEM> vola_bytes;

    //void change_pers_bytes(const Bytes& bytes) {
    //  *pers_bytes = bytes;
    //}

    void change_vola_bytes(const Bytes &bytes) {
        *vola_bytes = bytes;
    }

    REGISTER_RPC_FUNCTIONS(ByteArrayObject, change_vola_bytes);

    //  DEFAULT_SERIALIZATION_SUPPORT(ByteArrayObject,pers_bytes,vola_bytes);
    DEFAULT_SERIALIZATION_SUPPORT(ByteArrayObject, vola_bytes);
    // constructor
    //  ByteArrayObject(Persistent<Bytes> & _p_bytes,Persistent<Bytes,ST_MEM> & _v_bytes):
    ByteArrayObject(Persistent<Bytes, ST_MEM> &_v_bytes) :  //  ByteArrayObject(Persistent<Bytes> & _p_bytes):
                                                            //    pers_bytes(std::move(_p_bytes)) {
                                                           vola_bytes(std::move(_v_bytes)) {
    }
    // the default constructor
    ByteArrayObject(PersistentRegistry *pr) :  //    pers_bytes(nullptr,pr) {
                                              vola_bytes(nullptr, pr) {
    }
};

int main(int argc, char *argv[]) {

#ifndef NDEBUG
   spdlog::set_level(spdlog::level::trace);  
#endif
    if(argc < 6) {
        std::cout << "usage:" << argv[0] << " <all|half|one> <num_of_nodes> <msg_size> <count> <max_ops> [window_size=3]" << std::endl;
        return -1;
    }
    int sender_selector = 0;  // 0 for all sender
    if(strcmp(argv[1], "half") == 0) sender_selector = 1;
    if(strcmp(argv[1], "one") == 0) sender_selector = 2;
    int num_of_nodes = atoi(argv[2]);
    int msg_size = atoi(argv[3]);
    int count = atoi(argv[4]);
    int max_ops = atoi(argv[5]);
    uint64_t si_us = (1000000l / max_ops);
    // int qi_us = atoi(argv[6]);
    unsigned int window_size = 3;
    if(argc >= 7) {
        window_size = atoi(argv[6]);
    }

    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;
    query_node_info(node_id, my_ip, leader_ip);
    long long unsigned int max_msg_size = msg_size;
    long long unsigned int block_size = get_block_size(msg_size);
    const long long unsigned int sst_max_msg_size = (max_msg_size < 17000 ? max_msg_size : 0);
    derecho::DerechoParams derecho_params{max_msg_size, sst_max_msg_size, block_size, window_size};
    bool is_sending = true;

    derecho::CallbackSet callback_set{
            nullptr,  //we don't need the stability_callback here
            // the persistence_callback either
            [&](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
                // TODO: can be used to test the time from send to persistence.
            }};

    derecho::SubgroupInfo subgroup_info{
            {{std::type_index(typeid(ByteArrayObject)), [num_of_nodes, sender_selector](const derecho::View &curr_view, int &next_unassigned_rank, bool previous_was_successful) {
                  if(curr_view.num_members < num_of_nodes) {
                      std::cout << "not enough members yet:" << curr_view.num_members << " < " << num_of_nodes << std::endl;
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);

                  std::vector<uint32_t> members(num_of_nodes);
                  std::vector<int> senders(num_of_nodes, 1);
                  for(int i = 0; i < num_of_nodes; i++) {
                      members[i] = i;
                      switch(sender_selector) {
                          case 0:  // all senders
                              break;
                          case 1:  // half senders
                              if(i <= (num_of_nodes - 1) / 2) senders[i] = 0;
                              break;
                          case 2:  // one senders
                              if(i != (num_of_nodes - 1)) senders[i] = 0;
                              break;
                      }
                  }

                  subgroup_vector[0].emplace_back(curr_view.make_subview(members, derecho::Mode::ORDERED, senders));
                  next_unassigned_rank = std::max(next_unassigned_rank, num_of_nodes);
                  return subgroup_vector;
              }}},
            {std::type_index(typeid(ByteArrayObject))}};

    auto ba_factory = [](PersistentRegistry *pr) { return std::make_unique<ByteArrayObject>(pr); };

    std::unique_ptr<derecho::Group<ByteArrayObject>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<ByteArrayObject>>(
                node_id, my_ip, callback_set, subgroup_info, derecho_params,
                std::vector<derecho::view_upcall_t>{},
                ba_factory);
    } else {
        group = std::make_unique<derecho::Group<ByteArrayObject>>(
                node_id, my_ip, leader_ip, callback_set, subgroup_info,
                std::vector<derecho::view_upcall_t>{},
                ba_factory);
    }

    std::cout << "Finished constructing/joining Group" << std::endl;

    uint32_t node_rank = -1;
    auto members_order = group->get_members();
    cout << "The order of members is :" << endl;
    for(uint i = 0; i < (uint32_t)num_of_nodes; ++i) {
        cout << members_order[i] << " ";
        if(members_order[i] == node_id) {
            node_rank = i;
        }
    }
    cout << endl;
    if((sender_selector == 1) && (node_rank <= (uint32_t)(num_of_nodes - 1) / 2)) is_sending = false;
    if((sender_selector == 2) && (node_rank != (uint32_t)num_of_nodes - 1)) is_sending = false;

    std::cout << "my rank is:" << node_rank << ", and I'm sending:" << is_sending << std::endl;

    // spawn the query thread
    derecho::Replicated<ByteArrayObject> &handle = group->get_subgroup<ByteArrayObject>();
    std::unique_ptr<std::thread> pqt = nullptr;
    uint64_t *query_time_us;    // time used for temporal query
    uint64_t *appear_time_us;   // when the temporal query appears
    uint64_t *message_time_us;  // the latest timestamp
    query_time_us = (uint64_t *)malloc(sizeof(uint64_t) * num_of_nodes * count);
    appear_time_us = (uint64_t *)malloc(sizeof(uint64_t) * num_of_nodes * count);
    message_time_us = (uint64_t *)malloc(sizeof(uint64_t) * num_of_nodes * count);
    if(query_time_us == NULL || appear_time_us == NULL || message_time_us == NULL) {
        std::cerr << "allocate memory error!" << std::endl;
    }
    dbg_debug("about to start the querying thread.");
#if defined(_PERFORMANCE_DEBUG) || !defined(NDEBUG)
    int num_datapoints = 0;  // number of data points
    pqt = std::make_unique<std::thread>([&]() {
        struct timespec tqt;
        HLC tqhlc;
        tqhlc.m_logic = 0;
        uint32_t node_rank_seen = 0xffffffff;
        uint32_t msg_seqno_seen = 0xffffffff;
        dbg_debug("query thread is running.");
        bool bQuit = false;
        while(!bQuit) {
            clock_gettime(CLOCK_REALTIME, &tqt);
            tqhlc.m_rtc_us = tqt.tv_sec * 1e6 + tqt.tv_nsec / 1e3;
            while(true) {
                try {
                    struct timespec at;
                    dbg_debug("query vola_bytes with tqhlc({},{})...", tqhlc.m_rtc_us, tqhlc.m_logic);
                    std::unique_ptr<Bytes> bs = (*handle.user_object_ptr)->vola_bytes.get(tqhlc);
                    dbg_debug("query returned from vola_bytes");
                    clock_gettime(CLOCK_REALTIME, &at);
                    PayLoad *pl = (PayLoad *)(bs->bytes);

                    if(node_rank_seen != pl->node_rank || msg_seqno_seen != pl->msg_seqno) {
                        query_time_us[num_datapoints] = tqhlc.m_rtc_us;
                        appear_time_us[num_datapoints] = (uint64_t)(at.tv_sec * 1e6 + at.tv_nsec / 1e3);
                        message_time_us[num_datapoints] = (uint64_t)(pl->tv_sec * 1e6 + pl->tv_nsec / 1e3);
                        num_datapoints++;
                        node_rank_seen = pl->node_rank;
                        msg_seqno_seen = pl->msg_seqno;
                    }

                    dbg_trace("msg_seqno_seen={},count={},num_datapoints={}", msg_seqno_seen, count, num_datapoints);
                    if(msg_seqno_seen == (uint32_t)(count - 1)) {
                        std::cout << "query(us)\tappear(us)\tmessage(us)" << std::endl;
                        for(int i = 0; i < num_datapoints; i++) {
                            std::cout << query_time_us[i] << "\t" << appear_time_us[i] << "\t" << message_time_us[i] << std::endl;
                        }
                        dbg_trace("quitting...");
                        bQuit = true;
                    }
                    break;
                } catch(unsigned long long exp) {
                    dbg_debug("query thread return:{0:x}", exp);
                    // query time is too late or too early:
                    if(exp == PERSIST_EXP_BEYOND_GSF) {
                        usleep(10);  // sleep for 10 microseconds.
                    } else if(exp == PERSIST_EXP_INV_HLC) {
                        usleep(10);
                        pthread_yield();
                        dbg_trace("give up query with hlc({},{}) because it is too early.", tqhlc.m_rtc_us, tqhlc.m_logic);
                        break;
                    } else {
                        dbg_warn("unexpected exception({:x})", exp);
                    }
                }
            }
        }
        std::cout << std::flush;
        exit(0);
    });
#endif  //_PERFORMANCE_DEBUG || NDEBUG
    dbg_debug("querying thread started.");

    if(is_sending) {
        char *bbuf = new char[msg_size];
        bzero(bbuf, msg_size);
        Bytes bs(bbuf, msg_size);

        try {
            struct timespec start, cur;
            clock_gettime(CLOCK_REALTIME, &start);

#define DELTA_T_US(t1, t2) ((double)(((t2).tv_sec - (t1).tv_sec) * 1e6 + ((t2).tv_nsec - (t1).tv_nsec) * 1e-3))

            for(int i = 0; i < count; i++) {
                do {
                    pthread_yield();
                    clock_gettime(CLOCK_REALTIME, &cur);
                } while(DELTA_T_US(start, cur) < i * (double)si_us);
                {
                    ((PayLoad *)bs.bytes)->node_rank = (uint32_t)node_id;
                    ((PayLoad *)bs.bytes)->msg_seqno = (uint32_t)i;
                    ((PayLoad *)bs.bytes)->tv_sec = (uint64_t)cur.tv_sec;
                    ((PayLoad *)bs.bytes)->tv_nsec = (uint64_t)cur.tv_nsec;

                    handle.ordered_send<RPC_NAME(change_vola_bytes)>(bs);
                }
            }
        } catch(uint64_t exp) {
            std::cout << "Exception caught:0x" << std::hex << exp << std::endl;
            return -1;
        }
    }

#if defined(_PERFORMANCE_DEBUG) || !defined(NDEBUG)
//      (*handle.user_object_ptr)->vola_bytes.print_performance_stat();
#endif  //_PERFORMANCE_DEBUG

    std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
