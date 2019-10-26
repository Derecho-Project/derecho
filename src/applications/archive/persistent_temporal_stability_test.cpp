#include <iostream>
#include <map>
#include <memory>
#include <pthread.h>
#include <string>
#include <time.h>
#include <vector>

#include "bytes_object.hpp"
#include <derecho/core/derecho.hpp>

using std::cout;
using std::endl;
using derecho::Bytes;
using namespace persistent;

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
    Persistent<Bytes> pers_bytes;

    void change_pers_bytes(const Bytes &bytes) {
        *pers_bytes = bytes;
    }

    /** Named integers that will be used to tag the RPC methods */
    //  enum Functions { CHANGE_PERS_BYTES, CHANGE_VOLA_BYTES };
    enum Functions { CHANGE_PERS_BYTES };

    static auto register_functions() {
        return std::make_tuple(
                derecho::rpc::tag<CHANGE_PERS_BYTES>(&ByteArrayObject::change_pers_bytes));
    }

    DEFAULT_SERIALIZATION_SUPPORT(ByteArrayObject, pers_bytes);
    // constructor
    ByteArrayObject(Persistent<Bytes> &_p_bytes) : pers_bytes(std::move(_p_bytes)) {
    }
    // the default constructor
    ByteArrayObject(PersistentRegistry *pr) : pers_bytes([](){return std::make_unique<Bytes>();}, nullptr, pr) {
    }
};

int main(int argc, char *argv[]) {
    if(argc < 5) {
        std::cout << "usage:" << argv[0] << " <all|half|one> <num_of_nodes> <count> <max_ops> "<< std::endl;
        return -1;
    }
    derecho::Conf::initialize(argc,argv);
    int sender_selector = 0;  // 0 for all sender
    if(strcmp(argv[1], "half") == 0) sender_selector = 1;
    if(strcmp(argv[1], "one") == 0) sender_selector = 2;
    int num_of_nodes = std::stoi(argv[2]);
    int count = atoi(argv[3]);
    int max_ops = atoi(argv[4]);
    uint64_t si_us = (1000000l / max_ops);
    int msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);

    bool is_sending = true;

    derecho::SubgroupInfo subgroup_info{[num_of_nodes, sender_selector](
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
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
                  curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, num_of_nodes);
                  //Since we know there is only one subgroup type, just put a single entry in the map
                  derecho::subgroup_allocation_map_t subgroup_allocation;
                  subgroup_allocation.emplace(std::type_index(typeid(ByteArrayObject)),
                                              std::move(subgroup_vector));
                  return subgroup_allocation;
    }};

    auto ba_factory = [](PersistentRegistry *pr, derecho::subgroup_id_t) { return std::make_unique<ByteArrayObject>(pr); };

    derecho::Group<ByteArrayObject> group{{}, subgroup_info, nullptr, std::vector<derecho::view_upcall_t>{}, ba_factory};

    std::cout << "Finished constructing/joining Group" << std::endl;
    uint32_t node_rank = group.get_my_rank();

    if((sender_selector == 1) && (node_rank <= (uint32_t)(num_of_nodes - 1) / 2)) is_sending = false;
    if((sender_selector == 2) && (node_rank != (uint32_t)num_of_nodes - 1)) is_sending = false;

    std::cout << "my rank is:" << node_rank << ", and I'm sending:" << is_sending << std::endl;

    // Local Message Timestamp Array
    uint64_t *local_message_time_us = (uint64_t *)malloc(sizeof(uint64_t) * count);
    if(local_message_time_us == NULL) {
        std::cerr << "allocate memory error!" << std::endl;
        return -1;
    }

    // spawn the query thread
    derecho::Replicated<ByteArrayObject> &handle = group.get_subgroup<ByteArrayObject>();
    std::unique_ptr<std::thread> pqt = nullptr;
    uint64_t *query_time_us;    // time used for temporal query
    uint64_t *appear_time_us;   // when the temporal query appears
    uint64_t *message_time_us;  // the latest timestamp
    query_time_us = (uint64_t *)malloc(sizeof(uint64_t) * num_of_nodes * count);
    appear_time_us = (uint64_t *)malloc(sizeof(uint64_t) * num_of_nodes * count);
    message_time_us = (uint64_t *)malloc(sizeof(uint64_t) * num_of_nodes * count);
    if(query_time_us == NULL || appear_time_us == NULL || message_time_us == NULL) {
        std::cerr << "allocate memory error!" << std::endl;
        return -1;
    }
    dbg_default_debug("about to start the querying thread.");
#if defined(_PERFORMANCE_DEBUG) || !defined(NDEBUG)
    int num_datapoints = 0;     // number of data points
    pqt = std::make_unique<std::thread>([&]() {
        struct timespec tqt;
        HLC tqhlc;
        tqhlc.m_logic = 0;
        uint32_t msg_seqno_seen = 0xffffffff;
        uint32_t node_rank_seen = 0xffffffff;
        dbg_default_debug("query thread is running.");
        bool bQuit = false;
        while(!bQuit) {
            clock_gettime(CLOCK_REALTIME, &tqt);
            tqhlc.m_rtc_us = tqt.tv_sec * 1e6 + tqt.tv_nsec / 1e3;
            while(true) {
                try {
                    struct timespec at;
                    std::unique_ptr<Bytes> bs = (*handle.user_object_ptr)->pers_bytes.get(tqhlc);
                    clock_gettime(CLOCK_REALTIME, &at);
                    PayLoad *pl = (PayLoad *)(bs->bytes);

                    // uint32_t next_seqno = (pl->node_rank < node_rank)?(pl->msg_seqno-1):pl->msg_seqno; // roundrobin
                    // if(msg_seqno_seen != next_seqno){
                    if(node_rank_seen != pl->node_rank || msg_seqno_seen != pl->msg_seqno) {
                        query_time_us[num_datapoints] = tqhlc.m_rtc_us;
                        appear_time_us[num_datapoints] = (uint64_t)(at.tv_sec * 1e6 + at.tv_nsec / 1e3);
                        // message_time_us[num_datapoints] = local_message_time_us[next_seqno];
                        message_time_us[num_datapoints] = (uint64_t)(pl->tv_sec * 1e6 + pl->tv_nsec / 1e3);
                        num_datapoints++;
                        msg_seqno_seen = pl->msg_seqno;
                        node_rank_seen = pl->node_rank;
                    }

                    dbg_default_trace("msg_seqno_seen={},count={},num_datapoints={}", msg_seqno_seen, count, num_datapoints);
                    if(msg_seqno_seen == (uint32_t)(count - 1)) {
                        std::cout << "query(us)\tappear(us)\tmessage(us)" << std::endl;
                        for(int i = 0; i < num_datapoints; i++) {
                            std::cout << query_time_us[i] << "\t" << appear_time_us[i] << "\t" << message_time_us[i] << std::endl;
                        }
                        dbg_default_trace("quitting...");
                        bQuit = true;
                    }
                    break;
                } catch(unsigned long long exp) {
                    dbg_default_debug("query thread return:{0:x}", exp);
                    // query time is too late or too early:
                    if(exp == PERSIST_EXP_BEYOND_GSF) {
                        usleep(10);  // sleep for 10 microseconds.
                    } else if(exp == PERSIST_EXP_INV_HLC) {
                        usleep(10);
                        pthread_yield();
                        dbg_default_trace("give up query with hlc({},{}) because it is too early.", tqhlc.m_rtc_us, tqhlc.m_logic);
                        break;
                    } else {
                        dbg_default_warn("unexpected exception({:x})", exp);
                    }
                }
            }
        }
        std::cout << std::flush;
        exit(0);
    });
#endif  //_PERFORMANCE_DEBUG || _DEBUG
    dbg_default_debug("querying thread started.");

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
                    ((PayLoad *)bs.bytes)->node_rank = (uint32_t)node_rank;
                    ((PayLoad *)bs.bytes)->msg_seqno = (uint32_t)i;
                    ((PayLoad *)bs.bytes)->tv_sec = (uint64_t)cur.tv_sec;
                    ((PayLoad *)bs.bytes)->tv_nsec = (uint64_t)cur.tv_nsec;
                    local_message_time_us[i] = ((cur.tv_sec) * 1e6 + cur.tv_nsec / 1e3);
                    handle.ordered_send<ByteArrayObject::CHANGE_PERS_BYTES>(bs);
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
