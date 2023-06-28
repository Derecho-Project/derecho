#include <iostream>
#include <map>
#include <memory>
#include <pthread.h>
#include <string>
#include <time.h>
#include <vector>

#include <derecho/core/derecho.hpp>

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

    REGISTER_RPC_FUNCTIONS(ByteArrayObject, ORDERED_TARGETS(change_vola_bytes));

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
                                              vola_bytes([](){return std::make_unique<Bytes>();}, nullptr, pr) {
    }
};

int main(int argc, char *argv[]) {

    if(argc < 5) {
        std::cout << "usage:" << argv[0] << " <all|half|one> <num_of_nodes> <count> <max_ops>" << std::endl;
        return -1;
    }
    derecho::Conf::initialize(argc,argv);

    int sender_selector = 0;  // 0 for all sender
    if(strcmp(argv[1], "half") == 0) sender_selector = 1;
    if(strcmp(argv[1], "one") == 0) sender_selector = 2;
    int num_of_nodes = std::stoi(argv[2]);
    int msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
    int count = std::stoi(argv[3]);
    int max_ops = std::stoi(argv[4]);
    uint64_t si_us = (1000000l / max_ops);

    bool is_sending = true;

    derecho::SubgroupInfo subgroup_info{[num_of_nodes, sender_selector](
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        if(curr_view.num_members < num_of_nodes) {
            std::cout << "not enough members yet:" << curr_view.num_members << " < " << num_of_nodes << std::endl;
            throw derecho::subgroup_provisioning_exception();
        }
        derecho::subgroup_shard_layout_t subgroup_layout(1);

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

        subgroup_layout[0].emplace_back(curr_view.make_subview(members, derecho::Mode::ORDERED, senders));
        curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, num_of_nodes);
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(ByteArrayObject)), std::move(subgroup_layout));
        return subgroup_allocation;
    }};

    auto ba_factory = [](PersistentRegistry *pr,derecho::subgroup_id_t) { return std::make_unique<ByteArrayObject>(pr); };

    derecho::Group<ByteArrayObject> group{{}, subgroup_info, {}, std::vector<derecho::view_upcall_t>{}, ba_factory};

    std::cout << "Finished constructing/joining Group" << std::endl;
    uint32_t node_rank = group.get_my_rank();

    if((sender_selector == 1) && (node_rank <= (uint32_t)(num_of_nodes - 1) / 2)) is_sending = false;
    if((sender_selector == 2) && (node_rank != (uint32_t)num_of_nodes - 1)) is_sending = false;

    std::cout << "my rank is:" << node_rank << ", and I'm sending:" << is_sending << std::endl;

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
    }
    dbg_default_debug("about to start the querying thread.");
#if defined(_PERFORMANCE_DEBUG)
    int num_datapoints = 0;  // number of data points
    pqt = std::make_unique<std::thread>([&]() {
        struct timespec tqt;
        HLC tqhlc;
        tqhlc.m_logic = 0;
        uint32_t node_rank_seen = 0xffffffff;
        uint32_t msg_seqno_seen = 0xffffffff;
        dbg_default_debug("query thread is running.");
        bool bQuit = false;
        while(!bQuit) {
            clock_gettime(CLOCK_REALTIME, &tqt);
            tqhlc.m_rtc_us = tqt.tv_sec * 1e6 + tqt.tv_nsec / 1e3;
            while(true) {
                try {
                    struct timespec at;
                    dbg_default_debug("query vola_bytes with tqhlc({},{})...", tqhlc.m_rtc_us, tqhlc.m_logic);
                    std::unique_ptr<Bytes> bs = (*handle.user_object_ptr)->vola_bytes.get(tqhlc);
                    dbg_default_debug("query returned from vola_bytes");
                    clock_gettime(CLOCK_REALTIME, &at);
                    PayLoad *pl = (PayLoad *)(bs->get());

                    if(node_rank_seen != pl->node_rank || msg_seqno_seen != pl->msg_seqno) {
                        query_time_us[num_datapoints] = tqhlc.m_rtc_us;
                        appear_time_us[num_datapoints] = (uint64_t)(at.tv_sec * 1e6 + at.tv_nsec / 1e3);
                        message_time_us[num_datapoints] = (uint64_t)(pl->tv_sec * 1e6 + pl->tv_nsec / 1e3);
                        num_datapoints++;
                        node_rank_seen = pl->node_rank;
                        msg_seqno_seen = pl->msg_seqno;
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
                } catch(persistent_version_not_stable& exp) {
                    // query time is too late
                    usleep(10);  // sleep for 10 microseconds.
                } catch(persistent_invalid_hlc& exp) {
                    usleep(10);
                    pthread_yield();
                    dbg_default_trace("give up query with hlc({},{}) because it is too early.", tqhlc.m_rtc_us, tqhlc.m_logic);
                    break;
                } catch(persistent_exception& exp) {
                     dbg_default_warn("unexpected exception({:x})", typeid(exp).name());
                }
            }
        }
        std::cout << std::flush;
        exit(0);
    });
#endif  //_PERFORMANCE_DEBUG
    dbg_default_debug("querying thread started.");

    if(is_sending) {
        uint8_t* bbuf = new uint8_t[msg_size];
        bzero(bbuf, msg_size);
        Bytes bs(bbuf, msg_size);

        try {
            struct timespec start, cur;
            clock_gettime(CLOCK_REALTIME, &start);

#define DELTA_T_US(t1, t2) ((double)(((t2).tv_sec - (t1).tv_sec) * 1e6 + ((t2).tv_nsec - (t1).tv_nsec) * 1e-3))

            for(int i = 0; i < count; i++) {
                do {
                    sched_yield();
                    clock_gettime(CLOCK_REALTIME, &cur);
                } while(DELTA_T_US(start, cur) < i * (double)si_us);
                {
                    ((PayLoad *)bs.get())->node_rank = (uint32_t)node_rank;
                    ((PayLoad *)bs.get())->msg_seqno = (uint32_t)i;
                    ((PayLoad *)bs.get())->tv_sec = (uint64_t)cur.tv_sec;
                    ((PayLoad *)bs.get())->tv_nsec = (uint64_t)cur.tv_nsec;

                    handle.ordered_send<RPC_NAME(change_vola_bytes)>(bs);
                }
            }
        } catch(std::exception& exp) {
            std::cout << "Exception caught: " << typeid(exp).name() << ": " << exp.what() << std::endl;
            return -1;
        }
    }

#if defined(_PERFORMANCE_DEBUG)
//      (*handle.user_object_ptr)->vola_bytes.print_performance_stat();
#endif  //_PERFORMANCE_DEBUG

    std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
