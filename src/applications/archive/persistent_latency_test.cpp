#include <iostream>
#include <map>
#include <memory>
#include <pthread.h>
#include <string>
#include <time.h>
#include <vector>
#include <chrono>

#include "bytes_object.hpp"
#include <derecho/core/derecho.hpp>
#include <derecho/persistent/Persistent.hpp>

using std::endl;
using derecho::Bytes;
using namespace persistent;
using namespace std::chrono_literals;

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
    Persistent<Bytes, ST_SPDK> pers_bytes;

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
    ByteArrayObject(Persistent<Bytes, ST_SPDK> &_p_bytes) : pers_bytes(std::move(_p_bytes)) {
    }
    // the default constructor
    ByteArrayObject(PersistentRegistry *pr) : pers_bytes([](){return std::make_unique<Bytes>();}, nullptr, pr) {
    }
};

static inline uint64_t get_cur_us() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME,&ts);
    return static_cast<uint64_t>(ts.tv_sec*1e6 + ts.tv_nsec/1e3);
}

int main(int argc, char *argv[]) {

    if(argc < 4) {
        std::cout << "usage:" << argv[0] << " <all|half|one> <num_of_nodes> <count>" << std::endl;
        return -1;
    }
    int sender_selector = 0;  // 0 for all sender
    if(strcmp(argv[1], "half") == 0) sender_selector = 1;
    if(strcmp(argv[1], "one") == 0) sender_selector = 2;
    int num_of_nodes = atoi(argv[2]);
    uint32_t count = (uint32_t)atoi(argv[3]);
    uint64_t msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE) - 128;
    uint64_t window_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_WINDOW_SIZE);
    node_id_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    bool is_sending = true;
    uint32_t node_rank = -1;

    // 1 - On ordered_send, ts_before_ordered_send, and ts_after_ordered_send are filled.
    // 2 - On global stability callback, versions and ts_delivery are filled.
    // 3 - On local persist callback, ts_local_persist is filled, depending on versions.
    // 4 - On global persist callback, ts_global_persist is filled, depending versions.
    std::vector<persistent::version_t> versions(count,INVALID_VERSION); // dense
    std::vector<uint64_t> ts_before_ordered_send(count,0); // dense
    std::vector<uint64_t> ts_after_ordered_send(count,0); // dense
    std::vector<uint64_t> ts_delivery(count,0); // dense
    std::vector<uint64_t> ts_local_persist(count,0); // sparse - there may be hole
    std::vector<uint64_t> ts_global_persist(count,0); // sparse - there may be hole

    std::atomic<bool> done = false;

    derecho::CallbackSet callback_set{
            // 1 - global stability callback - delivery
            [&](derecho::subgroup_id_t sid, node_id_t nid, derecho::message_id_t msg_id, std::optional<std::pair<char*, long long int>> data,persistent::version_t ver) {
                static uint32_t filled_idx = 0; // start from 0;
                if (nid == my_id) {
                    versions[filled_idx] = ver;
                    ts_delivery[filled_idx] = get_cur_us();
                    filled_idx ++;
                }
            },
            [&](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
                static uint32_t filled_idx = 0; // ts_local_persist is already filled before filled_idx. Now we start from here.
                uint64_t now = get_cur_us();
                while( (filled_idx < count) && (versions[filled_idx] != INVALID_VERSION) && (versions[filled_idx] <= ver)) {
                    ts_local_persist[filled_idx++] = now;
                }
            },  // the persistence_callback either
            [&](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
                static uint32_t filled_idx = 0; // ts_global_persist is already filled before filled_idx. Now we start from here.
                uint64_t now = get_cur_us();
                while( (filled_idx < count) && (versions[filled_idx] != INVALID_VERSION) && (versions[filled_idx] <= ver)) {
                    ts_global_persist[filled_idx++] = now;
                }

                if (filled_idx >= count) {
                    done = true;
                }
            }
    };

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

    auto ba_factory = [](PersistentRegistry *pr) { return std::make_unique<ByteArrayObject>(pr); };

    derecho::Group<ByteArrayObject> group {
                callback_set, subgroup_info, nullptr,
                std::vector<derecho::view_upcall_t>{},
                ba_factory};

    std::cout << "Finished constructing/joining Group" << std::endl;
    node_rank = group.get_my_rank();

    if((sender_selector == 1) && (node_rank <= (uint32_t)(num_of_nodes - 1) / 2)) is_sending = false;
    if((sender_selector == 2) && (node_rank != (uint32_t)num_of_nodes - 1)) is_sending = false;

    std::cout << "my rank is:" << node_rank << ", and I'm sending:" << is_sending << std::endl;

    derecho::Replicated<ByteArrayObject> &handle = group.get_subgroup<ByteArrayObject>();

    if(is_sending) {
        std::vector<std::unique_ptr<Bytes>> buffers;
        for (uint32_t i=0;i<window_size+1;i++) {
            buffers.emplace_back(std::make_unique<Bytes>(static_cast<std::size_t>(msg_size)));
        }

        try {
            for(uint32_t i = 0; i < count; i++) {
                uint64_t msg_id = (static_cast<uint64_t>(node_rank)<<32) + i;
                int bidx = count%(window_size+1);
                // set msg_id
                *reinterpret_cast<uint64_t*>(buffers[bidx]->bytes) = msg_id;
                {
                    ts_before_ordered_send[i] = get_cur_us();
                    handle.ordered_send<ByteArrayObject::CHANGE_PERS_BYTES>(*buffers[bidx]);
                    ts_after_ordered_send[i] = get_cur_us();
                }
            }

        } catch(uint64_t exp) {
            std::cout << "Exception caught:0x" << std::hex << exp << std::endl;
            return -1;
        }
    }

    // TODO: use condition variable.
    std::cout << "wait until we done." << std::endl;
    while(!done) {
        std::this_thread::sleep_for(2s);
    }

    // print the data
    std::ofstream out_file("persistent_latency_data");
    out_file << "# message_size:\t" << msg_size << std::endl;
    out_file << "# num_of_nodes:\t" << num_of_nodes << std::endl;
    out_file << "# sender selector:\t" << argv[1] << std::endl;
    out_file << "#count\tordered_send(us)\tts_delivery(us)\tts_local_persist(us)\tts_global_persist(us)" << std::endl;
    double latency_sum = 0.0;
    for (uint32_t i=0;i<count;i++) {
        uint64_t start = ts_before_ordered_send[i];
        out_file << "[" << i << "]\t"
                  << ts_after_ordered_send[i] - start << " " 
                  << ts_delivery[i] - start << " " 
                  << ts_local_persist[i] - start << " " 
                  << ts_global_persist[i] - start << " " 
                  << std::endl;
        latency_sum += static_cast<double>(ts_global_persist[i] - start);
    }
    double average_latency = latency_sum/count;
    uint64_t timespan = ts_global_persist[count - 1] - ts_before_ordered_send[0];
    double msec = (double)timespan / 1000;
    double thp_gbps = ((double)count * msg_size * 8) / (timespan * 1000);
    double thp_ops = ((double)count * 1000000) / timespan; 

    double square_sum = 0.0;
    for (uint32_t i=0;i<count;i++) {
        square_sum += 
            ((static_cast<double>(ts_global_persist[i]-ts_before_ordered_send[i])-average_latency) *
             (static_cast<double>(ts_global_persist[i]-ts_before_ordered_send[i])-average_latency));
    }
    double latency_std = std::sqrt(square_sum/static_cast<double>(count+1));

    out_file << "# Average latency = " << latency_sum/count << " us" << std::endl;
    out_file << "# standard Deviation = " << latency_std << " us" << std::endl;
    out_file << "# Time span: " << msec << " millisecond" << std::endl;
    out_file << "# throughput: " << thp_gbps << " Gbit/s" << std::endl;
    out_file << "# throughput: " << thp_ops << " ops" << std::endl;
    out_file.close();

    group.barrier_sync();
    group.leave();
}
