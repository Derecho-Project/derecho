#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <time.h>
#include <vector>

#include "block_size.h"
#include "derecho/derecho.h"
#include "initialize.h"
#include <mutils-serialization/SerializationSupport.hpp>
#include <mutils-serialization/context_ptr.hpp>
#include <persistent/Persistent.hpp>

using mutils::context_ptr;

//This class is modified from Matt's implementation
struct Bytes : public mutils::ByteRepresentable{

        char *bytes;
        const std::size_t size;

        Bytes(const char * b, decltype(size) s)
                :size(s){
            bytes = nullptr;
            if(s>0) {
                bytes = new char[s];
                memcpy(bytes,b,s);
            }
        }
        virtual ~Bytes(){
            if(bytes!=nullptr) {
                delete bytes;
            }
        }

        std::size_t to_bytes(char* v) const{
                ((std::size_t*)(v))[0] = size;
                memcpy(v + sizeof(size),bytes,size);
                return size + sizeof(size);
        }

        std::size_t bytes_size() const {
                return size + sizeof(size);
        }

        void post_object(const std::function<void (char const * const,std::size_t)>& f) const{
                f((char*)&size,sizeof(size));
                f(bytes,size);
        }

        void ensure_registered(mutils::DeserializationManager&){}

        static std::unique_ptr<Bytes> from_bytes(mutils::DeserializationManager *, const  char * const v){
            return std::make_unique<Bytes>(v + sizeof(std::size_t),((std::size_t*)(v))[0]);
        }

        static context_ptr<Bytes> from_bytes_noalloc(mutils::DeserializationManager *, const char * const v)  {
                return context_ptr<Bytes>{new Bytes(v + sizeof(std::size_t),((std::size_t*)(v))[0])};
        }

};


/**
 * RPC Object with a single function that accepts a string
 */
class TestObject {
public:
    void fun(const std::string& words) {
    }

    void bytes_fun(const Bytes& bytes) {
    }

    /** Named integers that will be used to tag the RPC methods */
    enum Functions { FUN, BYTES_FUN };

    static auto register_functions() {
        return std::make_tuple(derecho::rpc::tag<FUN>(&TestObject::fun),
            derecho::rpc::tag<BYTES_FUN>(&TestObject::bytes_fun));
    }
};

int main(int argc, char* argv[]) {
    if(argc != 4) {
        std::cout << "usage:" << argv[0] << " <num_of_nodes> <max_msg_size> <count>" << std::endl;
        return -1;
    }
    int num_of_nodes = atoi(argv[1]);
    long long unsigned int max_msg_size = atoi(argv[2]);
    int count = atoi(argv[3]);

    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;
    query_node_info(node_id, my_ip, leader_ip);
    long long unsigned int block_size = get_block_size(max_msg_size);
    derecho::DerechoParams derecho_params{max_msg_size, block_size};

    derecho::CallbackSet callback_set{
            nullptr,  //we don't need the stability_callback here
            nullptr   //we don't need the persistence_callback either
    };

    derecho::SubgroupInfo subgroup_info{
            {{std::type_index(typeid(TestObject)), [num_of_nodes](const derecho::View& curr_view, int& next_unassigned_rank, bool previous_was_successful) {
                  if(curr_view.num_members < num_of_nodes) {
                      std::cout << "not enough members yet:" << curr_view.num_members << " < " << num_of_nodes << std::endl;
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);

                  std::vector<uint32_t> members(num_of_nodes);
                  for(int i = 0; i < num_of_nodes; i++) {
                      members[i] = i;
                  }

                  subgroup_vector[0].emplace_back(curr_view.make_subview(members));
                  next_unassigned_rank = std::max(next_unassigned_rank, num_of_nodes);
                  return subgroup_vector;
              }}},
            {std::type_index(typeid(TestObject))}};

    auto ba_factory = [](PersistentRegistry*) { return std::make_unique<TestObject>(); };

    std::unique_ptr<derecho::Group<TestObject>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<TestObject>>(
                node_id, my_ip, callback_set, subgroup_info, derecho_params,
                std::vector<derecho::view_upcall_t>{}, derecho::derecho_gms_port,
                ba_factory);
    } else {
        group = std::make_unique<derecho::Group<TestObject>>(
                node_id, my_ip, leader_ip, callback_set, subgroup_info,
                std::vector<derecho::view_upcall_t>{}, derecho::derecho_gms_port,
                ba_factory);
    }

    std::cout << "Finished constructing/joining Group" << std::endl;

    bool inadequately_provisioned = true;
    while(inadequately_provisioned) {
        try {
            group->get_subgroup<TestObject>();
            inadequately_provisioned = false;
        } catch(derecho::subgroup_provisioning_exception& e) {
            inadequately_provisioned = true;
        }
    }

    std::cout << "All members have joined, subgroups are provisioned." << std::endl;
    derecho::Replicated<TestObject>& handle = group->get_subgroup<TestObject>();
    //std::string str_1k(max_msg_size, 'x');
    char * bbuf = (char*)malloc(max_msg_size);
    bzero(bbuf,max_msg_size);
    Bytes bytes(bbuf,max_msg_size);

    struct timespec t1, t2;
    clock_gettime(CLOCK_REALTIME, &t1);

    for(int i = 0; i < count; i++) {
        //handle.ordered_send<TestObject::FUN>(str_1k);
        handle.ordered_send<TestObject::BYTES_FUN>(bytes);
    }
    clock_gettime(CLOCK_REALTIME, &t2);
    free(bbuf);

    int64_t nsec = ((int64_t)t2.tv_sec - t1.tv_sec) * 1000000000 + t2.tv_nsec - t1.tv_nsec;
    double msec = (double)nsec / 1000000;
    double thp_gbps = ((double)count * max_msg_size * 8) / nsec;
    double thp_ops = ((double)count * 1000000000) / nsec;
    std::cout << "timespan:" << msec << " millisecond." << std::endl;
    std::cout << "throughput:" << thp_gbps << "Gbit/s." << std::endl;
    std::cout << "throughput:" << thp_ops << "ops." << std::endl;

    std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
