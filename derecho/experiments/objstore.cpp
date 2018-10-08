#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <time.h>
#include <vector>
#include <string.h>
#include <chrono>

#include "block_size.h"
#include "derecho/derecho.h"
#include "initialize.h"
#include <mutils-serialization/SerializationSupport.hpp>
#include <persistent/Persistent.hpp>
#include "conf/conf.hpp"

using std::cout;
using std::endl;
using namespace persistent;
using namespace std::chrono_literals;

// The binary large object for serialized objects.
class Blob : public mutils::ByteRepresentable {
public:
    char * bytes;
    std::size_t size;

    // constructor - we always copy to 'own' the data
    Blob(const char * const b, const decltype(size) s) : bytes(nullptr),size(s) {
        if ( s > 0 ) {
            bytes = new char[s];
            memcpy(bytes, b, s);
        }
    }

    // copy constructor - we always copy to 'own' the data
    Blob(const Blob & other): bytes(nullptr),size(other.size) {
        bytes = nullptr;
        if ( size > 0 ) {
            bytes = new char[size];
            memcpy(bytes, other.bytes, other.size);
        }
    }

    // default constructor - no data at all
    Blob () : bytes(nullptr), size(0) { }

    // destructor
    virtual ~Blob() {
        if (bytes) delete bytes;
    }

    // move evaluator:
    Blob & operator = (Blob &&other) {
        char *swp_bytes = other.bytes;
        std::size_t swp_size = other.size;
        other.bytes = bytes;
        other.size = size;
        bytes = swp_bytes;
        size = swp_size;
        return *this;
    }

    // copy evaluator:
    Blob & operator = (const Blob &other) {
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
    
    static std::unique_ptr<Blob> from_bytes(mutils::DeserializationManager *, const char *const v) {
        return std::make_unique<Blob>(v + sizeof(std::size_t), ((std::size_t *)(v))[0]); 
    }   

    // we disabled from_bytes_noalloc calls for now.    
};

class OSObject : public mutils::ByteRepresentable {
public:
#define INVALID_OID	(0xffffffffffffffffLLU)
    const uint64_t oid; // object_id
    Blob blob; // the object

    bool operator == (const OSObject & other) {
      return this->oid == other.oid;
    }
    bool is_valid () const {
      return (oid == INVALID_OID);
    }

    // constructor 0 : normal
    OSObject(uint64_t & _oid, Blob & _blob):
        oid(_oid),
        blob(_blob) {}
    // constructor 1 : raw
    OSObject(const uint64_t _oid, const char * const _b, const std::size_t _s):
        oid(_oid),
        blob(_b,_s) {}
    // constructor 2 : move
    OSObject(OSObject && other):
        oid(other.oid) {
        blob = other.blob;
    }
    // constructor 3 : copy
    OSObject(const OSObject & other):
       oid(other.oid),
       blob(other.blob.bytes,other.blob.size) {}
    // constructor 4 : invalid
    OSObject() : oid(INVALID_OID) {}

    DEFAULT_SERIALIZATION_SUPPORT(OSObject, oid, blob);
};

class ObjStore : public mutils::ByteRepresentable, public derecho::PersistsFields {
public:
    Persistent<std::vector<OSObject>> objects;
    const OSObject inv_obj;

    void put(const OSObject & obj) {
        for (OSObject & o : *objects) {
            if ( o == obj ) {
                Blob tmp(obj.blob); // create
                o.blob = std::move(tmp); // swap.
                return;
            }
        }
        // we didn't find it. insert...
        objects->emplace_back(obj);
    }

    // This get is SUPER inefficient
    // Passing an output buffer??
    const OSObject get(const uint64_t oid) {
        for(OSObject & o : *objects) {
            if ( o.oid == oid ) {
                return o;
            }
        }
        return this->inv_obj;
    }

    enum Functions { PUT_OBJ, GET_OBJ };

    static auto register_functions() {
      return std::make_tuple(
        derecho::rpc::tag<PUT_OBJ>(&ObjStore::put),
        derecho::rpc::tag<GET_OBJ>(&ObjStore::get)
      );
    }

    DEFAULT_SERIALIZATION_SUPPORT(ObjStore, objects);

    /*
    **/

    // Only for test: persistent registry is null and we make a copy constructor move.
    ObjStore(Persistent<std::vector<OSObject>> & _objects) : objects(std::move(_objects)) {}

    // Working with derecho.
    ObjStore(PersistentRegistry* pr) : objects(nullptr, pr) {}
};

int main(int argc, char* argv[]) {
    if(argc < 2) {
        std::cout << "usage:" << argv[0] << " <num_of_nodes> [window_size]" << std::endl;
        return -1;
    }
    int num_of_nodes = atoi(argv[1]);

    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;
    query_node_info(node_id, my_ip, leader_ip);
    long long unsigned int max_msg_size = 10000;
    long long unsigned int msg_size = 1000;
    long long unsigned int block_size = get_block_size(msg_size);
    const long long unsigned int sst_max_msg_size = (max_msg_size < 17000 ? max_msg_size : 0);
    unsigned int window_size = 3;
    if (argc > 2) {
        window_size = atoi(argv[2]);
    }
    if(argc >= 6) {
        window_size = (unsigned int)atoi(argv[5]);
    }
    derecho::DerechoParams derecho_params{
      max_msg_size,
      sst_max_msg_size,
      block_size,
      window_size};
    bool is_sending = true;
    long count = 1;
    long total_num_messages = num_of_nodes * count;
    struct timespec t1,t2,t3;

    derecho::CallbackSet callback_set{
            nullptr,  //we don't need the stability_callback here
            // the persistence_callback either
            [&](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
                if(ver == (total_num_messages - 1)) {
                    if(is_sending) {
                        clock_gettime(CLOCK_REALTIME, &t3);
                        int64_t nsec = ((int64_t)t3.tv_sec - t1.tv_sec) * 1000000000 + t3.tv_nsec - t1.tv_nsec;
                        double msec = (double)nsec / 1000000;
                        double thp_gbps = ((double)count * msg_size * 8) / nsec;
                        double thp_ops = ((double)count * 1000000000) / nsec;
                        std::cout << "(pers)timespan:" << msec << " millisecond." << std::endl;
                        std::cout << "(pers)throughput:" << thp_gbps << "Gbit/s." << std::endl;
                        std::cout << "(pers)throughput:" << thp_ops << "ops." << std::endl;
                        std::cout << std::flush;
                    }
                }
            }};

    derecho::SubgroupInfo subgroup_info{
            {{std::type_index(typeid(ObjStore)), [num_of_nodes](const derecho::View& curr_view, int& next_unassigned_rank, bool previous_was_successful) {
                  if(curr_view.num_members < num_of_nodes) {
                      std::cout << "not enough members yet:" << curr_view.num_members << " < " << num_of_nodes << std::endl;
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);

                  std::vector<uint32_t> members(num_of_nodes);
                  std::vector<int> senders(num_of_nodes, 1);
                  for(int i = 0; i < num_of_nodes; i++)
                      members[i] = i;

                  subgroup_vector[0].emplace_back(curr_view.make_subview(members, derecho::Mode::ORDERED, senders));
                  next_unassigned_rank = std::max(next_unassigned_rank, num_of_nodes);
                  return subgroup_vector;
              }}},
            {std::type_index(typeid(ObjStore))}};

    auto store_factory = [](PersistentRegistry* pr) { return std::make_unique<ObjStore>(pr); };

    std::unique_ptr<derecho::Group<ObjStore>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<ObjStore>>(
                node_id, my_ip, callback_set, subgroup_info, derecho_params,
                std::vector<derecho::view_upcall_t>{}, derecho::getConfInt32(CONF_DERECHO_GMS_PORT),
                store_factory);
    } else {
        group = std::make_unique<derecho::Group<ObjStore>>(
                node_id, my_ip, leader_ip, callback_set, subgroup_info,
                std::vector<derecho::view_upcall_t>{}, derecho::getConfInt32(CONF_DERECHO_GMS_PORT),
                store_factory);
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

    std::cout << "my rank is:" << node_rank << ", and I'm sending:" << is_sending << std::endl;

    /* send operation */
    char data[256] = "Hello! My rank is: ";
    sprintf(data+strlen(data),"%d",node_rank);
    derecho::Replicated<ObjStore> & handle = group->get_subgroup<ObjStore>();
    {
        // send
        OSObject obj{(uint64_t)node_rank,data,strlen(data)};
        try {
            handle.ordered_send<ObjStore::PUT_OBJ>(obj);
        } catch (uint64_t exp) {
            std::cout << "Exception caught:0x" << std::hex << exp << std::endl;
        }
    }

    // std::this_thread::sleep_for(5s);

    /* query operation */
    {
        OSObject obj1;
        uint64_t key = num_of_nodes - node_rank - 1;
        auto results = handle.ordered_query<ObjStore::GET_OBJ>(key);
        decltype(results)::ReplyMap& replies = results.get();
        std::cout<<"Got a reply map!"<<std::endl;
        for (auto& ritr:replies) {
            auto obj = ritr.second.get();
            std::cout << "Reply from node " << ritr.first << " was [" << obj.oid << ":"
                << obj.blob.size << std::endl;
        }
    }
    /*
  if (node_id == 0) {
    derecho::Replicated<ByteArrayObject<1024>>& handle = group->get_subgroup<ByteArrayObject<1024>>();
    char my_array[1024];
    derecho::rpc::QueryResults<bool> results = handle.ordered_query<ByteArrayObject<1024>::CHANGE_STATE>(my_array);
    decltype(results)::ReplyMap& replies = results.get();
    std::cout<<"Got a reply map!"<<std::endl;
    for(auto& ritr:replies) {
      std::cout<<"Reply from node "<< ritr.first <<" was " << std::boolalpha << ritr.second.get()<<std::endl;
    }
  }
*/
    // if (node_id == 0) {

/*
    if(is_sending) {
        derecho::Replicated<ByteArrayObject>& handle = group->get_subgroup<ByteArrayObject>();
        char* bbuf = new char[msg_size];
        bzero(bbuf, msg_size);
        Bytes bs(bbuf, msg_size);

        try {
            clock_gettime(CLOCK_REALTIME, &t1);
            for(int i = 0; i < count; i++) {
                handle.ordered_send<ByteArrayObject::CHANGE_PERS_BYTES>(bs);
            }
            clock_gettime(CLOCK_REALTIME, &t2);

        } catch(uint64_t exp) {
            std::cout << "Exception caught:0x" << std::hex << exp << std::endl;
            return -1;
        }
        int64_t nsec = ((int64_t)t2.tv_sec - t1.tv_sec) * 1000000000 + t2.tv_nsec - t1.tv_nsec;
        double msec = (double)nsec / 1000000;
        double thp_gbps = ((double)count * msg_size * 8) / nsec;
        double thp_ops = ((double)count * 1000000000) / nsec;
        std::cout << "(send)timespan:" << msec << " millisecond." << std::endl;
        std::cout << "(send)throughput:" << thp_gbps << "Gbit/s." << std::endl;
        std::cout << "(send)throughput:" << thp_ops << "ops." << std::endl;
#ifdef _PERFORMANCE_DEBUG
        (*handle.user_object_ptr)->pers_bytes.print_performance_stat();
#endif  //_PERFORMANCE_DEBUG
    }
*/

    std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
