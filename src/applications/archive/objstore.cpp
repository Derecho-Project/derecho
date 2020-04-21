#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <time.h>
#include <vector>
#include <string.h>
#include <chrono>

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>
#include <derecho/conf/conf.hpp>
#include <optional>

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

    // Only for test: persistent registry is null and we make a copy constructor move.
    ObjStore(Persistent<std::vector<OSObject>> & _objects) : objects(std::move(_objects)) {}

    // Working with derecho.
    ObjStore(PersistentRegistry* pr) : objects([](){return std::make_unique<std::vector<OSObject>>();}, nullptr, pr) {}
};

typedef OSObject * POSObject;
OSObject **g_objs = nullptr;
void initialize_objects(uint32_t num_of_objs) {
  uint64_t max_msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
  uint32_t node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
  // We just reserved 128 bytes for the message header and serialization.
#define VALUE_SIZE(x) ((x) - 128)
  char default_value[VALUE_SIZE(max_msg_size)];
  if (g_objs == nullptr) {
    g_objs = new POSObject[num_of_objs];
    uint32_t i;
    for (i = 0; i < num_of_objs; i++) {
      g_objs[i] = new OSObject{
	(((uint64_t)node_id)<<32) + i,
	default_value,VALUE_SIZE(max_msg_size)};
    }
  }
}

int main(int argc, char* argv[]) {
    if(argc < 3 || (argc >3 && strcmp(argv[argc-3],"--"))) {
        std::cout << "usage:" << argv[0] << " [ derecho-config-list -- ] <num_of_nodes> <num_of_objs>" << std::endl;
        return -1;
    }
    derecho::Conf::initialize(argc,argv);
    int num_of_nodes = std::stoi(argv[argc-2]);
    int num_of_objs = std::stoi(argv[argc-1]);

    // create the key-value array
    initialize_objects(num_of_objs);

    uint64_t max_msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
    uint64_t total_num_messages = num_of_nodes * num_of_objs;
    struct timespec t_start,t_end;
    std::atomic<bool> bReady = false;
    uint32_t msg_counter = 0;
    persistent::version_t latest_version = INVALID_VERSION;

    derecho::CallbackSet callback_set{
            [&](derecho::subgroup_id_t subgroup, uint32_t nid, int32_t mid, std::optional<std::pair<char*, long long int>> data, persistent::version_t ver){
                msg_counter ++;
                latest_version = ver;
                if (msg_counter == (total_num_messages + num_of_nodes)) {
                    bReady = true;
                }
            },  // using stability callback to match the version.
            nullptr,  // local persistent frontier
            [&](derecho::subgroup_id_t subgroup, persistent::version_t ver) {
                // std::cout << "in global persistent callback: ver = " << ver << std::endl;
                if( bReady && (latest_version <= ver) ) {
                    clock_gettime(CLOCK_REALTIME, &t_end);
                    int64_t nsec = ((int64_t)t_end.tv_sec - t_start.tv_sec) * 1000000000 + t_end.tv_nsec - t_start.tv_nsec;
                    double msec = (double)nsec / 1000000;
                    double thp_gbps = ((double)num_of_objs * max_msg_size * 8) / nsec;
                    double thp_ops = ((double)num_of_objs * 1000000000) / nsec;
                    std::cout << "(pers)timespan:" << msec << " millisecond." << std::endl;
                    std::cout << "(pers)throughput:" << thp_gbps << "Gbit/s." << std::endl;
                    std::cout << "(pers)throughput:" << thp_ops << "ops." << std::endl;
                    std::cout << std::flush;
                }
            }}; // global persistent frontier 

    derecho::SubgroupInfo subgroup_function{[num_of_nodes](
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
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
        curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, num_of_nodes);
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(ObjStore)), std::move(subgroup_vector));
        return subgroup_allocation;
    }};
    auto store_factory = [](PersistentRegistry* pr,derecho::subgroup_id_t) { return std::make_unique<ObjStore>(pr); };

    derecho::Group<ObjStore> group{
                callback_set, subgroup_function, nullptr,
                std::vector<derecho::view_upcall_t>{},
                store_factory};

    std::cout << "Finished constructing/joining Group" << std::endl;

    uint32_t node_rank = group.get_my_rank();

    std::cout << "my rank is:" << node_rank << ", and I'm sending." << std::endl;

    /* send operation */
    derecho::Replicated<ObjStore> & handle = group.get_subgroup<ObjStore>();
    {
        clock_gettime(CLOCK_REALTIME, &t_start);
        // send
        try {
          for (uint32_t i=0;i<(uint32_t)num_of_objs;i++)
            handle.ordered_send<ObjStore::PUT_OBJ>(*g_objs[i]);
        } catch (uint64_t exp) {
            std::cout << "Exception caught:0x" << std::hex << exp << std::endl;
        }
    }

    /* query operation */
    {
        OSObject obj1;
        uint64_t key = num_of_nodes - node_rank - 1;
        auto results = handle.ordered_send<ObjStore::GET_OBJ>(key);
        decltype(results)::ReplyMap& replies = results.get();
        std::cout<<"Got a reply map!"<<std::endl;
        for (auto& ritr:replies) {
            auto obj = ritr.second.get();
            std::cout << "Reply from node " << ritr.first << " was [" << obj.oid << ":"
                << obj.blob.size << std::endl;
        }
    }

    std::cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
