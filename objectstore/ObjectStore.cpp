#include "ObjectStore.hpp"
#include <algorithm>
#include <errno.h>
#include <iostream>
#include <map>
#include <optional>

namespace objectstore {

/*
    The ObjectStore is composed of two kinds of Derecho nodes: the replicas and
    clients. The replicas are responsible for maintaining object data, while
    the clients provide an API layer to the application. The replicas are in
    subgroup 0 managed by class ObjectStore. The clients are only in top-level
    group and access the Replica's subgroup with ExternCaller.

    A short summary of the classes:

    - ObjectStore
    The basic implementation of the object store functionality, where 'basic'
    means absence of persistence and log.

    - DeltaObjectStore
    Delta feature is enabled based on ObjectStore.

    - IObjectStoreAPI
    The interface for p2p_query between clients and replicas.

    - IReplica
    The interface for operations provided by the replica subgroup.

    - IObjectStoreService
    The core APIs expose to object store users.

    - ObjectStoreService
    IObjectStoreService implementation.
    
 */

/*
    Object store configurations.
 */
#define CONF_OBJECTSTORE_MIN_REPLICATION_FACTOR "OBJECTSTORE/min_replication_factor"
#define CONF_OBJECTSTORE_REPLICAS "OBJECTSTORE/replicas"
#define CONF_OBJECTSTORE_PERSISTED "OBJECTSTORE/persisted"
#define CONF_OBJECTSTORE_LOGGED "OBJECTSTORE/logged"

class IObjectStoreAPI {
public:
    // insert or update a new object
    // @PARAM oid
    virtual void put(const Object& object) = 0;
    // remove an object
    // @PARAM oid
    //     the object id
    // @RETURN
    //     return true if oid removed successfully, false if no object is found.
    virtual bool remove(const OID& oid) = 0;
    // get an object
    // @PARAM oid
    //     the object id
    // @RETURN
    //     return the object. If an invalid object is returned, oid is not
    //     found.
    virtual const Object get(const OID& oid) = 0;
};

class IReplica {
public:
    // Perform an ordered 'put' in the subgroup
    // @PARAM oid
    virtual void orderedPut(const Object& object) = 0;
    // Perform an ordered 'remove' in the subgroup
    // @PARAM oid
    //     the object id
    // @RETURN
    //     return true if oid removed successfully, false if no object is found.
    virtual bool orderedRemove(const OID& oid) = 0;
    // Perform an ordered 'get' in the subgroup
    // @PARAM oid
    //     the object id
    // @RETURN
    //     return the object. If an invalid object is returned, oid is not
    //     found.
    virtual const Object orderedGet(const OID& oid) = 0;
};

class ObjectStore : public mutils::ByteRepresentable,
                    public derecho::GroupReference,
                    public IObjectStoreAPI,
                    public IReplica {
public:
    using derecho::GroupReference::group;
    std::map<OID, Object> objects;
    const ObjectWatcher object_watcher;
    const Object inv_obj;

    REGISTER_RPC_FUNCTIONS(ObjectStore,
                           orderedPut,
                           orderedRemove,
                           orderedGet,
                           put,
                           remove,
                           get);

    // @override IReplica::orderedPut
    virtual void orderedPut(const Object& object) {
        this->objects.erase(object.oid);
        this->objects.emplace(object.oid, object);  // copy constructor
        // call object watcher
        if (object_watcher) {
            object_watcher(object.oid,object);
        }
    }
    // @override IReplica::orderedRemove:
    virtual bool orderedRemove(const OID& oid) {
        if (this->objects.erase(oid)) {
            object_watcher(oid,inv_obj);
            return true;
        }
        return false; 
    }
    // @override IReplica::orderedGet
    virtual const Object orderedGet(const OID& oid) {
        if(objects.find(oid) != objects.end()) {
            return objects.at(oid);
        } else {
            return this->inv_obj;
        }
    }

    // @override IObjectStoreAPI::put
    virtual void put(const Object& object) {
        derecho::Replicated<ObjectStore>& subgroup_handle = group->template get_subgroup<ObjectStore>();
        subgroup_handle.ordered_send<RPC_NAME(orderedPut)>(object);
    }
    // @override IObjectStoreAPI::remove
    virtual bool remove(const OID& oid) {
        auto& subgroup_handle = group->template get_subgroup<ObjectStore>();
        derecho::rpc::QueryResults<bool> results = subgroup_handle.template ordered_send<RPC_NAME(orderedRemove)>(oid);
        decltype(results)::ReplyMap& replies = results.get();
        bool bRet = true;
        for(auto& reply_pair : replies) {
            if(!reply_pair.second.get()) {
                bRet = false;
                std::cerr << __FILE__ << ":L" << __LINE__ << ":" << __func__ << "\t node " << reply_pair.first << " returned false" << std::endl;
                break;
            }
        }
        return bRet;
    }
    // @override IObjectStoreAPI::get
    virtual const Object get(const OID& oid) {
        auto& subgroup_handle = group->template get_subgroup<ObjectStore>();
        derecho::rpc::QueryResults<const Object> results = subgroup_handle.template ordered_send<RPC_NAME(orderedGet)>(oid);
        decltype(results)::ReplyMap& replies = results.get();
        // Should we verify the consistency of replies?
        return replies.begin()->second.get();
    }

    // DEFAULT_SERIALIZATION_SUPPORT(ObjectStore, objects);

    DEFAULT_SERIALIZE(objects);

    static std::unique_ptr<ObjectStore> from_bytes(mutils::DeserializationManager* dsm, char const * buf) {
        return std::make_unique<ObjectStore>(
            std::move(*mutils::from_bytes<decltype(objects)>(dsm,buf).get()),
            dsm->mgr<IObjectStoreService>().getObjectWatcher());
    }

    DEFAULT_DESERIALIZE_NOALLOC(ObjectStore);

    void ensure_registered(mutils::DeserializationManager&) {}

    // constructors
    ObjectStore(const ObjectWatcher& ow) : object_watcher(ow) {}
    ObjectStore(std::map<OID, Object>& _objects, const ObjectWatcher& ow) : 
        objects(_objects),
        object_watcher(ow) {}
    ObjectStore(std::map<OID, Object>&& _objects, const ObjectWatcher& ow) : 
        objects(_objects),
        object_watcher(ow) {}
};

// Enable the Delta feature
class DeltaObjectStore : public ObjectStore,
                         public persistent::IDeltaSupport {
#define DEFAULT_DELTA_BUFFER_CAPACITY (4096)
    enum _OPID {
        PUT,
        REMOVE
    };
    struct {
        size_t capacity;
        size_t len;
        char* buffer;
        inline void setOpid(_OPID opid) {
            assert(buffer != nullptr);
            assert(capacity >= sizeof(uint32_t));
            *(_OPID*)buffer = opid;
        }
        inline void setDataLen(const size_t& dlen) {
            assert(capacity >= (dlen + sizeof(uint32_t)));
            this->len = dlen + sizeof(uint32_t);
        }
        inline char* dataPtr() {
            assert(buffer != nullptr);
            assert(capacity > sizeof(uint32_t));
            return buffer + sizeof(uint32_t);
        }
        inline void calibrate(const size_t& dlen) {
            size_t new_cap = dlen + sizeof(uint32_t);
            if(this->capacity >= new_cap) {
                return;
            }
            // calculate new capacity
            int width = sizeof(size_t) << 3;
            int right_shift_bits = 1;
            new_cap--;
            while(right_shift_bits < width) {
                new_cap |= new_cap >> right_shift_bits;
                right_shift_bits = right_shift_bits << 1;
            }
            new_cap++;
            // resize
            this->buffer = (char*)realloc(buffer, new_cap);
            if(this->buffer == nullptr) {
                std::cerr << __FILE__ << ":" << __LINE__ << " Fail to allocate delta buffer." << std::endl
                          << std::flush;
                throw OBJECTSTORE_EXP_MALLOC(errno);
            } else {
                this->capacity = new_cap;
            }
        }
        inline bool isEmpty() {
            return (this->len == 0);
        }
        inline void clean() {
            this->len = 0;
        }
        inline void destroy() {
            if(this->capacity > 0) {
                free(this->buffer);
            }
        }
    } delta;

    void initialize_delta() {
        delta.buffer = (char*)malloc(DEFAULT_DELTA_BUFFER_CAPACITY);
        if(delta.buffer == nullptr) {
            std::cerr << __FILE__ << ":" << __LINE__ << " Fail to allocate delta buffer." << std::endl
                      << std::flush;
            throw OBJECTSTORE_EXP_MALLOC(errno);
        }
        delta.len = DEFAULT_DELTA_BUFFER_CAPACITY;
    }

public:
    ///////////////////////////////////////////////////////////////////////////
    // Object Store Delta is represented by an operation id and a list of
    // argument. The operation id (OPID) is a 4 bytes integer.
    // 1) put(const Object& object):
    // [OPID:PUT]   [object]
    // 2) remove(const OID& oid)
    // [OPID:REMOVE][oid]
    // 3) get(const OID& oid)
    // no need to prepare a delta
    ///////////////////////////////////////////////////////////////////////////
    virtual void finalizeCurrentDelta(const DeltaFinalizer& df) {
        df(this->delta.buffer, this->delta.len);
        this->delta.clean();
    }
    virtual void applyDelta(char const* const delta) {
        const char* data = (delta + sizeof(const uint32_t));
        switch(*(const uint32_t*)delta) {
            case PUT:
                ObjectStore::orderedPut(*mutils::from_bytes<Object>(nullptr, data));
                break;
            case REMOVE:
                ObjectStore::orderedRemove(*(const OID*)data);
                break;
            default:
                std::cerr << __FILE__ << ":" << __LINE__ << ":" << __func__ << " " << std::endl;
        };
    }
    // Can we get the serialized operation representation from Derecho?
    virtual void orderedPut(const Object& object) {
        // create delta.
        assert(this->delta.isEmpty());
        this->delta.calibrate(object.bytes_size());
        object.to_bytes(this->delta.dataPtr());
        this->delta.setDataLen(object.bytes_size());
        this->delta.setOpid(PUT);
        // put
        ObjectStore::orderedPut(object);
    }
    // Can we get the serialized operation representation from Derecho?
    virtual bool orderedRemove(const OID& oid) {
        // create delta
        assert(this->delta.isEmpty());
        this->delta.calibrate(sizeof(OID));
        *(OID*)this->delta.dataPtr() = oid;
        this->delta.setDataLen(sizeof(OID));
        this->delta.setOpid(REMOVE);
        // remove
        return ObjectStore::orderedRemove(oid);
    }

    // Not going to register them as RPC functions because DeltaObjectStore
    // works with PersistedObjectStore instead of the type for Replicated<T>.
    // REGISTER_RPC_FUNCTIONS(ObjectStore, put, remove, get);

    // DEFAULT_SERIALIZATION_SUPPORT(DeltaObjectStore, objects);

    DEFAULT_SERIALIZE(objects);

    static std::unique_ptr<ObjectStore> from_bytes(mutils::DeserializationManager* dsm, char const * buf) {
        return std::make_unique<ObjectStore>(
            std::move(*mutils::from_bytes<decltype(objects)>(dsm,buf).get()),
            dsm->mgr<IObjectStoreService>().getObjectWatcher());
    }

    DEFAULT_DESERIALIZE_NOALLOC(DeltaObjectStore);

    void ensure_registered(mutils::DeserializationManager&) {}

    // constructor
    DeltaObjectStore(const ObjectWatcher& ow) : ObjectStore(ow) {
        initialize_delta();
    }
    DeltaObjectStore(std::map<OID, Object>& _objects, const ObjectWatcher& ow) : 
        ObjectStore(_objects, ow) {
        initialize_delta();
    }
    DeltaObjectStore(std::map<OID, Object>&& _objects, const ObjectWatcher& ow) : 
        ObjectStore(_objects, ow) {
        initialize_delta();
    }
    virtual ~DeltaObjectStore() {
        if(delta.buffer != nullptr) {
            free(delta.buffer);
        }
    }
};

// ==============================================================

// helper functions
// get replica list
// @PARAM replica_str
//     a list of replicas in string representation like: 1,2,5-7,100
// @RETURN
//     a vector of replicas
static std::vector<node_id_t> parseReplicaList(
        const std::string& replica_str) {
    std::string::size_type s = 0, e;
    std::vector<node_id_t> replicas;
    while(s < replica_str.size()) {
        e = replica_str.find(',', s);
        if(e == std::string::npos) {
            e = replica_str.size();
        }
        if(e > s) {
            std::string range = replica_str.substr(s, e - s);
            std::string::size_type hyphen_pos = range.find('-');
            if(hyphen_pos != std::string::npos) {
                // range is "a-b"
                node_id_t rsid = std::stol(range.substr(0, hyphen_pos));
                node_id_t reid = std::stol(range.substr(hyphen_pos + 1));
                while(rsid <= reid) {
                    replicas.push_back(rsid);
                    rsid++;
                }
            } else {
                replicas.push_back((node_id_t)std::stol(range));
            }
        }
        s = e + 1;
    }
    return std::move(replicas);
}

class ObjectStoreService : public IObjectStoreService,
                           public derecho::IDeserializationContext {
private:
    const ObjectWatcher& object_watcher;
    std::vector<node_id_t> replicas;
    const bool bReplica;
    const node_id_t myid;
    derecho::Group<ObjectStore> group;
    std::mutex write_mutex;

public:
    // constructor
    ObjectStoreService(const ObjectWatcher& ow) : 
        object_watcher(ow),
        replicas(parseReplicaList(derecho::getConfString(CONF_OBJECTSTORE_REPLICAS))),
        bReplica(std::find(replicas.begin(), replicas.end(),
            derecho::getConfUInt64(CONF_DERECHO_LOCAL_ID)) != replicas.end()),
        myid(derecho::getConfUInt64(CONF_DERECHO_LOCAL_ID)),
        group(
                {},  // callback set
                // derecho::SubgroupInfo
                {
                    [this](const std::type_index& subgroup_type,
                           const std::unique_ptr<derecho::View>& prev_view,
                           derecho::View& curr_view) {
                        if (subgroup_type == std::type_index(typeid(ObjectStore))) {
                            std::vector<node_id_t> active_replicas;
                            for(uint32_t i = 0; i < curr_view.members.size(); i++){
                                const node_id_t id = curr_view.members[i];
                                if(!curr_view.failed[i] && std::find(replicas.begin(),replicas.end(),id) != replicas.end()) {
                                    active_replicas.push_back(id);
                                }
                            }
                            if(active_replicas.size() < derecho::getConfUInt32(CONF_OBJECTSTORE_MIN_REPLICATION_FACTOR)) {
                                throw derecho::subgroup_provisioning_exception();
                            }
    
                            derecho::subgroup_shard_layout_t subgroup_vector(1);
                            subgroup_vector[0].emplace_back(curr_view.make_subview(active_replicas));
                            curr_view.next_unassigned_rank += active_replicas.size();
                            return subgroup_vector;
                        } else {
                            return derecho::subgroup_shard_layout_t{};
                        }
                    }
                }, 
                std::shared_ptr<derecho::IDeserializationContext>{this},
                std::vector<derecho::view_upcall_t>{},                               // view up-calls
                [this](PersistentRegistry*) { return std::make_unique<ObjectStore>(object_watcher); }  // factories ...
        ) {}

    virtual const bool isReplica() {
        return bReplica;
    }

    virtual void put(const Object& object) {
        std::lock_guard<std::mutex> guard(write_mutex);
        if(bReplica) {
            // replica server can do ordered send
            derecho::Replicated<ObjectStore>& os_rpc_handle = group.get_subgroup<ObjectStore>();
            os_rpc_handle.ordered_send<RPC_NAME(orderedPut)>(object);
        } else {
            // send request to a static mapped replica. Use random mapping for load-balance?
            node_id_t target = myid % replicas.size();
            derecho::ExternalCaller<ObjectStore>& os_p2p_handle = group.get_nonmember_subgroup<ObjectStore>();
            os_p2p_handle.p2p_send<RPC_NAME(put)>(target, object);
        }
    }

    virtual bool remove(const OID& oid) {
        bool bRet;
        std::lock_guard<std::mutex> guard(write_mutex);
        if(bReplica) {
            // replica server can do ordered send
            derecho::Replicated<ObjectStore>& os_rpc_handle = group.get_subgroup<ObjectStore>();
            derecho::rpc::QueryResults<bool> results = os_rpc_handle.ordered_send<RPC_NAME(orderedRemove)>(oid);
            decltype(results)::ReplyMap& replies = results.get();
            // should we check reply consistency?
            bRet = replies.begin()->second.get();
        } else {
            // send request to a static mapped replica. Use random mapping for load-balance?
            node_id_t target = myid % replicas.size();
            derecho::ExternalCaller<ObjectStore>& os_p2p_handle = group.get_nonmember_subgroup<ObjectStore>();
            derecho::rpc::QueryResults<bool> results = os_p2p_handle.p2p_query<RPC_NAME(remove)>(target, oid);
            bRet = results.get().get(target);
        }
        return bRet;
    }

    virtual Object get(const OID& oid) {
        std::lock_guard<std::mutex> guard(write_mutex);
        if(bReplica) {
            // replica server can do ordered send
            derecho::Replicated<ObjectStore>& os_rpc_handle = group.get_subgroup<ObjectStore>();
            derecho::rpc::QueryResults<const Object> results = os_rpc_handle.ordered_send<RPC_NAME(orderedGet)>(oid);
            decltype(results)::ReplyMap& replies = results.get();
            // should we check reply consistency?
            return std::move(replies.begin()->second.get());
        } else {
            // send request to a static mapped replica. Use random mapping for load-balance?
            node_id_t target = myid % replicas.size();
            derecho::ExternalCaller<ObjectStore>& os_p2p_handle = group.get_nonmember_subgroup<ObjectStore>();
            derecho::rpc::QueryResults<const Object> results = os_p2p_handle.p2p_query<RPC_NAME(get)>(target, oid);
            return std::move(results.get().get(target));
        }
    }

    virtual void leave() {
        group.leave();
    }

    virtual const ObjectWatcher& getObjectWatcher() {
        return this->object_watcher;
    }

    // get singleton
    static IObjectStoreService& get(int argc, char** argv, const ObjectWatcher& ow = {});
};

// The singleton unique pointer
std::unique_ptr<IObjectStoreService> IObjectStoreService::singleton;

// get the singleton
// NOTE: caller only get access to this member object. The ownership of this
// object is NOT transferred.
IObjectStoreService& IObjectStoreService::getObjectStoreService(int argc, char** argv, const ObjectWatcher& ow) {

    if(IObjectStoreService::singleton.get() == nullptr) {
        // step 1: initialize the configuration
        derecho::Conf::initialize(argc, argv);
        // step 2: create the group resources
        IObjectStoreService::singleton = std::make_unique<ObjectStoreService>(ow);
    }

    return *IObjectStoreService::singleton.get();
};

}  // namespace objectstore
