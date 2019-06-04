#include <derecho/objectstore/ObjectStore.hpp>
#include <derecho/utils/logger.hpp>
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

    - DeltaObjectStoreCore
    Delta feature is enabled.

    - VolatileUnloggedObjectStore (Type for a derecho subgroup)
    The implementation of an object store with out persistence and log.

    - PersistentUnloggedObjectStore (Type for a derecho subgroup)
    The implementation of an object store with persistence. Operations are
    unlogged.

    - PersistentLoggedObjectStore (Type for a derecho subgroup)
    The implementation of an object store with both persistence and log. We
    do not explicitly support a "VolatileLoggedObjectStore" Type right now.

    - IObjectStoreAPI
    The interface for p2p_send between clients and replicas.

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
    // @PARAM object - reference to the object to be inserted or updated.
    //        object.ver is not used and will be assigned a value after
    //        'put' operation finish.
    // @RETURN
    //     return the version of the new object
    virtual std::tuple<version_t,uint64_t> put(const Object& object) = 0;
    // remove an object
    // @PARAM oid
    //     the object id
    // @RETURN
    //     return the version of the remove operation.
    virtual std::tuple<version_t,uint64_t> remove(const OID& oid) = 0;
    // get an object
    // @PARAM oid
    //     the object id
    // @PARAM ver
    //     the version of the object requested. An ordered get will be performed
    //     if the version is INVALID_VERSION.
    // @RETURN
    //     return the object. If an invalid object is returned, one of the
    //     following happens:
    //     - oid is not found
    //     - ver is not found
    //     - ver is not INVALID_VERSION, but versions are not logged.
    virtual const Object get(const OID& oid, const version_t& ver) = 0;
    // @PARAM oid
    //     the object id
    // @PARAM ts_us
    //     timestamp in microsecond
    // @RETURN
    //     return the object. If an invalid object is returned, oid is not
    //     found or is not found at time ts_us.
    virtual const Object get_by_time(const OID& oid, const uint64_t& ts_us) = 0;
};

class IReplica {
public:
    // Perform an ordered 'put' in the subgroup
    //
    // @PARAM oid
    // @RETURN
    //     return the version of the new object
    virtual std::tuple<version_t,uint64_t> orderedPut(const Object& object) = 0;
    // Perform an ordered 'remove' in the subgroup
    // @PARAM oid
    //     the object id
    // @RETURN
    //     return the version of the remove operation
    virtual std::tuple<version_t,uint64_t> orderedRemove(const OID& oid) = 0;
    // Perform an ordered 'get' in the subgroup
    // @PARAM oid
    //     the object id
    // @RETURN
    //     return the object. If an invalid object is returned, oid is not
    //     found.
    virtual const Object orderedGet(const OID& oid) = 0;
};

class VolatileUnloggedObjectStore : public IReplica,
                                    public mutils::ByteRepresentable,
                                    public derecho::GroupReference,
                                    public IObjectStoreAPI {
public:
    using derecho::GroupReference::group;
    std::map<OID, Object> objects;
    const ObjectWatcher object_watcher;
    const Object inv_obj;

    REGISTER_RPC_FUNCTIONS(VolatileUnloggedObjectStore,
                           orderedPut,
                           orderedRemove,
                           orderedGet,
                           put,
                           remove,
                           get,
                           get_by_time);

    inline std::tuple<version_t,uint64_t> get_version() {
        derecho::Replicated<VolatileUnloggedObjectStore>& subgroup_handle = group->template get_subgroup<VolatileUnloggedObjectStore>();
        return subgroup_handle.get_next_version();
    }

    // @override IObjectStoreAPI::put
    virtual std::tuple<version_t,uint64_t> put(const Object& object) {
        derecho::Replicated<VolatileUnloggedObjectStore>& subgroup_handle = group->template get_subgroup<VolatileUnloggedObjectStore>();
        auto results = subgroup_handle.ordered_send<RPC_NAME(orderedPut)>(object);
        decltype(results)::ReplyMap& replies = results.get();
        std::tuple<version_t,uint64_t> vRet(INVALID_VERSION,0);
        // TODO: should we verify consistency of the versions?
        for(auto& reply_pair : replies) {
            vRet = reply_pair.second.get();
        }
        return vRet;
    }
    // @override IObjectStoreAPI::remove
    virtual std::tuple<version_t,uint64_t> remove(const OID& oid) {
        auto& subgroup_handle = group->template get_subgroup<VolatileUnloggedObjectStore>();
        derecho::rpc::QueryResults<std::tuple<version_t,uint64_t>> results = subgroup_handle.template ordered_send<RPC_NAME(orderedRemove)>(oid);
        decltype(results)::ReplyMap& replies = results.get();
        std::tuple<version_t,uint64_t> vRet(INVALID_VERSION,0);
        // TODO: should we verify consistency of the versions?
        for(auto& reply_pair : replies) {
            vRet = reply_pair.second.get();
        }
        return vRet;
    }
    // @override IObjectStoreAPI::get
    virtual const Object get(const OID& oid, const version_t& ver) {
        // check version
        if(ver != INVALID_VERSION) {
            dbg_default_info("{}:{} does not support versioned query ( oid = {}; ver = 0x{:x} ). Return with an invalid object.",
                             typeid(*this).name(), __func__, oid, ver);
            return inv_obj;
        }

        auto& subgroup_handle = group->template get_subgroup<VolatileUnloggedObjectStore>();
        derecho::rpc::QueryResults<const Object> results = subgroup_handle.template ordered_send<RPC_NAME(orderedGet)>(oid);
        decltype(results)::ReplyMap& replies = results.get();
        // here we only check the first reply.
        // Should we verify the consistency of all replies?
        return replies.begin()->second.get();
    }
    //@overrid IObjectStoreAPI::get_by_time
    virtual const Object get_by_time(const OID& oid, const uint64_t& ts_us) {
        dbg_default_info("{}:{} does not support temporal query (oid = {}; timestamp = {} us). Return with an invalid object.",
                         typeid(*this).name(), __func__, oid, ts_us);
        return inv_obj;
    }

    // This is for REGISTER_RPC_FUNCTIONS
    // @override IReplica::orderedPut
    virtual std::tuple<version_t,uint64_t> orderedPut(const Object& object) {
        std::tuple<version_t,uint64_t> version = get_version();
        dbg_default_info("orderedPut object:{},version:0x{:x},timestamp:{}", object.oid, std::get<0>(version), std::get<1>(version));
        this->objects.erase(object.oid);
        object.ver = version;
        this->objects.emplace(object.oid, object);  // copy constructor
        // call object watcher
        if(object_watcher) {
            object_watcher(object.oid, object);
        }
        return object.ver;
    }
    // @override IReplica::orderedRemove
    virtual std::tuple<version_t,uint64_t> orderedRemove(const OID& oid) {
        auto version = get_version();
        dbg_default_info("orderedRemove object:{},version:0x{:x},timestamp:{}", oid, std::get<0>(version), std::get<1>(version));
        if(this->objects.erase(oid)) {
            object_watcher(oid, inv_obj);
        }
        return version;
    }
    // @override IReplica::orderedGet
    virtual const Object orderedGet(const OID& oid) {
        auto version = get_version();
        dbg_default_info("orderedGet object:{},version:0x{:x},timestamp:{}", oid, std::get<0>(version), std::get<1>(version));
        if(objects.find(oid) != objects.end()) {
            return objects.at(oid);
        } else {
            return this->inv_obj;
        }
    }

    DEFAULT_SERIALIZE(objects);

    static std::unique_ptr<VolatileUnloggedObjectStore> from_bytes(mutils::DeserializationManager* dsm, char const* buf) {
// OPTION ONE to test
//        return std::make_unique<VolatileUnloggedObjectStore>(
//                std::move(*mutils::from_bytes<decltype(objects)>(dsm, buf)),
//                dsm->mgr<IObjectStoreService>().getObjectWatcher());
        auto ptr_to_objects = mutils::from_bytes<decltype(objects)>(dsm, buf);
        auto ptr_to_return = std::make_unique<VolatileUnloggedObjectStore>(std::move(*ptr_to_objects),
                dsm->mgr<IObjectStoreService>().getObjectWatcher());
        ptr_to_objects.release(); // to avoid double free.
        return ptr_to_return;
    }

    DEFAULT_DESERIALIZE_NOALLOC(VolatileUnloggedObjectStore);

    void ensure_registered(mutils::DeserializationManager&) {}

    // constructors
    VolatileUnloggedObjectStore(const ObjectWatcher& ow) : object_watcher(ow) {}
    VolatileUnloggedObjectStore(const std::map<OID, Object>& _objects, const ObjectWatcher& ow) : objects(_objects), object_watcher(ow) {}
    VolatileUnloggedObjectStore(std::map<OID, Object>&& _objects, const ObjectWatcher& ow) : objects(std::move(_objects)), object_watcher(ow) {}
};

// Enable the Delta feature
class DeltaObjectStoreCore : public mutils::ByteRepresentable,
                             public persistent::IDeltaSupport<DeltaObjectStoreCore> {
#define DEFAULT_DELTA_BUFFER_CAPACITY (4096)
    enum _OPID {
        PUT,
        REMOVE
    };
    // _dosc_delta is a name used only for struct constructor.
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
                dbg_default_crit("{}:{} Failed to allocate delta buffer. errno={}", __FILE__, __LINE__, errno);
                throw derecho::derecho_exception("Failed to allocate delta buffer.");
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
            dbg_default_crit("{}:{} Failed to allocate delta buffer. errno={}", __FILE__, __LINE__, errno);
            throw derecho::derecho_exception("Failed to allocate delta buffer.");
        }
        delta.capacity = DEFAULT_DELTA_BUFFER_CAPACITY;
        delta.len = 0;
    }

public:
    std::map<OID, Object> objects;
    const ObjectWatcher object_watcher;
    const Object inv_obj;
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
    // @override IDeltaSupport::finalizeCurrentDelta()
    virtual void finalizeCurrentDelta(const DeltaFinalizer& df) {
        df(this->delta.buffer, this->delta.len);
        this->delta.clean();
    }
    // @override IDeltaSupport::applyDelta()
    virtual void applyDelta(char const* const delta) {
        const char* data = (delta + sizeof(const uint32_t));
        switch(*(const uint32_t*)delta) {
            case PUT:
                applyOrderedPut(*mutils::from_bytes<Object>(nullptr, data));
                break;
            case REMOVE:
                applyOrderedRemove(*(const OID*)data);
                break;
            default:
                std::cerr << __FILE__ << ":" << __LINE__ << ":" << __func__ << " " << std::endl;
        };
    }

    // @override IDeltaSupport::create()
    static std::unique_ptr<DeltaObjectStoreCore> create(mutils::DeserializationManager* dm) {
        if(dm != nullptr) {
            try {
                return std::make_unique<DeltaObjectStoreCore>(dm->mgr<IObjectStoreService>().getObjectWatcher());
            } catch(...) {
            }
        }
        return std::make_unique<DeltaObjectStoreCore>((ObjectWatcher){});
    }

    inline void applyOrderedPut(const Object& object) {
        // put
        this->objects.erase(object.oid);
        this->objects.emplace(object.oid, object);
        // call object watcher
        if(object_watcher) {
            object_watcher(object.oid, object);
        }
    }
    inline bool applyOrderedRemove(const OID& oid) {
        bool bRet = false;
        // remove
        if(this->objects.erase(oid)) {
            // call object watcher
            if(object_watcher) {
                object_watcher(oid, inv_obj);
            }
            bRet = true;
        }
        return bRet;
    }

    // Can we get the serialized operation representation from Derecho?
    virtual bool orderedPut(const Object& object) {
        // create delta.
        assert(this->delta.isEmpty());
        this->delta.calibrate(object.bytes_size());
        object.to_bytes(this->delta.dataPtr());
        this->delta.setDataLen(object.bytes_size());
        this->delta.setOpid(PUT);
        // apply orderedPut
        applyOrderedPut(object);
        return true;
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
        return applyOrderedRemove(oid);
    }

    virtual const Object orderedGet(const OID& oid) {
        if(objects.find(oid) != objects.end()) {
            return objects.at(oid);
        } else {
            return this->inv_obj;
        }
    }

    // Not going to register them as RPC functions because DeltaObjectStoreCore
    // works with PersistedObjectStore instead of the type for Replicated<T>.
    // REGISTER_RPC_FUNCTIONS(ObjectStore, put, remove, get);

    // DEFAULT_SERIALIZATION_SUPPORT(DeltaObjectStoreCore, objects);

    DEFAULT_SERIALIZE(objects);

    static std::unique_ptr<DeltaObjectStoreCore> from_bytes(mutils::DeserializationManager* dsm, char const* buf) {
        if(dsm != nullptr) {
            try {
                return std::make_unique<DeltaObjectStoreCore>(
                    std::move(*mutils::from_bytes<decltype(objects)>(dsm, buf).get()),
                    dsm->mgr<IObjectStoreService>().getObjectWatcher());
            } catch(...) {
            }
        }
        return std::make_unique<DeltaObjectStoreCore>(
            std::move(*mutils::from_bytes<decltype(objects)>(dsm, buf).get()),
            (ObjectWatcher){});
    }

    DEFAULT_DESERIALIZE_NOALLOC(DeltaObjectStoreCore);

    void ensure_registered(mutils::DeserializationManager&) {}

    // constructor
    DeltaObjectStoreCore(const ObjectWatcher& ow) : object_watcher(ow) {
        initialize_delta();
    }
    DeltaObjectStoreCore(const std::map<OID, Object>& _objects, const ObjectWatcher& ow) : objects(_objects), object_watcher(ow) {
        initialize_delta();
    }
    DeltaObjectStoreCore(std::map<OID, Object>&& _objects, const ObjectWatcher& ow) : objects(_objects), object_watcher(ow) {
        initialize_delta();
    }
    virtual ~DeltaObjectStoreCore() {
        if(delta.buffer != nullptr) {
            free(delta.buffer);
        }
    }
};

class PersistentLoggedObjectStore : public mutils::ByteRepresentable,
                                    public derecho::PersistsFields,
                                    public derecho::GroupReference,
                                    public IObjectStoreAPI,
                                    public IReplica {
private:
    const Object inv_obj;

public:
    using derecho::GroupReference::group;
    Persistent<DeltaObjectStoreCore> persistent_objectstore;

    REGISTER_RPC_FUNCTIONS(PersistentLoggedObjectStore,
                           orderedPut,
                           orderedRemove,
                           orderedGet,
                           put,
                           remove,
                           get,
                           get_by_time);

    // @override IReplica::orderedPut
    virtual std::tuple<version_t,uint64_t> orderedPut(const Object& object) {
        auto& subgroup_handle = group->template get_subgroup<PersistentLoggedObjectStore>();
        object.ver = subgroup_handle.get_next_version();
        dbg_default_info("orderedPut object:{},version:0x{:x},timestamp:{}", object.oid, std::get<0>(object.ver), std::get<1>(object.ver));
        this->persistent_objectstore->orderedPut(object);
        return object.ver;
    }
    // @override IReplica::orderedRemove
    virtual std::tuple<version_t,uint64_t> orderedRemove(const OID& oid) {
        auto& subgroup_handle = group->template get_subgroup<PersistentLoggedObjectStore>();
        std::tuple<version_t,uint64_t> vRet = subgroup_handle.get_next_version();
        dbg_default_info("orderedRemove object:{},version:0x{:x},timestamp:{}", oid, std::get<0>(vRet), std::get<1>(vRet));
        this->persistent_objectstore->orderedRemove(oid);
        return vRet;
    }
    // @override IReplica::orderedGet
    virtual const Object orderedGet(const OID& oid) {
#ifndef NDEBUG
        auto& subgroup_handle = group->template get_subgroup<PersistentLoggedObjectStore>();
        auto version = subgroup_handle.get_next_version();
        dbg_default_info("orderedGet object:{},version:0x{:x},timestamp:{}", oid, std::get<0>(version), std::get<1>(version));
#endif
        return this->persistent_objectstore->orderedGet(oid);
    }
    // @override IObjectStoreAPI::put
    virtual std::tuple<version_t,uint64_t> put(const Object& object) {
        auto& subgroup_handle = group->template get_subgroup<PersistentLoggedObjectStore>();
        auto results = subgroup_handle.ordered_send<RPC_NAME(orderedPut)>(object);
        decltype(results)::ReplyMap& replies = results.get();
        std::tuple<version_t,uint64_t> vRet(INVALID_VERSION,0);
        for(auto& reply_pair : replies) {
            vRet = reply_pair.second.get();
        }
        return vRet;
    }
    // @override IObjectStoreAPI::remove
    virtual std::tuple<version_t,uint64_t> remove(const OID& oid) {
        auto& subgroup_handle = group->template get_subgroup<PersistentLoggedObjectStore>();
        derecho::rpc::QueryResults<std::tuple<version_t,uint64_t>> results = subgroup_handle.template ordered_send<RPC_NAME(orderedRemove)>(oid);
        decltype(results)::ReplyMap& replies = results.get();
        std::tuple<version_t,uint64_t> vRet(INVALID_VERSION,0);
        for(auto& reply_pair : replies) {
            vRet = reply_pair.second.get();
        }
        return vRet;
    }
    // @override IObjectStoreAPI::get
    virtual const Object get(const OID& oid, const version_t& ver) {
        if(ver == INVALID_VERSION) {
            auto& subgroup_handle = group->template get_subgroup<PersistentLoggedObjectStore>();
            derecho::rpc::QueryResults<const Object> results = subgroup_handle.template ordered_send<RPC_NAME(orderedGet)>(oid);

            decltype(results)::ReplyMap& replies = results.get();
            // Here we only wait for the first reply.
            // Should we verify the consistency of replies?
            return replies.begin()->second.get();
        } else {  // do a versioned get
            // 1 - check if the version is valid
            version_t lv = persistent_objectstore.getLatestVersion();
            if(ver > lv || ver < 0) {
                dbg_default_info("{}::{} failed with invalid version ( oid={}, ver = 0x{:x}, latest_version=0x{:x}), returning an invalid object.",
                                 typeid(*this).name(), __func__, oid, ver, lv);
                return inv_obj;
            }
            // 2 - return the object
            // TODO:
            // - avoid copy here!!!
            // - delta implementation needs to be improved!!! The existing
            //   implementation of Persistent<T> will Rebuild the whole object
            //   store on each versioned call. Fix this in persistent/
            //   Persistent.hpp
            try {
                return persistent_objectstore[ver]->objects.at(oid);
            } catch(std::out_of_range ex) {
                return inv_obj;
            }
        }
    }
    // @override IObjectStoreAPI::get_by_time
    virtual const Object get_by_time(const OID& oid, const uint64_t& ts_us) {
        dbg_default_debug("get_by_time, oid={}, ts={}.", oid, ts_us);
        const HLC hlc(ts_us,0ull); // generate a hybrid logical clock: TODO: do we have to use HLC????
        try{
            return persistent_objectstore.get(hlc)->objects.at(oid);
        } catch (const int64_t &ex) {
            dbg_default_warn("temporal query throws exception:0x{:x}. oid={}, ts={}", ex, oid, ts_us);
        } catch (...) {
            dbg_default_warn("temporal query throws unknown exception. oid={}, ts={}", oid, ts_us);
        }
        return inv_obj;
    }

    // DEFAULT_SERIALIZATION_SUPPORT(PersistentLoggedObjectStore,persistent_objectstore);

    DEFAULT_SERIALIZE(persistent_objectstore);

    static std::unique_ptr<PersistentLoggedObjectStore> from_bytes(mutils::DeserializationManager* dsm, char const*
                                                                                                                buf) {
// OPTION ONE to be tested
//        return std::make_unique<PersistentLoggedObjectStore>(
//                std::move(*mutils::from_bytes<decltype(persistent_objectstore)>(dsm, buf)));
// OPTION TWO
        auto ptr_to_persistent_objectstore = mutils::from_bytes<decltype(persistent_objectstore)>(dsm, buf);
        auto ptr_to_return = std::make_unique<PersistentLoggedObjectStore>(std::move(*ptr_to_persistent_objectstore));
        ptr_to_persistent_objectstore.release(); // to avoid double free.
        return ptr_to_return;
    }

    DEFAULT_DESERIALIZE_NOALLOC(PersistentLoggedObjectStore);

    void ensure_registered(mutils::DeserializationManager&) {}

    // constructors TODO: how to pass ObjectWatcher to Persistent? ==>
    PersistentLoggedObjectStore(persistent::PersistentRegistry* pr, IObjectStoreService& oss) : persistent_objectstore(
                                                                                            [&]() {
                                                                                                return std::make_unique<DeltaObjectStoreCore>(oss.getObjectWatcher());
                                                                                            },
                                                                                            nullptr,
                                                                                            pr,
                                                                                            mutils::DeserializationManager({&oss})) {}
    // Persistent<T> does not allow copy constructor.
    // PersistentLoggedObjectStore(Persistent<DeltaObjectStoreCore>& _persistent_objectstore) :
    //    persistent_objectstore(_persistent_objectstore) {}
    PersistentLoggedObjectStore(Persistent<DeltaObjectStoreCore>&& _persistent_objectstore) : persistent_objectstore(std::move(_persistent_objectstore)) {}
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
    return replicas;
}

class ObjectStoreService : public IObjectStoreService {
private:
    enum OSSMode {
        VOLATILE_UNLOGGED,
        VOLATILE_LOGGED,
        PERSISTENT_UNLOGGED,
        PERSISTENT_LOGGED
    };
    OSSMode mode;
    const ObjectWatcher& object_watcher;
    std::vector<node_id_t> replicas;
    const bool bReplica;
    const node_id_t myid;
    derecho::Group<VolatileUnloggedObjectStore, PersistentLoggedObjectStore> group;
    // TODO: WHY do I need "write_mutex"? I should be able to update the data
    // concurrently from multiple threads. Right?
    std::mutex write_mutex;

public:
    // constructor
    ObjectStoreService(const ObjectWatcher& ow) : mode(
                                                          derecho::getConfBoolean(CONF_OBJECTSTORE_PERSISTED) ? (derecho::getConfBoolean(CONF_OBJECTSTORE_LOGGED) ? PERSISTENT_LOGGED : PERSISTENT_UNLOGGED) : (derecho::getConfBoolean(CONF_OBJECTSTORE_LOGGED) ? VOLATILE_LOGGED : VOLATILE_UNLOGGED)),
                                                  object_watcher(ow),
                                                  replicas(parseReplicaList(derecho::getConfString(CONF_OBJECTSTORE_REPLICAS))),
                                                  bReplica(std::find(replicas.begin(), replicas.end(),
                                                                     derecho::getConfUInt64(CONF_DERECHO_LOCAL_ID))
                                                           != replicas.end()),
                                                  myid(derecho::getConfUInt64(CONF_DERECHO_LOCAL_ID)),
                                                  group(
                                                          {},  // callback set
                                                          // derecho::SubgroupInfo
                                                          {
                                                                  [this](const std::vector<std::type_index>& subgroup_type_order,
                                                                         const std::unique_ptr<derecho::View>& prev_view,
                                                                         derecho::View& curr_view) {
                                                                      derecho::subgroup_allocation_map_t subgroup_allocation;
                                                                      for(const auto& subgroup_type : subgroup_type_order) {
                                                                          if(subgroup_type == std::type_index(typeid(VolatileUnloggedObjectStore)) || subgroup_type == std::type_index(typeid(PersistentLoggedObjectStore))) {
                                                                              std::vector<node_id_t> active_replicas;
                                                                              for(uint32_t i = 0; i < curr_view.members.size(); i++) {
                                                                                  const node_id_t id = curr_view.members[i];
                                                                                  if(!curr_view.failed[i] && std::find(replicas.begin(), replicas.end(), id) != replicas.end()) {
                                                                                      active_replicas.push_back(id);
                                                                                  }
                                                                              }
                                                                              if(active_replicas.size() < derecho::getConfUInt32(CONF_OBJECTSTORE_MIN_REPLICATION_FACTOR)) {
                                                                                  throw derecho::subgroup_provisioning_exception();
                                                                              }

                                                                              derecho::subgroup_shard_layout_t subgroup_vector(1);
                                                                              subgroup_vector[0].emplace_back(curr_view.make_subview(active_replicas));
                                                                              curr_view.next_unassigned_rank += active_replicas.size();
                                                                              subgroup_allocation.emplace(subgroup_type, std::move(subgroup_vector));
                                                                          } else {
                                                                              subgroup_allocation.emplace(subgroup_type, derecho::subgroup_shard_layout_t{});
                                                                          }
                                                                      }
                                                                      return subgroup_allocation;
                                                                  }},
                                                          this,
                                                          std::vector<derecho::view_upcall_t>{},  // view up-calls
                                                          // factories ...
                                                          [this](persistent::PersistentRegistry*) { return std::make_unique<VolatileUnloggedObjectStore>(object_watcher); },
                                                          [this](persistent::PersistentRegistry* pr) { return std::make_unique<PersistentLoggedObjectStore>(pr, *this); }) {
        // Unimplemented yet:
        if(mode == PERSISTENT_UNLOGGED || mode == VOLATILE_LOGGED) {
            // log it
            dbg_default_error("ObjectStoreService mode {} is not supported yet.", mode);
            throw derecho::derecho_exception("Unimplmented ObjectStoreService mode: persistent_unlogged/volatile_logged.");
        }
    }

    virtual const bool isReplica() {
        return bReplica;
    }

    template <typename T>
    derecho::rpc::QueryResults<std::tuple<version_t,uint64_t>> _aio_put(const Object& object, const bool& force_client) {
        std::lock_guard<std::mutex> guard(write_mutex);
        if(bReplica && !force_client) {
            // replica server can do ordered send
            derecho::Replicated<T>& os_rpc_handle = group.template get_subgroup<T>();
            return std::move(os_rpc_handle.template ordered_send<RPC_NAME(orderedPut)>(object));
        } else {
            // send request to a static mapped replica. Use random mapping for load-balance?
            node_id_t target = replicas[myid % replicas.size()];
            derecho::ExternalCaller<T>& os_p2p_handle = group.get_nonmember_subgroup<T>();
            return std::move(os_p2p_handle.template p2p_send<RPC_NAME(put)>(target, object));
        }
    }

    template <typename T>
    std::tuple<version_t,uint64_t> _bio_put(const Object& object, const bool& force_client) {
        derecho::rpc::QueryResults<std::tuple<version_t,uint64_t>> results = this->template _aio_put<T>(object, force_client);
        decltype(results)::ReplyMap& replies = results.get();

        std::tuple<version_t,uint64_t> vRet(INVALID_VERSION,0);
        for(auto& reply_pair : replies) {
            vRet = reply_pair.second.get();
        }
        return vRet;
    }

    // blocking put
    virtual std::tuple<version_t,uint64_t> bio_put(const Object& object, const bool& force_client) {
        dbg_default_debug("bio_put object id={}, mode={}, force_client={}", object.oid, mode, force_client);
        std::tuple<version_t,uint64_t> vRet(INVALID_VERSION,0);
        switch(this->mode) {
            case VOLATILE_UNLOGGED:
                vRet = this->template _bio_put<VolatileUnloggedObjectStore>(object, force_client);
                break;
            case PERSISTENT_LOGGED:
                vRet = this->template _bio_put<PersistentLoggedObjectStore>(object, force_client);
                break;
            default:
                dbg_default_error("Cannot execute 'put' in unsupported mode {}.", mode);
        }
        return vRet;
    }

    // non-blocking put
    virtual derecho::rpc::QueryResults<std::tuple<version_t,uint64_t>> aio_put(const Object& object, const bool& force_client) {
        dbg_default_debug("aio_put object id={}, mode={}, force_client={}", object.oid, mode, force_client);
        switch(this->mode) {
            case VOLATILE_UNLOGGED:
                return this->template _aio_put<VolatileUnloggedObjectStore>(object, force_client);
            case PERSISTENT_LOGGED:
                return this->template _aio_put<PersistentLoggedObjectStore>(object, force_client);
            default:
                dbg_default_error("Cannot execute 'put' in unsupported mode {}.", mode);
                throw derecho::derecho_exception("Cannot execute 'put' in unsupported mode");
        }
    }

    template <typename T>
    derecho::rpc::QueryResults<std::tuple<version_t,uint64_t>> _aio_remove(const OID& oid, const bool& force_client) {
        std::lock_guard<std::mutex> guard(write_mutex);
        if(bReplica && !force_client) {
            // replica server can do ordered send
            derecho::Replicated<T>& os_rpc_handle = group.template get_subgroup<T>();
            return std::move(os_rpc_handle.template ordered_send<RPC_NAME(orderedRemove)>(oid));
        } else {
            // send request to a static mapped replica. Use random mapping for load-balance?
            node_id_t target = replicas[myid % replicas.size()];
            derecho::ExternalCaller<T>& os_p2p_handle = group.get_nonmember_subgroup<T>();
            return std::move(os_p2p_handle.template p2p_send<RPC_NAME(remove)>(target, oid));
        }
    }

    template <typename T>
    std::tuple<version_t,uint64_t> _bio_remove(const OID& oid, const bool& force_client) {
        derecho::rpc::QueryResults<std::tuple<version_t,uint64_t>> results = this->template _aio_remove<T>(oid, force_client);
        decltype(results)::ReplyMap& replies = results.get();

        std::tuple<version_t,uint64_t> vRet(INVALID_VERSION,0);
        for(auto& reply_pair : replies) {
            vRet = reply_pair.second.get();
        }
        return vRet;
    }

    // blocking remove
    virtual std::tuple<version_t,uint64_t> bio_remove(const OID& oid, const bool& force_client) {
        dbg_default_debug("bio_remove object id={}, mode={}, force_client={}", oid, mode, force_client);
        switch(this->mode) {
            case VOLATILE_UNLOGGED:
                return this->template _bio_remove<VolatileUnloggedObjectStore>(oid, force_client);
            case PERSISTENT_LOGGED:
                return this->template _bio_remove<PersistentLoggedObjectStore>(oid, force_client);
            default:
                dbg_default_error("Cannot execute 'remove' in unsupported mode {}.", mode);
                throw derecho::derecho_exception("Cannot execute 'remove' in unsupported mode {}.'");
        }
    }

    // non-blocking remove
    virtual derecho::rpc::QueryResults<std::tuple<version_t,uint64_t>> aio_remove(const OID& oid, const bool& force_client) {
        dbg_default_debug("aio_remove object id={}, mode={}, force_client={}", oid, mode, force_client);
        switch(this->mode) {
            case VOLATILE_UNLOGGED:
                return this->template _aio_remove<VolatileUnloggedObjectStore>(oid, force_client);
            case PERSISTENT_LOGGED:
                return this->template _aio_remove<PersistentLoggedObjectStore>(oid, force_client);
            default:
                dbg_default_error("Cannot execute 'remove' in unsupported mode {}.", mode);
                throw derecho::derecho_exception("Cannot execute 'remove' in unsupported mode {}.'");
        }
    }

    // get
    template <typename T>
    derecho::rpc::QueryResults<const Object> _aio_get(const OID& oid, const version_t& ver, const bool& force_client) {
        std::lock_guard<std::mutex> guard(write_mutex);
        if(bReplica && !force_client) {
            derecho::Replicated<T>& os_rpc_handle = group.template get_subgroup<T>();
            if(ver == INVALID_VERSION) {
                // replica server can do ordered send
                return std::move(os_rpc_handle.template ordered_send<RPC_NAME(orderedGet)>(oid));
            } else {
                // send a local p2p send/query to access history versions
                return std::move(os_rpc_handle.template p2p_send<RPC_NAME(get)>(myid, oid, ver));
            }
        } else {
            // Send request to a static mapped replica. Use random mapping for load-balance?
            node_id_t target = replicas[myid % replicas.size()];
            derecho::ExternalCaller<T>& os_p2p_handle = group.template get_nonmember_subgroup<T>();
            return std::move(os_p2p_handle.template p2p_send<RPC_NAME(get)>(target, oid, ver));
        }
    }

    template <typename T>
    derecho::rpc::QueryResults<const Object> _aio_get(const OID& oid, const uint64_t& ts_us) {
        std::lock_guard<std::mutex> guard(write_mutex);
        if (bReplica) {
            // send to myself.
            derecho::Replicated<T>& os_rpc_handle = group.template get_subgroup<T>();
            return std::move(os_rpc_handle.template p2p_send<RPC_NAME(get_by_time)>(myid, oid, ts_us));
        } else {
            // Send request to a static mapped replica. Use random mapping for load-balance?
            node_id_t target = replicas[myid % replicas.size()];
            derecho::ExternalCaller<T>& os_p2p_handle = group.template get_nonmember_subgroup<T>();
            return std::move(os_p2p_handle.template p2p_send<RPC_NAME(get_by_time)>(target, oid, ts_us));
        }
    }

    template <typename T>
    Object _bio_get(const OID& oid, const version_t& ver, const bool& force_client) {
        derecho::rpc::QueryResults<const Object> results = this->template _aio_get<T>(oid, ver, force_client);
        decltype(results)::ReplyMap& replies = results.get();
        // should we check reply consistency?
        return replies.begin()->second.get();
    }

    template <typename T>
    Object _bio_get(const OID& oid, const uint64_t& ts_us) {
        derecho::rpc::QueryResults<const Object> results = this->template _aio_get<T>(oid, ts_us);
        decltype(results)::ReplyMap& replies = results.get();
        return replies.begin()->second.get();
    }

    virtual Object bio_get(const OID& oid, const version_t& ver, const bool& force_client) {
        dbg_default_debug("bio_get object id={}, ver={}, mode={}, force_client={}", oid, ver, mode, force_client);
        switch(this->mode) {
            case VOLATILE_UNLOGGED:
                return this->template _bio_get<VolatileUnloggedObjectStore>(oid, ver, force_client);
            case PERSISTENT_LOGGED:
                return this->template _bio_get<PersistentLoggedObjectStore>(oid, ver, force_client);
            default:
                dbg_default_error("Cannot execute 'get' in unsupported mode {}.", mode);
                throw derecho::derecho_exception("Cannot execute 'get' in unsupported mode {}.'");
        }
    }

    virtual Object bio_get(const OID& oid, const uint64_t& ts_us) {
        dbg_default_debug("bio_get object id={}, ts_us={}.", oid, ts_us);
        switch(this->mode) {
            case VOLATILE_UNLOGGED:
                return this->template _bio_get<VolatileUnloggedObjectStore>(oid, ts_us);
            case PERSISTENT_LOGGED:
                return this->template _bio_get<PersistentLoggedObjectStore>(oid, ts_us);
            default:
                dbg_default_error("Cannot execute 'get' in unsupported mode {}.", mode);
                throw derecho::derecho_exception("Cannot execute 'get' in unsupported mode {}.'");
        }
    }

    virtual derecho::rpc::QueryResults<const Object> aio_get(const OID& oid, const version_t& ver, const bool& force_client) {
        dbg_default_debug("aio_get object id={}, ver={}, mode={}, force_client={}", oid, ver, mode, force_client);
        switch(this->mode) {
            case VOLATILE_UNLOGGED:
                return this->template _aio_get<VolatileUnloggedObjectStore>(oid, ver, force_client);
            case PERSISTENT_LOGGED:
                return this->template _aio_get<PersistentLoggedObjectStore>(oid, ver, force_client);
            default:
                dbg_default_error("Cannot execute 'get' in unsupported mode {}.", mode);
                throw derecho::derecho_exception("Cannot execute 'get' in unsupported mode {}.'");
        }
    }

    virtual derecho::rpc::QueryResults<const Object> aio_get(const OID& oid, const uint64_t& ts_us) {
        dbg_default_debug("aio_get object id={}, ts_us={}.", oid, ts_us);
        switch(this->mode) {
            case VOLATILE_UNLOGGED:
                return this->template _aio_get<VolatileUnloggedObjectStore>(oid, ts_us);
            case PERSISTENT_LOGGED:
                return this->template _aio_get<PersistentLoggedObjectStore>(oid, ts_us);
            default:
                dbg_default_error("Cannot execute 'get' in unsupported mode {}.", mode);
                throw derecho::derecho_exception("Cannot execute 'get' in unsupported mode {}.'");
        }
    }

    virtual void leave(bool group_shutdown) {
        if(group_shutdown) {
            group.barrier_sync();
        }
        group.leave(group_shutdown);
    }

    virtual const ObjectWatcher& getObjectWatcher() {
        return this->object_watcher;
    }

    // get singleton
    static IObjectStoreService& get(int argc, char** argv, const ObjectWatcher& ow = {});
};

// The singleton unique pointer
std::shared_ptr<IObjectStoreService> IObjectStoreService::singleton;

// get the singleton
// NOTE: caller only get access to this member object. The ownership of this
// object is NOT transferred.
IObjectStoreService& IObjectStoreService::getObjectStoreService(int argc, char** argv, const ObjectWatcher& ow) {
    if(IObjectStoreService::singleton.get() == nullptr) {
        // step 1: initialize the configuration
        derecho::Conf::initialize(argc, argv);
        // step 2: create the group resources
        IObjectStoreService::singleton = std::make_shared<ObjectStoreService>(ow);
    }

    return *IObjectStoreService::singleton.get();
};

}  // namespace objectstore
