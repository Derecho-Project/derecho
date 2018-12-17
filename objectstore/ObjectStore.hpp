#ifndef OBJECTSTORE_HPP
#define OBJECTSTORE_HPP

#include <errno.h>
#include <iostream>
#include <map>

#include "Object.hpp"
#include "ObjectStoreException.hpp"

namespace objectstore {

#define CONF_OBJECTSTORE_REPLICAS "OBJECTSTORE/replicas"

class ObjectStore : public mutils::ByteRepresentable {
public:
    std::map<OID, Object> objects;
    const Object inv_obj;

    virtual void put(const Object& object) {
        this->objects.erase(object.oid);
        this->objects.emplace(object.oid, object);  // copy constructor
    }
    virtual void remove(const OID& oid) {
        this->objects.erase(oid);
    }
    virtual const Object get(const OID& oid) {
        if(objects.find(oid)!=objects.end()) {
            return objects.at(oid);
        } else {
            return this->inv_obj;
        }
    }

    REGISTER_RPC_FUNCTIONS(ObjectStore, put, remove, get);
    DEFAULT_SERIALIZATION_SUPPORT(ObjectStore, objects);

    // constructors
    ObjectStore() {}
    ObjectStore(std::map<OID, Object>& _objects) : objects(_objects){}
};

// Enable the Delta feature
class DeltaObjectStore : public ObjectStore,
                         public IDeltaSupport {
    enum _OPID {
        PUT,
        REMOVE
    };
#define DEFAULT_DELTA_BUFFER_CAPACITY (4096)

    struct {
        size_t capacity;
        size_t len;
        char* buffer;
        inline void setOpid(_OPID opid) {
            assert(buffer!=nullptr);
            assert(capacity>=sizeof(uint32_t));
            *(_OPID*)buffer = opid;
        }
        inline void setDataLen(const size_t& dlen) {
            assert(capacity>=(dlen+sizeof(uint32_t)));
            this->len = dlen + sizeof(uint32_t);
        }
        inline char* dataPtr() {
            assert(buffer!=nullptr);
            assert(capacity>sizeof(uint32_t));
            return buffer + sizeof(uint32_t);
        }
        inline void calibrate(const size_t& dlen) {
            size_t new_cap = dlen + sizeof(uint32_t);
            if (this->capacity >= new_cap) {
                return;
            }
            // calculate new capacity
            int width = sizeof(size_t) << 3;
            int right_shift_bits = 1;
            new_cap --;
            while(right_shift_bits < width) {
                new_cap|= new_cap >> right_shift_bits;
                right_shift_bits = right_shift_bits << 1;
            }
            new_cap ++;
            // resize
            this->buffer = (char*)realloc(buffer, new_cap);
            if(this->buffer == nullptr) {
                std::cerr << __FILE__ << ":" << __LINE__ << " Fail to allocate delta buffer." << std::endl << std::flush;
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
            if(this->capacity > 0){
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
        df(this->delta.buffer,this->delta.len);
        this->delta.clean();
    }
    virtual void applyDelta(char const* const delta) {
        const char * data = (delta + sizeof(const uint32_t));
        switch(*(const uint32_t*)delta) {
        case PUT:
            ObjectStore::put(*mutils::from_bytes<Object>(nullptr,data));
            break;
        case REMOVE:
            ObjectStore::remove(*(const OID*)data);
            break;
        default:
            std::cerr << __FILE__ << ":" << __LINE__ << " " << std::endl;
        };
    }
    //TODO: Can we get the serialized operation representation from Derecho?
    virtual void put(const Object& object) {
        // create delta.
        assert(this->delta.isEmpty());
        this->delta.calibrate(object.bytes_size());
        object.to_bytes(this->delta.dataPtr());
        this->delta.setDataLen(object.bytes_size());
        this->delta.setOpid(PUT);
        // put
        ObjectStore::put(object);
    }
    // TODO: Can we get the serialized operation representation from Derecho?
    virtual void remove(const OID& oid) {
        // create delta
        assert(this->delta.isEmpty());
        this->delta.calibrate(sizeof(OID));
        *(OID*)this->delta.dataPtr() = oid;
        this->delta.setDataLen(sizeof(OID));
        this->delta.setOpid(REMOVE);
        // remove
        ObjectStore::remove(oid);
    }
    
    // Not going to register them as RPC functions because DeltaObjectStore
    // works with PersistedObjectStore instead of the type for Replicated<T>.
    // REGISTER_RPC_FUNCTIONS(ObjectStore, put, remove, get);
    
    DEFAULT_SERIALIZATION_SUPPORT(DeltaObjectStore, objects);

    // constructor
    DeltaObjectStore() : ObjectStore() {
        initialize_delta();
    }
    DeltaObjectStore(std::map<OID, Object>& _objects) : ObjectStore(_objects){
        initialize_delta();
    }
    virtual ~DeltaObjectStore() {
        if(delta.buffer != nullptr) {
            free(delta.buffer);
        }
    }
};

}  // namespace objectstore
#endif//OBJECTSTORE_HPP
