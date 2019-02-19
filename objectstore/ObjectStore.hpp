#ifndef OBJECTSTORE_HPP
#define OBJECTSTORE_HPP

#include <optional>

#include "Object.hpp"

namespace objectstore {
// if object is valid, this is a PUT operation; otherwise, a REMOVE operation.
using ObjectWatcher = std::function<void(const OID&,const Object&)>;

// The core API. See `test.cpp` for how to use it.
class IObjectStoreService : public derecho::IDeserializationContext {
private:
    static std::unique_ptr<IObjectStoreService> singleton;
public:
    virtual const bool isReplica() = 0;
    // blocking operations
    virtual bool bio_put(const Object& object, bool use_replica_api=false) = 0;
    virtual bool bio_remove(const OID& oid, bool use_replica_api=false) = 0;
    virtual Object bio_get(const OID& oid, bool use_replica_api=false) = 0;
    // non blocking operations
    /*** TODO
    virtual derecho::rpc::QueryResults<bool> aio_put(const OBject& object, bool use_replica_api) = 0;
    virtual derecho::rpc::QueryResults<bool> bio_remove(const OID& oid, bool use_replica_api) = 0;
    virtual derecho::rpc::QueryResults<Object> bio_get(const OID& oid, bool use_replica_api) = 0;
     ***/

    virtual void leave() = 0; // leave gracefully
    virtual const ObjectWatcher& getObjectWatcher() = 0;

    // get singleton
    static IObjectStoreService& getObjectStoreService(int argc, char ** argv, const ObjectWatcher& ow = {});
};

}  // namespace objectstore
#endif//OBJECTSTORE_HPP
