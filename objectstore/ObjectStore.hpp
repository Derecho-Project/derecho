#ifndef OBJECTSTORE_HPP
#define OBJECTSTORE_HPP

#include <optional>

#include "Object.hpp"
#include "ObjectStoreException.hpp"

namespace objectstore {
// if object is valid, this is a PUT operation; otherwise, a REMOVE operation.
using ObjectWatcher = std::function<void(const OID&,const Object&)>;

// The core API. See `test.cpp` for how to use it.
class IObjectStoreService {
private:
    static std::unique_ptr<IObjectStoreService> singleton;
public:
    virtual const bool isReplica() = 0;
    virtual void put(const Object& object) = 0;
    virtual bool remove(const OID& oid) = 0;
    virtual Object get(const OID& oid) = 0;
    virtual void leave() = 0; // leave gracefully
    virtual const ObjectWatcher& getObjectWatcher() = 0;

    // get singleton
    static IObjectStoreService& get(int argc, char ** argv, const ObjectWatcher& ow = {});
};

}  // namespace objectstore
#endif//OBJECTSTORE_HPP
