# dPods: The Derecho Plain-Old-Data Store

dPods is a high-performant, replicated, and fault-tolerant objectstore service built upon the core functionalities of the Derecho library. We design dPods with the following objectives in mind:
1. Temporal Queries: We logged the operating time of each operation to allow go back to any consistent time cut of the history. **Currently, the time-query API is to be added yet.**
2. Zero-copy: We use RDMA to move data around for replication or between the server and clients. We try best to avoid unecessary memory copy which eats up performance when handling large data objects. **We are working on a slab allocator to allow the application to manage the objects in *RDMA-ready* memory regions meaning the objects are accessible to RDMA devices and ready to be transferred to remote nodes without any local memory copy.**

There are three kinds of nodes in dPods Store: The *replicas*, *clients*, and *external clients*. *replicas* store the data and keep a log of all the operations. *clients* put/get/remove the object by issuing Derecho P2P calls to *replicas*. Both *replicas* and *clients* are in the top level derecho group and *replicas* are in the subgroup managing all dPods data and operations. *External clients* are nodes that talk to *replicas* through some relay services (e.g. RESTful API). The *external clients* focus on language compatibility rather than performance. In the [current version](f379c6eef813c073c28b803c99ab441ea4002975), we didn't provide an implementation of an external client.

## dPods Getting Started
The dPods API is shown in [ObjectStore.hpp](https://github.com/Derecho-Project/derecho-unified/blob/master/objectstore/ObjectStore.hpp). A dPods *replica* or *client* node needs to start the service and get a handle to it as following:
```cpp
    // oss - objectstore service
    auto& oss = objectstore::IObjectStoreService::getObjectStoreService(argc, argv,
        [&](const objectstore::OID& oid, const objectstore::Object& object){
            std::cout << "watcher: " << oid << "->" << object << std::endl;
        });
```
The `argc` and `argv` are command line arguments carrying derecho configurations to be passed to the derecho core. The third argument is a callable object watching on the updates. For example, in a pub/sub system, a consumer is hoping to be notified of incoming data, which can be handled here. Please note that only the replica nodes will be notified. The client nodes can register a watch but to be ignored.

Careful readers may wondering what determines if a node is replica and client. We defined this in derecho configuration file. dPods rely on the new options in the `[OBJECTSTORE]` section.
```
[OBJECTSTORE]
# 'min_replication_factor' is the minimum number of replicas required to run
# an objectstore. 
min_replication_factor = 2
# 'replicas' is a list of nodes (ids) which are in the replica subgroup. The
# node IDs are separated by comma(,). Hyphen is allowed for a range.
# Examples:
# replicas = 0,1,2
# replicas = 0-2,9-10,12,30,100-105
# The number of replica must be greater than 'min_replication_factor'.
replicas = 0-2
# 'persisted' controls the persistence of the ObjectStore. Set it to 'true' if
# the data need to survive system restarts or failure. 
persisted = false
# 'logged' controls if the history is maintained. Set it to  'true' if access 
# to history is required. NOTE: 'logged' only works with 'persisted' = true. 
logged = false
```
Notice that the node id is defined by the `local_id` in '[DERECHO]' section.

Once we get the handle to the ObjectStore service, we can put and get the objects in the store:
```cpp
    // put
    std::istringstream ss(args);
    std::string oid,odata;
    ss >> oid >> odata;
    objectstore::Object object(std::stol(oid),odata.c_str(),odata.length()+1);
    try{
        oss.put(object);
    } catch (...) {
        return false;
    }
    
    // get
    try{
        objectstore::Object obj = oss.get(std::stol(args));
        std::cout << obj << std::endl;
    } catch (...) {
        return false;
    }
    
    // remove
    try{
        return oss.remove(std::stol(args));
    } catch (...) {
        return false;
    }
```

On application shutdown, the application can close the local store service by calling the `leave` API:
```cpp
    oss.leave();
```

Please check the example application code at [test.cpp](https://github.com/Derecho-Project/derecho-unified/blob/master/objectstore/test.cpp).
