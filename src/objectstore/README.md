# dPods: The Derecho Plain-Old-Data Store

dPods is a high-performant, replicated, and fault-tolerant objectstore service built upon the core functionalities of the Derecho library. We designed dPods with the following objectives in mind:
1. Temporal Queries: We log the time of occurence for each operation.  In versioned mode, this allows queries against any stable time in the past.  Queries with the same time that access a set of distinct objects or distinct replicas will be satisfied of a consistent cut across the history. **NOTE: the time-query API has not yet been added.**
2. Zero-copy: We use RDMA to move data around for replication or between the server and clients. We try our best to avoid unecessary memory copies, which eat up performance when handling large data objects. **We are working on a slab allocator to allow the application to manage the objects in *RDMA-ready* memory regions, meaning the objects are accessible to RDMA devices and ready to be transferred to remote nodes without any local memory copy.  This should help users design zero-copy objects.**

There are three kinds of members (processes) in a dPods Store deployment: The *replicas*, *clients*, and *external clients*. *replicas* store the data and keep a log of all the operations. *clients* put/get/remove the object by issuing Derecho P2P calls to *replicas*. Both *replicas* and *clients* are members of the top level derecho group, and *replicas* are also me,bers of the subgroup managing dPods data and carrying out operations on the data. *External clients* are nodes that talk to *replicas* through some relay services (e.g. RESTful API). The *external clients* focus on language compatibility rather than performance. In the [current version](f379c6eef813c073c28b803c99ab441ea4002975), we haven't provided a demo implementation illustrating this style of using REST from an external client.

dPODS also supports cput (conditional put). This allows the caller to do a put if the object still has some expected version number, but fails if the version has changed, enabling lock-free updates.

## dPods Getting Started

To access the dPods service, a process need either start a dPods node as *replica* or *client*, or access those service as an *external client*. In this document, we only talk about the former method. We plan to add the latter soon.

A dPods *replica* or *client* node needs to start the service and get a handle to it as following:
```cpp
    // oss - objectstore service
    auto& oss = objectstore::IObjectStoreService::getObjectStoreService(argc, argv,
        [&](const objectstore::OID& oid, const objectstore::Object& object){
            std::cout << "watcher: " << oid << "->" << object << std::endl;
        });
```
The `argc` and `argv` are command line arguments carrying derecho configurations to be passed to the derecho core. The third argument is a callable object watching on the updates. For example, in a pub/sub system, a consumer is hoping to be notified of incoming data, which can be handled here. Please note that only the replica nodes will be notified. The client nodes can register a watch but to be ignored.

Careful readers may wondering what determines whether a node is a replica,as opposed to a client. We defined this in derecho configuration file. dPods rely on the new options in the `[OBJECTSTORE]` section.
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

Please check the example application code in [test.cpp](https://github.com/Derecho-Project/derecho/blob/master/objectstore/test.cpp).

## The dPods API
The dPods API is shown in [ObjectStore.hpp](https://github.com/Derecho-Project/derecho/blob/master/objectstore/ObjectStore.hpp). TODO: explaining the normal put/get/remove as well as temporal query, conditional-put.
## TODO: More on the dPods versions

