Full source documentation can be found at https://derecho-project.github.io/.

# Derecho
This is the main repository for the Derecho project. It unifies the RDMC, SST, and Derecho modules under a single, easy-to-use repository. 

## Organization
The code for this project is split into modules that interact only through each others' public interfaces. Each module resides in a directory with its own name, which is why the repository's root directory is mostly empty. External dependencies are located in the `third_party` directory, and will be pointers to Git submodules whenever possible to reduce code duplication. 

## Installation
Derecho is a library that helps you build replicated, fault-tolerant services in a datacenter with RDMA networking. Here's how to start using it in your projects.

### Prerequisites
* Linux (other operating systems don't currently support the RDMA features we use)
* A C++ compiler supporting C++14: GCC 5.4+ or Clang 3.5+
* The following system libraries: `rdmacm` (packaged for Ubuntu as `librdmacm-dev 1.0.21`), and `ibverbs` (packaged for Ubuntu as `libibverbs-dev 1.1.8`).
* libboost-dev, libboost-system and libboost-system-dev
* CMake 2.8.1 or newer, if you want to use the bundled build scripts

### Getting Started
Since this repository uses Git submodules to refer to some bundled dependencies, a simple `git clone` will not actually download all the code. To download a complete copy of the project, run

    git clone --recursive https://github.com/Derecho-Project/derecho-unified.git

Once cloning is complete, to compile the code, `cd` into the `derecho-unified` directory and run:
* mkdir Release
* cd Release
* `cmake -DCMAKE_BUILD_TYPE=Release ..`
* `make`

This will place the binaries and libraries in the sub-dierectories of `Release`.
The other build type is Debug. If you need to build the Debug version, replace Release by Debug in the above instructions. We explicitly disable in-source build, so running `cmake .` in `derecho-unified` will not work.

To add your own executable (that uses Derecho) to the build system, simply add an executable target to CMakeLists.txt with `derecho` as a "linked library." You can do this either in the top-level CMakeLists.txt or in the CMakeLists.txt inside the "derecho" directory. It will look something like this:

    add_executable(my_project_main my_project_main.cpp)
	target_link_libraries(my_project_main derecho)

To use Derecho in your code, you simply need to include the header `derecho/derecho.h` in your \*.h or \*.cpp files:
   
```cpp
#include "derecho/derecho.h"
```

### Testing (and some hidden gotchas)
There are many experiment files in derecho/experiments that can be run to test the installation. To be able to run the tests, you need a minimum of two machines connected by RDMA. The RDMA devices on the machines should be active. In addition, you need to run the following commands to install and load the required kernel modules:
* sudo apt-get install rdmacm-utils rdmacm-utils librdmacm-dev libibverbs-dev ibutils libmlx4-1
sudo apt-get install infiniband-diags libmthca-dev opensm ibverbs-utils libibverbs1 libibcm1 libibcommon1
* sudo modprobe -a rdma_cm ib_uverbs ib_umad ib_ipoib mlx4_ib iw_cxgb3 iw_cxgb4 iw_nes iw_c2 ib_mthca
Depending on your system, some of the modules might not load which is fine.

RDMA requires memory pinning of memory regions shared with other nodes. There's a limit on the maximum amount of memory a process can pin, typically 64 KB, which Derecho easily exceeds. Therefore, you need to set this to unlimited. To do so, append the following lines to /etc/security/limits.conf:
* username hard memlock unlimited
* username soft memlock unlimited

where username is your linux username. A * in place of username will set this limit to unlimited for all users. Log out and back in again for the limits to reapply. You can test this by verifying that `ulimit -l` outputs `unlimited` in bash.

We currently do not have a systematic way of asking the user for RDMA device configuration. So, we pick an arbitrary RDMA device in functions `resources_create` in `sst/verbs.cpp` and `verbs_initialize` in `rdmc/verbs_helper.cpp`. Look for the loop `for(i = 1; i < num_devices; i++)`. If you have a single RDMA device, most likely you want to start `i` from `0`. If you have multiple devices, you want to start `i` from the order (zero-based) of the device you want to use in the list of devices obtained by running `ibv_devices` in bash.

To test if one of the experiments is working correctly, go to two of your machines (nodes), `cd` to `Release/derecho/experiments` and run `./derecho_bw_test 0 10000 15 1000 1 0` on both. The programs will ask for input.
The input to the first node is:
* 0 (it's node id)
* 2 (number of nodes for the experiment)
* ip-addr of node 1
* ip-addr of node 2
Replace the node id 0 by 1 for the input to the second node.
As a confirmation that the experiment finished successfully, the first node will write a log of the result in the file `data_derecho_bw` something along the lines of `12 0 10000 15 1000 1 0 0.37282
`. Full experiment details including explanation of the arguments, results and methodology is explained in the source documentation at the link given earlier.

## Using Derecho
The file `typed_subgroup_test.cpp` within derecho/experiments shows a complete working example of a program that sets up and uses a Derecho group with several Replicated Objects. You can read through that file if you prefer to learn by example, or read on for an explanation of how to use various features of Derecho.

### Replicated Objects
One of the core building blocks of Derecho is the concept of a Replicated Object. This provides a simple way for you to define state that is replicated among several machines and a set of RPC functions that operate on that state.

A Replicated Object is any class that (1) is serializable with the mutils-serialization framework and (2) implements a static method called `register_functions()`. The [mutils-serialization](https://github.com/mpmilano/mutils-serialization) library should have more documentation on making objects serializable, but the most straightforward way is to inherit `mutils::ByteRepresentable`, use the macro `DEFAULT_SERIALIZATION_SUPPORT`, and write an element-by-element constructor. The `register_functions()` method is how your class specifies to Derecho which of its methods should be converted to RPC functions and what their numeric "function tags" should be. It should return a `std::tuple` containing a pointer to each RPC-callable method, wrapped in the template function `derecho::rpc::tag`. The template parameter to `tag` is the integer that will be used to identify RPC calls to the corresponding method pointer, so we recommend you use a named constant that has the same name as the method. Here is an example of a Replicated Object declaration:

```cpp
class Cache : public mutils::ByteRepresentable {
    std::map<std::string, std::string> cache_map;

public:
    void put(const std::string& key, const std::string& value);
    std::string get(const std::string& key); 
    bool contains(const std::string& key);
    bool invalidate(const std::string& key);
    enum Functions { PUT,
                     GET,
                     CONTAINS,
                     INVALIDATE };

    static auto register_functions() {
        return std::make_tuple(derecho::rpc::tag<PUT>(&Cache::put),
                               derecho::rpc::tag<GET>(&Cache::get),
                               derecho::rpc::tag<CONTAINS>(&Cache::contains),
                               derecho::rpc::tag<INVALIDATE>(&Cache::invalidate));
    }

    Cache() : cache_map() {}
    Cache(const std::map<std::string, std::string>& cache_map) : cache_map(cache_map) {}

    DEFAULT_SERIALIZATION_SUPPORT(Cache, cache_map);
};
```

This object has one field, `cache_map`, so the DEFAULT_SERIALIZATION_SUPPORT macro is called with the name of the class and the name of this field. The second constructor, which initializes the field from a parameter of the same type, is required for serialization support. The object has four RPC methods, `put`, `get`, `contains`, and `invalidate`, and `register_functions()` tags them with enum constants that have similar names.

### Groups and Subgroups

Derecho organizes nodes (machines or processes in a system) into Groups, which can then be divided into subgroups and shards. Any member of a Group can communicate with any other member, and all run the same group-management service that handles failures and accepts new members. Subgroups, which are any subset of the nodes in a Group, correspond to Replicated Objects; each subgroup replicates the state of a Replicated Object and any member of the subgroup can handle RPC calls on that object. Shards are disjoint subsets of a subgroup that each maintain their own state, so one subgroup can replicate multiple instances of the same type of Replicated Object. A Group must be statically configured with the types of Replicated Objects it can support, but the number of subgroups and their exact membership can change at runtime according to functions that you provide. 

Note that more than one subgroup can use the same type of Replicated Object, so there can be multiple independent instances of a Replicated Object in a Group even if those subgroups are not sharded. A subgroup is usually identified by the type of Replicated Object it implements and an integral index number specifying which subgroup of that type it is. 

To start using Derecho, a process must either start or join a Group by constructing an instance of `derecho::Group`, which then provides the interface for interacting with other nodes in the Group. (The difference between starting and joining a group is simply a matter of calling a different constructor). A `derecho::Group` expects a set of variadic template parameters representing the types of Replicated Objects that it can support in its subgroups. For example, this declaration is a pointer to a Group object that can have subgroups of type LoadBalancer, Cache, and Storage:
```cpp
std::unique_ptr<derecho::Group<LoadBalancer, Cache, Storage>> group;
```

#### Defining Subgroup Membership

In order to start or join a Group, all members (including processes that join later) must define functions that provide the membership (as a subset of the current View) for each subgroup and shard, given as input the current View. These functions are organized in a map keyed by `std::type_index` in struct SubgroupInfo, where the key for a subgroup-membership function is the type of Replicated Object associated with that subgroup. Since there can be more than one subgroup that implements the same Replicated Object type (as separate instances of the same type of object), the return type of a subgroup membership function is a vector-of-vectors: the index of the outer vector identifies which subgroup is being described, and the inner vector contains an entry for each shard of that subgroup. 

Derecho provides a default subgroup membership function that automatically assigns nodes from the Group into disjoint subgroups and shards, given a policy that describes the desired number of nodes in each subgroup/shard. It assigns nodes in ascending rank order, and leaves any "extra" nodes (not needed to fully populate all subgroups) at the end (highest rank) of the membership list. This function is stateful, remembering its previous output from View to View, and at each View change it attempts to preserve the correct number of nodes in each shard without re-assigning any nodes to new roles. It does this by assigning idle nodes from the end of the Group's membership list to replace failed members of subgroups.

There are several helper functions in `subgroup_functions.h` that construct AllocationPolicy objects for different scenarios, to make it easier to set up the default subgroup membership function. Here is an example of a SubgroupInfo that uses these functions to set up two types of Replicated Objects using the default membership function:
```cpp
derecho::SubgroupInfo subgroup_info {
	{{std::type_index(typeid(Foo)), derecho::DefaultSubgroupAllocator(
			derecho::one_subgroup_policy(derecho::even_sharding_policy(2, 3)))},
	 {std::type_index(typeid(Bar)), derecho::DefaultSubgroupAllocator(
			derecho::identical_subgroups_policy(2, derecho::even_sharding_policy(1, 3)))}
	}, 
	{std::type_index(typeid(Foo)), std::type_index(typeid(Bar))}
};

```
Based on the policies constructed for the constructor argument of DefaultSubgroupAllocator, the function associated with Foo will create one subgroup of type Foo, with two shards of 3 members each. The function associated with Bar will create two subgroups of type Bar, each of which has only one shard of size 3. Note that the second component of SubgroupInfo is a list of the same Replicated Object types that are in the function map; this list specifies the order in which the membership functions will be run. 

More advanced users may, of course, want to define their own subgroup membership functions. We will describe how to do this in a later section of the user guide.


#### Constructing a Group

Although the SubgroupInfo is the most important part of constructing a `derecho::Group`, it requires several additional parameters.
* The **node ID** and **IP address** of the process starting or joining the group
* The **IP address of the Group's leader**, if a process is joining the group. The "leader" is simply the process with the lowest-numbered node ID in the group.
* A set of **callback functions** that will be notified when each Derecho message is delivered to the node (stability callback) or persisted to disk (persistence callback). These can be null, and are probably not useful if you're only using the Replicated Object features of Derecho (since the "messages" will be serialized RPC calls).
* For the process that starts the group, the **Derecho parameters** that specify low-level configuration options, such as the maximum size of a message that will be sent in this group and the length of timeout to use for detecting node failures.
* A set of **View upcalls** that will be notified when the group experiences a View change event (nodes fail or join the group). This is optional and can be empty, but it can be useful for adding additional failure-handling or load-balancing behavior to your application.
* For each template parameter in the type of `derecho::Group`, its constructor will expect an additional argument of type `derecho::Factory`, which is a function or functor that constructs instances of the Replicated Object (it's just an alias for `std::function<std::unique_ptr<T>(void)>`). 

### Invoking RPC Functions

Once a process has joined a Group and one or more subgroups, it can invoke RPC functions on any of the Replicated Objects in the Group. The options a process has for invoking RPC functions depend on its membership status:

* A node can perform an **ordered query** or **ordered send** to invoke an RPC function on a Replicated Object only when it is a member of that object's subgroup and shard. Both of these operations send a multicast to all other members of the object's shard, and guarantee that the multicast will be delivered in order (so the function call will take effect at the same time on every node). An ordered query waits for responses from each member of the shard (specifically, it provides a set of Future objects that can be used to wait for responses), while an ordered send does not wait for responses and thus should only be used to invoke `void` functions.
* A node can perform a **P2P query** or **P2P send** to invoke a read-only RPC function on any Replicated Object, regardless of whether it is a member of that object's subgroup and shard. These peer-to-peer operations send a message directly to a specific node, and it is up to the sender to pick a node that is a member of the desired object's shard. They cannot be used for mutative RPC function calls because they do not guarantee ordering of the message with respect to any other (peer-to-peer or "ordered") message, and could only update one replica at a time.

Ordered sends and queries are invoked through the `Replicated` interface, whose template parameter is the type of the Replicated Object it communicates with. You can obtain a `Replicated` by using Group's `get_subgroup` method, which uses a template parameter to specify the type of the Replicated Object and an integer argument to specify which subgroup of that type (remember that more than one subgroup can implement the same type of Replicated Object). For example, this code retrieves the Replicated object corresponding to the second subgroup of type Cache:
```cpp
Replicated<Cache>& cache_rpc_handle = group->get_subgroup<Cache>(1);
```
The `ordered_send` and `ordered_query` methods use their template parameter, which is an integral "function tag," to specify which RPC function they invoke; this should correspond to the same constant you used to tag that function in the Replicated Object's `register_functions()` method. Their arguments are the arguments that will be passed to the RPC function call. The `ordered_send` function returns nothing, while the `ordered_query` function returns an instance of `derecho::rpc::QueryResults` with a template parameter equal to the return type of the RPC function. Using the Cache example from earlier, this is what RPC calls to the "put" and "contains" functions would look like:
```cpp
cache_rpc_handle.ordered_send<Cache::PUT>("Foo", "Bar");
derecho::rpc::QueryResults<bool> results = cache_rpc_handle.ordered_query<Cache::CONTAINS>("Foo");
```

P2P (peer-to-peer) sends and queries are invoked through the `ExternalCaller` interface, which is exactly like the `Replicated` interface except it only provides the `p2p_send` and `p2p_query` functions. ExternalCaller objects are provided through the `get_nonmember_subgroup` method of Group, which works exactly like `get_subgroup` (except for the assumption that the caller is not a member of the requested subgroup). For example, this is how a process that is not a member of the second Cache-type subgroup would get an ExternalCaller to that subgroup:
```cpp
ExternalCaller<Cache>& p2p_cache_handle = group->get_nonmember_subgroup<Cache>(1);
```
When invoking a P2P send or query, the caller must specify, as the first argument, the ID of the node to communicate with. The caller must ensure that this node is actually a member of the subgroup that the ExternalCaller targets (though it can be in any shard of that subgroup). For example, if node 5 is in the Cache subgroup targeted above, this is how a non-member process could make a call to `get`:
```cpp
derecho::rpc::QueryResults<std::string> results = p2p_cache_handle.p2p_query<Cache::GET>(5, "Foo");
```

#### Using QueryResults objects

The result of an ordered query is a slightly complex object, because it must contain a `std::future` for each member of the subgroup, but the membership of the subgroup might change during the query invocation. Thus, a QueryResults object is actually itself a future, which is fulfilled with a map from node IDs to futures as soon as Derecho can guarantee that the query will be delivered in a particular View. (The node IDs in the map are the members of the subgroup in that View). Each `std::future` in the map will be fulfilled with either the response from that node or a `node_removed_from_group_exception`, if a View change occurred after the query was delivered but before that node had a chance to respond.

As an example, this code waits for the responses from each node and combines them to ensure that all replicas agree on an item's presence in the cache:
```cpp
derecho::rpc::QueryResults<bool> results = cache_rpc_handle.ordered_query<Cache::CONTAINS>("Stuff");
bool contains_accum = true;
for(auto& reply_pair : results.get()) {
	bool contains_result = reply_pair.second.get();
	contains_accum = contains_accum && contains_result;
}
```
Note that the type of `reply_pair` is `std::pair<derecho::node_id_t, std::future<bool>>`, which is why a node's response is accessed by writing `reply_pair.second.get()`.

### Tracking Updates with Version Vectors
Derecho allows tracking data update history with a version vector in memory or persistent storage. A new class template is introduced for this purpose: Persistent<T,ST>. In a Persistent instance, data is managed in an in-memory object of type T (we call it the ‘current object’) along with a log in a datastore specified by storage type ST. The log can be index using a version, an index, or a timestamp. A version is a 64-bit integer attached to each version, it is managed by derecho SST and guaranteed to be monotonic. A log is also an array of versions accessible using zero-based indices. Each log is also attached by a timestamp (microseconds) indicating when this update happens according to local RTC. To enable this feature, we need to manage the data in a serializable object T, and define a member of type Persistent<T> in the Replicated Object in a relevant group. Persistent_typed_subgroup_test.cpp gives an example.
```cpp
/**
 * Example for replicated object with Persistent<T>
 */
 class PFoo : public mutils::ByteRepresentable {
  Persistent<int> pint;
 public:
  virtual ~PFoo() noexcept (true) {}
  int read_state() {
    return *pint; 
  }
  bool change_state(int new_int) {
    if(new_int == *pint) {
      return false;
    }
 
    *pint = new_int;
    return true;
  }
 
  enum Functions { READ_STATE,
                   CHANGE_STATE };
  static auto register_functions() {
    return std::make_tuple(derecho::rpc::tag<READ_STATE>(&PFoo::read_state),
      derecho::rpc::tag<CHANGE_STATE>(&PFoo::change_state));
  }
 
  // constructor for PersistentRegistry
  PFoo(PersistentRegistry * pr):pint(nullptr,pr) {}
  PFoo(Persistent<int> & init_pint):pint(std::move(init_pint)) {}
  DEFAULT_SERIALIZATION_SUPPORT(PFoo, pint);
 };
```
	
 For simplicity, the versioned type is int in this example. Basically, you set it up as a non versioned member of a replicated object except that you need pass the PersistentRegistry to the constructor from the replicated object to Persistent<T>. Derecho uses PersistentRegistry to keep track of all the Persistent<T> objects so that it can create versions on updates.The Persistent<T> constructor hooks itself to the registry.

By default, the Persistent<T> stores log in filesystem (.plog in the current directory). Application can specify memory as storage by setting the second template parameter: Persistent<T,ST_MEM> (or Volatile<T> as syntactic sugar). We are working on more store storage types including NVM.

Once the version vector is setup with derecho, the application can query the value with the get() APIs in Persistent<T>. In [persistent_temporal_query_test.cpp](https://github.com/Derecho-Project/derecho-unified/blob/master/derecho/experiments/persistent_temporal_query_test.cpp), a temporal query example is illustrated.
