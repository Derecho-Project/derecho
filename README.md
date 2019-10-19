Full source documentation can be found at https://derecho-project.github.io/.


# Derecho [![Build Status](https://travis-ci.com/Derecho-Project/derecho.svg?branch=master)](https://travis-ci.com/Derecho-Project/derecho)
This is the main repository for the Derecho project. It unifies the RDMC, SST, and Derecho modules under a single, easy-to-use repository. 

## Intended use cases and assumptions.
Derecho is aimed at supporting what are called "cloud micro-services", meaning pools of servers that would reside in a cluster or on a cloud data-center, probably to support the "first tier" where requests arrive from external clients, and that perform some kind of well-defined subtask like data cleaning, image classification, compression and deduplication, etc.  Although the system could definitely run in other settings, if you stray too far from our intended use cases, you'll probably trigger timeout-related crashes that we might not be very eager to try and "fix".  We recommend using Zookeeper or some other tool if you are aiming at a very different setup.

Derecho was created to leverage modern RDMA hardware, which is becoming quite common.  However, it can run on high-speed, low-latency TCP links (there is a configuration option to tell it what you want us to use, and we can also support Intel OMNI-Path and a few other options, too).  The one request we would make is that the nodes have rather uniform properties, and that this also be true for the links between them.  Derecho also assumes a relatively low rate of membership changes.  Processes can join or leave, and we even batch joins and leaves if a burst of them occurs, but the design of the system really assumes long periods (think "minutes") of stability, and we basically lock everything down during the short periods (think "hundreds of milliseconds") needed for reconfiguration.

In Figure I of our paper (http://www.cs.cornell.edu/ken/derecho-tocs.pdf) you can see an example setup.  Notice the external clients.  You can have as many of those as you like, and they can connect in all sorts of ways: TCP, RESTful RPC, WCF -- whatever you feel comfortable with (you have to do that part yourself, so use tools familiar to you).  But within the microservice itself, you want the members to be in a single cluster or datacenter, with high-speed low-latency links from node to node, no firewalls that would block our connections, etc.

Configured to run on TCP, Derecho should work when virtualized (true VMs or containers).  With RDMA, you probably need bare-metal or some sort of container running on bare-metal.  RDMA hardware doesn't virtualize well and we aren't interested in trying to get that to work.  In the TCP case, you can run a few instances of Derecho on one machine, but don't push this to the point where they would be suffering big scheduling or paging delays: that isn't matched to our intended use case, and we won't be able to help if you try that and it doesn't work well.

To obtain such high speeds, Derecho sacrifices some of the interoperability seen with other platforms, such as the CORBA or JINI communication layers. Unlike those technologies, Derecho does not convert data into a universally-compatible wire format.  Instead, it requires that the sender and receiver have identical endian formats, and identical data structure layouts and padding.  The best way to ensure that this is so is to only build a Derecho group using members that will run on the same computer architecture (for example, i86 or ARM), and only build the code with the same version of C++, configured with the same language level and optimization level.  We are implementing a self-test in Derecho to detect that an incompatible system is attempting to join a Derecho group, and reject the request.  However, this is not yet part of the system.

Derecho does not have any specific O/S dependency.  We've tested most extensively on the current release of Ubuntu, which is also the default Linux configuration used on Azure IoT Edge and Azure IoT, but there is no reason the system couldn't be used on other platforms.

## Organization
The code for this project is split into modules that interact only through each others' public interfaces. Each module resides in a directory with its own name, which is why the repository's root directory is mostly empty. External dependencies have to be installed beforehand (Yes, we removed the submodules.) 

## Installation
Derecho is a library that helps you build replicated, fault-tolerant services in a datacenter with RDMA networking. Here's how to start using it in your projects.

### Prerequisites
* Linux (other operating systems don't currently support the RDMA features we use)
* A C++ compiler supporting C++17: GCC 7.3+ or Clang 7+
* CMake 2.8.1 or newer, if you want to use the bundled build scripts
* The following system libraries: `rdmacm` (packaged for Ubuntu as `librdmacm-dev`), `ibverbs` (packaged for Ubuntu as `libibverbs-dev`).
* Open Fabric Interface (OFI) library: [`libfabric`](https://github.com/ofiwg/libfabric). To avoid compatibility issue, please use commit `fcf0f2ec3c7109e06e09d3650564df8d2dfa12b6` on `master` branch. ([Installation script](https://github.com/Derecho-Project/derecho/blob/packaging/scripts/prerequisites/install-libfabric.sh))
* Logging library: [`spdlog`](https://github.com/gabime/spdlog). To avoid compatibility issue, please use commit `10e809cf644d55e5bd7d66d02e2604e2ddd7fb48` on `master` branch. ([Installation script](https://github.com/Derecho-Project/derecho/blob/packaging/scripts/prerequisites/install-spdlog.sh))
* Matthew's C++ utilities
  - [`mutils`](https://github.com/mpmilano/mutils) ([Installation script](https://github.com/Derecho-Project/derecho/blob/packaging/scripts/prerequisites/install-mutils.sh))
  - [`mutils-containers`](https://github.com/mpmilano/mutils-containers) ([Installation script](https://github.com/Derecho-Project/derecho/blob/packaging/scripts/prerequisites/install-mutils-containers.sh))
  - [`mutils-tasks`](https://github.com/mpmilano/mutils-tasks) ([Installation script](https://github.com/Derecho-Project/derecho/blob/packaging/scripts/prerequisites/install-mutils-tasks.sh))

### Getting Started
To download the project, run

    git clone https://github.com/Derecho-Project/derecho.git

Once cloning is complete, to build the code, `cd` into the `derecho` directory and run:
* `mkdir Release`
* `cd Release`
* `cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=<path-to-install-dir> ..`
* ``make -j `lscpu | grep "^CPU(" | awk '{print $2}'` ``

This will place the binaries and libraries in the sub-directories of `Release`.
The other build type is Debug. If you need to build the Debug version, replace Release by Debug in the above instructions. We explicitly disable in-source build, so running `cmake .` in `derecho` will not work.

Once the project is built, install it by run:
* `make install`

By default, derecho will be install into `/usr/local/`. Please make sure you have `sudo` privileges to write to system directories.

Successful installation will set up the followings in `$DESTDIR`:
* `include/derecho` - the header files
* `lib/libderecho.so` - the main shared library
* `lib/libdpods.so` - the derecho Old-Plain-Data storage library
* `lib/cmake/derecho` and `lib/cmake/dpods` - cmake support for `find_package(derecho)`/`find_package(dpods)`
* `share/derecho` - sample derecho configuration files.

To uninstall, run:
* ``rm -rf `cat install_manifest.txt` ``

To build your own derecho executable, simple run:
* `g++ -std=c++1z -o myapp myapp.cpp -lderecho -pthread`
* or, `g++ -std=c++1z -o myapp myapp.cpp -lderecho -ldpods -pthread`, if you use **dPods**

To use Derecho in your code, you simply need to 
- include the header `derecho/core/derecho.hpp` in your \*.h \*.hpp or \*.cpp files, and
- specify a configuration file, either by setting environment variable `DERECHO_CONF_FILE` or by placing a file named `derecho.cfg` in the working directory. A sample configuration file along with an explanation can be found in `<installation-prefix>/share/derecho/derecho-sample.cfg`. 

The configuration file consists of three sections: **DERECHO**, **RDMA**, and **PERS**. The **DERECHO** section includes core configuration options for a Derecho instance, which every application will need to customize. The **RDMA** section includes options for RDMA hardware specifications. The **PERS** section allows you to customize the persistent layer's behavior. 

#### Configuring Core Derecho
Applications need to tell the Derecho library which node is the initial leader with the options **leader_ip** and **leader_gms_port**. Each node then specifies its own ID (**local_id**) and the IP address and ports it will use for Derecho component services (**local_ip**, **gms_port**, **rpc_port**, **sst_port**, and **rdmc_port**). 

The other important parameters are the message sizes. Since Derecho pre-allocates buffers for RDMA communication, each application should decide on an optimal buffer size based on the amount of data it expects to send at once. If the buffer size is much larger than the messages an application actually sends, Derecho will pin a lot of memory and leave it underutilized. If the buffer size is smaller than the application's actual message size, it will have to split messages into segments before sending them, causing unnecessary overhead.

Three message-size options control the memory footprint and performance of Derecho.  In all cases, larger values will increase the memory (DRAM) footprint of the application, and it is fairly easy to end up with a huge memory size if you just pick giant values.  The defaults keep the memory size smaller, but can reduce performance if an application is sending high rates of larger messages.

The options are named **max_payload_size**, **max_smc_payload_size**, and **block_size**. 

No message bigger than **max_payload_size** will be sent by Derecho. 

To understand the other two options, it helps to remember that internally, Derecho makes use of two sub-protocols when it transmits your data.  One sub-protocol is optimized for small messages, and is called SMC.  Messages equal to or smaller than **max_smc_payload_size** will be sent using SMC.  Normally **max_smc_payload_size** is set to a small value, like 1K, but we have tested with values up to 10K.  This limit should not be made much larger  performance will suffer and memory would bloat.

Larger messages are sent via RDMC, our big object protocol.  These will be automatically broken into chunks.  Each chunk will be of size  **block_size**.  The **block_size** value we tend to favor in our tests is 1MB, but we have run experiments with values as large as 100MB.   If you plan to send huge objects, like 100MB or even multi-gigabyte images, consider a larger block size  it pays off at that scale.  If you expect that huge objects would be rare, use a value like 1MB.

More information about Derecho parameter setting can be found in the comments in [the default configuration file](https://github.com/Derecho-Project/derecho/blob/master/conf/derecho-default.cfg).  You may want to read about **window_size**, **timeout_ms**, and **rdmc_send_algorithm**.

#### Configuring RDMA Devices
The most important configuration entries in this section are **provider** and **domain**. The **provider** option specifies the type of RDMA device (i.e. a class of hardware) and the **domain** option specifies the device (i.e. a specific NIC or network interface). This [Libfabric document](https://www.slideshare.net/seanhefty/ofi-overview) explains the details of those concepts.

The **tx_depth** and **rx_depth** configure the maximum of pending requests that can be waiting for acknowledgement before communication blocks. Those numbers can be different from one device to another. We recommend setting them as large as possible.

Here are some sample configurations showing how Derecho might be configured for two common types of hardware.

**Configuration 1**: run Derecho over TCP/IP with Ethernet interface 'eth0':

```
...
[RDMA]
provider = sockets
domain = eth0
tx_depth = 256
rx_depth = 256
...
```

**Configuration 2**: run Derecho over verbs RDMA with RDMA device 'mlx5_0':

```
...
[RDMA]
provider = verbs
domain = mlx5_0
tx_depth = 4096
rx_depth = 4096
...
```


#### Configuring Persistent Behavior
The application can specify the location for persistent state in the file system with **file_path**, which defaults to the `.plog` folder in the working directory. **ramdisk_path** controls the location of states for `Volatile<T>`, which defaults to tmpfs (ramdisk). **reset** controls weather to clean up the persisted state when a Derecho service shuts down. We default this to true. **Please set `reset` to `false` for normal use of `Persistent<T>`.**

#### Specify Configuration with Command Line Arguments
We also allow applications to specify configuration options on the command line. Any command line configuration options override the equivalent option in configuration file. To use this feature while still accepting application-specific command-line arguments, we suggest using the following code:

```cpp
#define NUM_OF_APP_ARGS () // specify the number of application arguments.
int main(int argc, char* argv[]) {
    if((argc < (NUM_OF_APP_ARGS+1)) || 
       ((argc > (NUM_OF_APP_ARGS+1)) && strcmp("--", argv[argc - NUM_OF_APP_ARGS - 1]))) {
        cout << "Invalid command line arguments." << endl;
        cout << "USAGE:" << argv[0] << "[ derecho-config-list -- ] application-argument-list" << endl;
        return -1;
    }
    Conf::initialize(argc, argv); // pick up configurations in the command line list
    // pick up the application argument list and continue ...
    ...
}
```
Then, call the application as follows, assuming the application's name is `app`:

```bash
$ app --DERECHO/local_id=0 --PERS/reset=false -- <application-argument-list>
```
Please refer to the [bandwidth_test](https://github.com/Derecho-Project/derecho/blob/master/applications/tests/performance_tests/bandwidth_test.cpp) application for more details.

### Setup and Testing
There are some sample programs in the folder applications/demos that can be run to test the installation. In addition, there are some performance tests in the folder applications/tests/performance\_tests that you may want to use to measure the performance Derecho achieves on your system. To be able to run the tests, you need a minimum of two machines connected by RDMA. The RDMA devices on the machines should be active. In addition, you need to run the following commands to install and load the required kernel modules for using RDMA hardware:

```
sudo apt-get install rdmacm-utils ibutils libmlx4-1 infiniband-diags libmthca-dev opensm ibverbs-utils libibverbs1 libibcm1 libibcommon1
sudo modprobe -a rdma_cm ib_uverbs ib_umad ib_ipoib mlx4_ib iw_cxgb3 iw_cxgb4 iw_nes iw_c2 ib_mthca
```
Depending on your system, some of the modules might not load which is fine.

RDMA requires memory pinning of memory regions shared with other nodes. There's a limit on the maximum amount of memory a process can pin, typically 64 KB, which Derecho easily exceeds. Therefore, you need to set this to unlimited. To do so, append the following lines to /etc/security/limits.conf:
* `[username] hard memlock unlimited`
* `[username] soft memlock unlimited`

where `[username]` is your linux username. A `*` in place of the username will set this limit to unlimited for all users. Log out and back in again for the limits to reapply. You can test this by verifying that `ulimit -l` outputs `unlimited` in bash.

The persistence layer of Derecho stores durable logs of updates in memory-mapped files. Linux also limits the size of memory-mapped files to a small size that Derecho usually exceeds, so you will need to set the system parameter `vm.overcommit_memory` to `1` for persistence to work. To do this, run the command

    sysctl -w vm.overcommit_memory = 1


We currently do not have a systematic way of asking the user for RDMA device configuration. So, we pick an arbitrary RDMA device in functions `resources_create` in `sst/verbs.cpp` and `verbs_initialize` in `rdmc/verbs_helper.cpp`. Look for the loop `for(i = 1; i < num_devices; i++)`. If you have a single RDMA device, most likely you want to start `i` from `0`. If you have multiple devices, you want to start `i` from the order (zero-based) of the device you want to use in the list of devices obtained by running `ibv_devices` in bash.

A simple test to see if your setup is working is to run the test `bandwidth_test` from applications/tests/performance\_tests. To run it, go to two of your machines (nodes), `cd` to `Release/applications/tests/performance_tests` and run `./bandwidth_test 0 10000 15 1000 1 0` on both. The programs will ask for input.
The input to the first node is:
* 0 (its node ID)
* 2 (number of nodes for the experiment)
* IP address of node 1
* IP address of node 2

Replace the node ID 0 by 1 for the input to the second node.
As a confirmation that the experiment finished successfully, the first node will write a log of the result in the file `data_derecho_bw`, which will be something along the lines of `12 0 10000 15 1000 1 0 0.37282`. Full experiment details including explanation of the arguments, results and methodology is explained in the source documentation for this program.

## Using Derecho
The file `simple_replicated_objects.cpp` within applications/demos shows a complete working example of a program that sets up and uses a Derecho group with several Replicated Objects. You can read through that file if you prefer to learn by example, or read on for an explanation of how to use various features of Derecho.

### Replicated Objects
One of the core building blocks of Derecho is the concept of a Replicated Object. This provides a simple way for you to define state that is replicated among several machines and a set of RPC functions that operate on that state.

A Replicated Object is any class that (1) is serializable with the mutils-serialization framework and (2) implements a static method called `register_functions()`. The [mutils-serialization](https://github.com/mpmilano/mutils-serialization) library should have more documentation on making objects serializable, but the most straightforward way is to inherit `mutils::ByteRepresentable`, use the macro `DEFAULT_SERIALIZATION_SUPPORT`, and write an element-by-element constructor. The `register_functions()` method is how your class specifies to Derecho which of its methods should be converted to RPC functions and what their numeric "function tags" should be. It should return a `std::tuple` containing a pointer to each RPC-callable method, wrapped in the template function `derecho::rpc::tag`, whose template parameter is an integer constant. We have provided a default implementation of this function with the macro `REGISTER_RPC_FUNCTIONS`, which registers each method in its argument using the integer constant generated by the macro `RPC_NAME`. Here is an example of a Replicated Object declaration that uses the default implementation macros: 

```cpp
class Cache : public mutils::ByteRepresentable {
    std::map<std::string, std::string> cache_map;

public:
    void put(const std::string& key, const std::string& value);
    std::string get(const std::string& key); 
    bool contains(const std::string& key);
    bool invalidate(const std::string& key);
    Cache() : cache_map() {}
    Cache(const std::map<std::string, std::string>& cache_map) : cache_map(cache_map) {}
    DEFAULT_SERIALIZATION_SUPPORT(Cache, cache_map);
    REGISTER_RPC_FUNCTIONS(Cache, put, get, contains, invalidate);
};
```

This object has one field, `cache_map`, so the DEFAULT\_SERIALIZATION\_SUPPORT macro is called with the name of the class and the name of this field. The second constructor, which initializes the field from a parameter of the same type, is required for serialization support. The object has four RPC methods, `put`, `get`, `contains`, and `invalidate`, so the REGISTER\_RPC\_FUNCTIONS macro is called with the name of the class and the names of these methods. When these RPC functions are called, they will be identified with the tags `RPC_NAME(put)`, `RPC_NAME(get)` `RPC_NAME(contains)`, and `RPC_NAME(invalidate)`.

### Groups and Subgroups

Derecho organizes nodes (machines or processes in a system) into Groups, which can then be divided into subgroups and shards. Any member of a Group can communicate with any other member, and all run the same group-management service that handles failures and accepts new members. Subgroups, which are any subset of the nodes in a Group, correspond to Replicated Objects; each subgroup replicates the state of a Replicated Object and any member of the subgroup can handle RPC calls on that object. Shards are disjoint subsets of a subgroup that each maintain their own state, so one subgroup can replicate multiple instances of the same type of Replicated Object. A Group must be statically configured with the types of Replicated Objects it can support, but the number of subgroups and their exact membership can change at runtime according to functions that you provide. 

Note that more than one subgroup can use the same type of Replicated Object, so there can be multiple independent instances of a Replicated Object in a Group even if those subgroups are not sharded. A subgroup is usually identified by the type of Replicated Object it implements and an integral index number specifying which subgroup of that type it is. 

To start using Derecho, a process must either start or join a Group by constructing an instance of `derecho::Group`, which then provides the interface for interacting with other nodes in the Group. (The difference between starting and joining a group is simply a matter of calling a different constructor). A `derecho::Group` expects a set of variadic template parameters representing the types of Replicated Objects that it can support in its subgroups. For example, this declaration is a pointer to a Group object that can have subgroups of type LoadBalancer, Cache, and Storage:

```cpp
std::unique_ptr<derecho::Group<LoadBalancer, Cache, Storage>> group;
```

#### Defining Subgroup Membership

In order to start or join a Group, all members (including processes that join later) must define a function that provides the membership (as a subset of the current View) for each subgroup. The membership function's input is the type of Replicated Object associated with a subgroup, the current View, and the previous View if there was one. Since there can be more than one subgroup that implements the same Replicated Object type (as separate instances of the same type of object), the return type of the function is a vector-of-vectors: the index of the outer vector identifies which subgroup is being described, and the inner vector contains an entry for each shard of that subgroup. For backwards-compatibility reasons, the membership function must be wrapped in a struct called SubgroupInfo before being passed into the Group constructor.

Derecho provides a default subgroup membership function that automatically assigns nodes from the Group into disjoint subgroups and shards, given a policy that describes the desired number of nodes in each subgroup/shard. It assigns nodes in ascending rank order, and leaves any "extra" nodes (not needed to fully populate all subgroups) at the end (highest rank) of the membership list. During a View change, this function attempts to preserve the correct number of nodes in each shard without re-assigning any nodes to new roles. It does this by copying the subgroup membership from the previous View as much as possible, and assigning idle nodes from the end of the Group's membership list to replace failed members of subgroups.

There are several helper functions in `subgroup_functions.h` that construct AllocationPolicy objects for different scenarios, to make it easier to set up the default subgroup membership function. Here is an example of how the default membership function could be configured for two types of Replicated Objects using these functions:

```cpp
derecho::SubgroupInfo subgroup_function {derecho::DefaultSubgroupAllocator({
    {std::type_index(typeid(Foo)), derecho::one_subgroup_policy(derecho::even_sharding_policy(2,3))},
    {std::type_index(typeid(Bar)), derecho::identical_subgroups_policy(
            2, derecho::even_sharding_policy(1,3))}
})};
```
Based on the policies constructed for the constructor argument of DefaultSubgroupAllocator, when the function is called for subgroup Foo, it will create one subgroup, with two shards of 3 members each. When the function is called for subgroup Bar, it will create two subgroups of type Bar, each of which has only one shard of size 3. Note that the order in which subgroups are allocated is the order in which their Replicated Object types are listed in the Group's template parameters, so this instance of the default subgroup allocator will assign the first 6 nodes to the Foo subgroup and the second 6 nodes to the Bar subgroups the first time it runs.

More advanced users may, of course, want to define their own subgroup membership functions. The demo program `overlapping_replicated_objects.cpp` shows a relatively simple example of a user-defined membership function. In this program, the SubgroupInfo contains a C++ lambda function that implements the `shard_view_generator_t` type signature and handles subgroup assignment for Replicated Objects of type Foo, Bar, and Cache:

```cpp
[](const std::type_index& subgroup_type,
   const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
    if(subgroup_type == std::type_index(typeid(Foo)) || subgroup_type == std::type_index(typeid(Bar))) {
        if(curr_view.num_members < 3) {
            throw derecho::subgroup_provisioning_exception();
        }
        derecho::subgroup_shard_layout_t subgroup_vector(1);
        std::vector<node_id_t> first_3_nodes(&curr_view.members[0], &curr_view.members[0] + 3);
        subgroup_vector[0].emplace_back(curr_view.make_subview(first_3_nodes));
        curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, 3);
        return subgroup_vector;
    } else { //subgroup_type == std::type_index(typeid(Cache))
        if(curr_view.num_members < 6) {
            throw derecho::subgroup_provisioning_exception();
        }
        derecho::subgroup_shard_layout_t subgroup_vector(1);
        std::vector<node_id_t> next_3_nodes(&curr_view.members[3], &curr_view.members[3] + 3);
        subgroup_vector[0].emplace_back(curr_view.make_subview(next_3_nodes));
        curr_view.next_unassigned_rank += 3;
        return subgroup_vector;
    }
};
```
For all three types of Replicated Object, the function creates one subgroup and one shard. For the Foo and Bar subgroups, it assigns first three nodes in the current View's members list (thus, these subgroups are co-resident on the same three nodes), while for the Cache subgroup it assigns nodes 3 to 6 on the current View's members list. Note that if there are not enough members in the current view to assign 3 nodes to each subgroup, the function throws `derecho::subgroup_provisioning_exception`. This is how subgroup membership functions indicate to the view management logic that a view has suffered too many failures to continue executing (it is "inadequately provisioned") and must wait for more members to join before accepting any more state updates.


#### Constructing a Group

Although the subgroup allocation function is the most important part of constructing a `derecho::Group`, it requires a few additional parameters.
* A set of **callback functions** that will be notified when each Derecho message is delivered to the node (stability callback) or persisted to disk (persistence callback). These can be null, and are probably not useful if you're only using the Replicated Object features of Derecho (since the "messages" will be serialized RPC calls).
* A set of **View upcalls** that will be notified when the group experiences a View change event (nodes fail or join the group). This is optional and can be empty, but it can be useful for adding additional failure-handling or load-balancing behavior to your application.
* For each template parameter in the type of `derecho::Group`, its constructor will expect an additional argument of type `derecho::Factory`, which is a function or functor that constructs instances of the Replicated Object (it's just an alias for `std::function<std::unique_ptr<T>(void)>`). 

### Invoking RPC Functions

Once a process has joined a Group and one or more subgroups, it can invoke RPC functions on any of the Replicated Objects in the Group. The options a process has for invoking RPC functions depend on its membership status:

* A node can perform an **ordered send** to invoke an RPC function on a Replicated Object only when it is a member of that object's subgroup and shard. This sends a multicast to all other members of the object's shard, and guarantees that the multicast will be delivered in order (so the function call will take effect at the same time on every node). An ordered send waits for responses from each member of the shard (specifically, it provides a set of Future objects that can be used to wait for responses), unless the function invoked had a return type of `void`, in which case there are no responses to wait for.
* A node can perform a **P2P query** or **P2P send** to invoke a read-only RPC function on any Replicated Object, regardless of whether it is a member of that object's subgroup and shard. P2P send is used for `void`-returning functions, while P2P query is used for non-`void` functions. These peer-to-peer operations send a message directly to a specific node, and it is up to the sender to pick a node that is a member of the desired object's shard. They cannot be used for mutative RPC function calls because they do not guarantee ordering of the message with respect to any other (peer-to-peer or "ordered") message, and could only update one replica at a time.

Ordered sends are invoked through the `Replicated` interface, whose template parameter is the type of the Replicated Object it communicates with. You can obtain a `Replicated` by using Group's `get_subgroup` method, which uses a template parameter to specify the type of the Replicated Object and an integer argument to specify which subgroup of that type (remember that more than one subgroup can implement the same type of Replicated Object). For example, this code retrieves the Replicated object corresponding to the second subgroup of type Cache:

```cpp
Replicated<Cache>& cache_rpc_handle = group->get_subgroup<Cache>(1);
```

The `ordered_send` method uses its template parameter, which is an integral "function tag," to specify which RPC function it will invoke; if you are using the `REGISTER_RPC_FUNCTIONS` macro, the function tag will be the integer generated by the `RPC_NAME` macro applied to the name of the function. Its arguments are the arguments that will be passed to the RPC function call, and it returns an instance of `derecho::rpc::QueryResults` with a template parameter equal to the return type of the RPC function. Using the Cache example from earlier, this is what RPC calls to the "put" and "contains" functions would look like:

```cpp
cache_rpc_handle.ordered_send<RPC_NAME(put)>("Foo", "Bar");
derecho::rpc::QueryResults<bool> results = cache_rpc_handle.ordered_send<RPC_NAME(contains)>("Foo");
```

P2P (peer-to-peer) sends and queries are invoked through the `ExternalCaller` interface, which is exactly like the `Replicated` interface except that it only provides the `p2p_send` and `p2p_query` functions. ExternalCaller objects are provided through the `get_nonmember_subgroup` method of Group, which works exactly like `get_subgroup` (except for the assumption that the caller is not a member of the requested subgroup). For example, this is how a process that is not a member of the second Cache-type subgroup would get an ExternalCaller to that subgroup:

```cpp
ExternalCaller<Cache>& p2p_cache_handle = group->get_nonmember_subgroup<Cache>(1);
```

When invoking a P2P send or query, the caller must specify, as the first argument, the ID of the node to communicate with. The caller must ensure that this node is actually a member of the subgroup that the ExternalCaller targets (though it can be in any shard of that subgroup). Nodes can find out the current membership of a subgroup by calling the `get_subgroup_members` method on the Group, which uses the same template parameter and argument as `get_subgroup` to select a subgroup by type and index. For example, assuming Cache subgroups are not sharded, this is how a non-member process could make a call to `get`, targeting the first node in the second subgroup of type Cache:

```cpp
std::vector<node_id_t> cache_members = group.get_subgroup_members<Cache>(1)[0];
derecho::rpc::QueryResults<std::string> results = p2p_cache_handle.p2p_query<RPC_NAME(get)>(cache_members[0], "Foo");
```

#### Using QueryResults objects

The result of an ordered query is a slightly complex object, because it must contain a `std::future` for each member of the subgroup, but the membership of the subgroup might change during the query invocation. Thus, a QueryResults object is actually itself a future, which is fulfilled with a map from node IDs to futures as soon as Derecho can guarantee that the query will be delivered in a particular View. (The node IDs in the map are the members of the subgroup in that View). Each `std::future` in the map will be fulfilled with either the response from that node or a `node_removed_from_group_exception`, if a View change occurred after the query was delivered but before that node had a chance to respond.

By the time the caller sees this exception, the view will have been updated.  Thus a caller that wishes to reissue a request could do so immediately after the exception is caught: it can already look up the new membership, select a new target, and send a new request.  On the other hand, notice that Derecho provides no indication of whether the target that failed did so before or after the original request was received.  Thus if your target might have taken some action (like issuing an update request), you may have to include application-layer logic to make sure your reissued request won't be performed twice if the initial request actually got past the update step, and the failure occurred later.  A simple way to do this is to make your requests idempotent, for example by including a request-id and an "this is a retry" flag, and if the flag is true, having the group member check to see if that request-id has already been performed.

As an example, this code waits for the responses from each node and combines them to ensure that all replicas agree on an item's presence in the cache:

```cpp
derecho::rpc::QueryResults<bool> results = cache_rpc_handle.ordered_query<RPC_NAME(contains)>("Stuff");
bool contains_accum = true;
for(auto& reply_pair : results.get()) {
    bool contains_result = reply_pair.second.get();
    contains_accum = contains_accum && contains_result;
}
```

Note that the type of `reply_pair` is `std::pair<derecho::node_id_t, std::future<bool>>`, which is why a node's response is accessed by writing `reply_pair.second.get()`.

### Tracking Updates with Version Vectors

Derecho allows tracking data update history with a version vector in memory or persistent storage. A new class template is introduced for this purpose: `Persistent<T,ST>`. In a Persistent instance, data is managed in an in-memory object of type T (we call it the "current object") along with a log in a datastore specified by storage type ST. The log can be indexed using a version number, an index, or a timestamp. A version number is a 64-bit integer attached to each version; it is managed by the Derecho SST and guaranteed to be monotonic. A log is also an array of versions accessible using zero-based indices. Each log entry also has an attached timestamp (microseconds) indicating when this update happened according to the local real-time clock. To enable this feature, we need to manage the data in a serializable object T, and define a member of type Persistent<T> in the Replicated Object in a relevant group. Persistent\_typed\_subgroup_test.cpp gives an example.

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
     
    // constructor with PersistentRegistry
    PFoo(PersistentRegistry * pr) : pint(nullptr,pr) {}
    PFoo(Persistent<int> & init_pint) : pint(std::move(init_pint)) {}
    DEFAULT_SERIALIZATION_SUPPORT(PFoo, pint);
    REGISTER_RPC_FUNCTIONS(PFoo, read_state, change_state);
};
```
	
For simplicity, the versioned type is int in this example. You set it up in the same way as a non-versioned member of a replicated object, except that you need to pass the PersistentRegistry from the constructor of the replicated object to the constructor of the `Persistent<T>`. Derecho uses PersistentRegistry to keep track of all the Persistent<T> objects in a single Replicated Object so that it can create versions on updates. The Persistent<T> constructor registers itself in the registry.

By default, the Persistent<T> stores its log in the file-system (in a folder called .plog in the current directory). Application can specify memory as the storage location by setting the second template parameter: `Persistent<T,ST_MEM>` (or `Volatile<T>` as syntactic sugar). We are working on more store storage types including NVM.

Once the version vector is set up with Derecho, the application can query the value with the get() APIs in Persistent<T>. In [persistent_temporal_query_test.cpp](https://github.com/Derecho-Project/derecho/blob/master/derecho/experiments/persistent_temporal_query_test.cpp), a temporal query example is illustrated.

###  Notes on Very Large Deployments
We are committed to supporting Derecho with RDMA on 1000 (or even more) physical nodes, one application instance per node.  On a machine that actually allows some small number K of applications to share an RDMA NIC, we would even be happy to help get things working with k\*1000's of group members... eventually.  However, we do not recommend that Derecho developers start by trying to work at that scale before gaining experience at smaller scales.  Even launching a Derecho test program at that scale would be very challenging, and we will only be able to help if the team undertaking this has a good level of experience with the system at smaller scales.
<details><summary>More on very large deployments</summary>
<p>

Moreover, "running Derecho" is a bit of a broad term.  For example, we view it as highly experimental to run with TCP on more than 16-32 nodes (we do want to scale over TCP, but it will take time).  So we would not recommend attempting to run Derecho on TCP at 1000-node scale, no matter how good your reasons: this very likely will be hard to engineer into our TCP layering, which actually runs over LibFabric, and would likely expose LibFabric scaling and performance issues.  Similarly, it is not wise to try and run 1000 Derecho application instances on, for example, 2 AWS servers, using AWS container virtualization (or the same comment with Azure, or Google, or IBM, or whatever as your provider).  That will never work, due to timeouts, and we will not try to support that sort of thing: it would be a waste of our time.   Container virtualization isn't capable of supporting this kind of application.

Additionally, it is important for you as the developer to realize that launching on 1000 physical nodes is hard, and you will spend days or weeks before this is fully stable.  Once Derecho is up and running, you depend only on our layers and those are designed to work at large scale.  But Linux is not designed to boot an application with 1000 members all of which make connections to one-another (so you get 1,000,000 connections right as it starts), plus the file system issues mentioned above.  Linux isn't normally exposed to this sort of nearly simultaneous burst of load.  Thus, Linux can be overwhelmed.

Even on HPC systems, which can support MPI at that scale, because MPI doesn't use an all-to-all connection pattern, we have seen these kinds of difficulties at massive scale.  In MPI there is one leader and N-1 followers, so the primary pattern that arises is really 1-N connections (more accurately, they do have some cases at runtime (like AllReduce) that can create K x K patterns.  I don't know how much success those folks have had with K>=1000, though.  My impression is that All Reduce normally runs on a significantly smaller scale.  Other KxK situations on MPI are probably delicate to initialize, too.)

At Cornell, up to now, our largest experiments involved cases where we benchmarked RDMC (not the full Derecho) on 1000's of nodes at the LLNL supercomputer center.  And it was a nightmare getting to the point where that worked.  In the end, we actually had a special batch script to launch them 50 at a time, and have them connect in batches, to avoid overloading the file system and TCP layer.  

Our largest Derecho experiments have been on a Texas supercomputer, where we had successful and completely stable runs on 256 physical nodes and probably could have pushed towards 1024 or more had we not run out of credits: "renting" 1000's of non-virtualized nodes is expensive.   Then just as we applied for more credit, they decommissioned the entire machine (Stampede-1).  So that whole line of experiments ended abruptly.  Still, we do think it could have been carried quite a bit further.  In this mode, we felt we were experimenting on a use case and deployment of a kind that Derecho needs to support.

So... the concept of Derecho at 1000's of nodes is something we definitely intend to support, in some specific situations where the goal makes sense, the underlying infrastructure should be able to do it, and where we have access to do debugging of our own RDMA layers during the startup.  But this isn't a sensible thing to do as your very first Derecho deployment!

</p>
</details>
