# Derecho Java wrapper [TODO: more details. Explain the key java classes. use text from report.]

This is an experimental project testing the feasibility of Java API for Derecho, which is written in pure C++. By using this wrapper, Java programmers could build a distributed system that enjoys the speedup and the fault-tolerance that Derecho offers without switching their language from Java to C++.

Our programming model builds on top of the original Derecho system, by creating all the essential Derecho files in Java (eg. Group, View, Replicated, etc), but performing all the client’s computations (eg. creating a group) in the C++ Derecho library. We communicate from the Java code with the C++ code using the Java Native Interface (JNI).

# Interface

The interface exposed to Java programmers are very similar to that of C++ programmers. To get started, you should implement a Group. 

`  public Group(List<Class<?>> subgroupTypes, IShardViewGenerator shardViewGenerator, ICallbackSet callbackSet) ` 

Here, you should implement a `IShardViewGenerator` to specify the subgroup provision. The `IShardViewGenerator` features the following interface:

`public Map<Class<?>, List<List<Set<Integer>>>> generate(List<Class<?>> subgroupTypes, View previousView, View currentView)`

Derecho uses the process group model to characterize a distributed system. Therefore, you should implement this function to tell Derecho how to split your distributed system into subgroups and shards. A *subgroup* is a group of replicated state machines (processes) that handles requests in order. A *shard* is a group of machines (processes) within a subgroup that Derecho could use to shard the data. In Derecho Java, each machine in the distributed system is an `Object` with a certain class. Therefore, in Derecho Java, all objects within a subgroup must have the same class. You should specify what machines should be in what shards, and what shards should be in what subgroups. For example, if your `generate()` generates the following `Map`:

`[Foo, [[[1, 2], [3]], [[4]] ]], [Bar, [[[5]]]]`

Then there are two subgroups with `Foo` class. One of the subgroups has two shards, where one shard contains machines 1 and 2, and the other contains machine 3. The other subgroup has only one shard, which only contains machine 4. There is only one subgroup with `Bar` class, which has only one shard with machine 5.

`View` class specifies the metadata of Derecho's current situation, including a list of members and my rank. You can use it to customize your partition in `generate()`. 

The `subgroupTypes` parameter in `Group` constructor specifies the classes of subgroups in the distributed system. If you choose to implement `generate()` in the above way, then you should put `[Foo, Bar]` as the `subgroupTypes` parameter for the `Group` constructor. 

You should also implement a `ICallbackSet` and specialize three callbacks, `global_stability_callback, local_persistence_callback, global_persistence_callback`, which will be called when a message is stabilized globally, put into persistent storage locally, or put into persistent storage globally.

After you have created a `Group`, you should select a subgroup to send message using the following interface:

`public Replicated getSubgroup(Class<?> cls, int subgroupIndex) `

You need to specify the class of the subgroup you want to send messages to, as well as the subgroup ID of the subgroup. In the above example, the subgroup with machine 4 has subgroup ID 2. The `Bar` subgroup has subgroup ID 3.

After you acquired the `Replicated` object, you can use various services that Derecho Java could offer. You could call `send()` to send raw messages to this subgroup using an `IMessageGenerator`. You could call `ordered_send()` to send a method to this subgroup, and every machine in the subgroup would execute the method in the same order as they are sent. You could call `p2p_send()` to send a method to a particular machine in any subgroup. `ordered_send()` and `p2p_send()` returns a `QueryResults` future that would give you a `replyMap` when you call `get()` method from it. The `replyMap` would contain the results of execution at every node that has executed the method.

Derecho Java is an Java API that allows you to implement a fast and fault tolerant distributed system by exploiting RDMA. However, Java is a slow language, and there is a lot of overhead while serializing and deserializing objects and methods to and from Java. Also, JNI has a lot of overhead. If you want the best performance comparable with that of C++, you should send large messages/objects through the interface, and implement your own serialize/deserialize methods that would best fit your need.

# Prerequisites

You can use Derecho Java by switching to Derecho Java branch. 

`git checkout java`

In addition to the requirements to install Derecho, we require Java 11 to use Derecho Java. To install Java 11 on a Linux machine, use the following commands:

`sudo apt-get update
sudo apt-get install openjdk-11-jre openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
sudo update-java-alternatives --list
sudo update-java-alternatives --set java-11-openjdk-amd64
`

# Build 

If you successfully installed Derecho in Java branch, congratuations! You do not need to install Derecho Java additionally. 

```
#!/bin/bash
export LD_LIBRARY_PATH=<Derecho_Toy build dir>:<derecho build dir>

java -jar <Derecho_Toy build dir>/derecho.jar [number of nodes] [num_senders_selector] [num_messages] [delivery_mode] [message_size]
```

# Test [TODO: follow cascade document]

# Limitations and Future Work [TODO: can use text from report.]
