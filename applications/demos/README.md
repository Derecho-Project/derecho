# Application Demos
The example programs here show how to use different aspects of Derecho's functionality.

## 1. Simple Data Distribution Service (DDS)
This demo shows how to build a simplified [DDS](https://en.wikipedia.org/wiki/Data_Distribution_Service) system based on the Derecho ObjectStore service. Each process in a distributed application can start a number of producers and consumers on any topics. The DDS is responsible for routing the message from the producers to the consumers on the same topics.

### Build the DDS Demo
This demo is incorporated in the Derecho building system. So, it is built together with the whole derecho project. To speed up,  you can also do 
```
make dds_demo
```
after `cmake`. The binary can be found at `build/applications/demos/dds_demo`.

### Configure and Run
The demo read configurations from the main configuration file (Please refer to "Configuring Core Derecho" section in the [derecho document](https://github.com/Derecho-Project/derecho-unified) for more details). The relevant options of DDS are
* `DDS_DEMO/producer/topics`: the topics on which a node publishes messages.
* `DDS_DEMO/producer/message_freq_hz`: the message rate of the producers.
* `DDS_DEMO/consumer/topics`: the topics on which a node subscribes to.
Since the DDS Demo rely on [Derecho Plain-old-data Store(dPods)](https://github.com/Derecho-Project/derecho-unified/tree/master/objectstore), we need to provide configurations for dPods as following:
```
[OBJECTSTORE]
replicas = 0-2
persisted = false
logged = false
```
The most important option in `OBJECTSTORE` section is `replicas`, which designates a set of nodes by IDs to keep the data, and hence be able to create consumers. (Yes, only the replica nodes can start consumers. This is a limitation we are going to remove later in the object store.) The `persisted` and `logged` options need to be set to `false` so far (to be supported later.)

Let's assume we have three nodes A(ID=0), B(ID=1), and C(ID=2). A publishes to `topic1`, B publishes to `topic2`, and C publishes to `topic3`; and they send a message per second. A, B, and C are all subscribe to all three topics. Then A's confiugration should look like this:
```
# core derecho configurations
...
[OBJECTSTORE]
replicas = 0-2
persisted = false
logged = false
[DDS_DEMO/producer]
topics = topic1
message_freq_hz = 1.0
[DDS_DEMO/consumer]
topics = topic1,topic2,topic3
```
B's configuration:
```
# core derecho configurations
...
[OBJECTSTORE]
replicas = 0-2
persisted = false
logged = false
[DDS_DEMO/producer]
topics = topic2
message_freq_hz = 1.0
[DDS_DEMO/consumer]
topics = topic1,topic2,topic3
```
C's configuration:
```
# core derecho configurations
...
[OBJECTSTORE]
replicas = 0-2
persisted = false
logged = false
[DDS_DEMO/producer]
topics = topic3
message_freq_hz = 1.0
[DDS_DEMO/consumer]
topics = topic1,topic2,topic3
```
Then, you can run `dds_demo` on nodes A, B, and C. The demo will print the message produced and consumed as following:
```
producer@topic1 msg-1
consumer@topic1 msg-1
consumer@topic2 msg-1
consumer@topic3 msg-1
...
```
### Future Features
This is a simple demo to be extended with more features we plan to support
* Message persistence
* Logged messages that can be retrieved with temporal queries
* External consumers that need not to be an object store replica
* Pointer to large message to avoid unnecessary data transfer

### Source Code Files
The Demo includes the following files:
* [simple_data_distribution_service.hpp](https://github.com/Derecho-Project/derecho-unified/blob/master/applications/demos/simple_data_distribution_service.hpp): the interface of simple DDS. 
* [simple_data_distribution_service.cpp](https://github.com/Derecho-Project/derecho-unified/blob/master/applications/demos/simple_data_distribution_service.hpp): the implementation of simple DDS.
* [dds_demo.cpp](https://github.com/Derecho-Project/derecho-unified/blob/master/applications/demos/dds_demo.cpp): the DDS demo source.
* [dds-default.cfg](https://github.com/Derecho-Project/derecho-unified/blob/master/applications/demos/dds-default.cfg): the sample configuration file



## 2. Over Lapping Replicated Objects
TODO

## 3. Random Messages
TODO

## 4. Simple Replicated Objects
TODO
