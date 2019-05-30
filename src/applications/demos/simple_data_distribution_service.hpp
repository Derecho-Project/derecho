#pragma once

#include <derecho/objectstore/ObjectStore.hpp>
#include <derecho/conf/conf.hpp>

/**
   Data Distribution Service(DDS) on Derecho Object Store
   ======================================================

   This software is a simple implementation of DDS with the Derecho Object
   Store Service. An application using this service needs to start as a Derecho
   node. A DDS node can support multiple producers and consumers. To use DDS is
   easy:

   1. An application needs first set up the derecho configuration files,
   assigning a role (a pure client or a replica) to each node. A replica node
   can run both consumers and producers, while a client node can only run
   producer. We will remove this limitation with external consumers later.
   2. Then the application initialize an IConnection object which manages the
   DDS service on each node.
   3. The application then creates consumers and producers from the IConnection
   object and does the real work.

   Please refer to the "DDS Demo" part below for how to use it. Here is a list
   of current limitations:
   1. It does not support external nodes (the processes not in the derecho group)
   so far.
   2. Early versions are dropped so far. We are going to enable it when the log
   feature is finished in Derecho Object Store.
   3. No data is persisted. We are going to enable persistent when persistence
   feature is finished in Derecho Object Store.

   The core Interfaces and Classes of DDS:

   - IConnection
   The interface for DDS Service.

   - IMessageProducer/MessageProducer
   The interface/implementation of Producers

   - IMessageConsumer/MessageConsumer
   The interface/implementation of Consumers

   - IConnectionListener
   The interface of Consumer DataHandlers.
 */

class IConnectionListener {
public:
    virtual void onData(const char* data, size_t len) = 0;
    virtual void onError(uint64_t error_code) = 0;
    virtual ~IConnectionListener() = default;
};

class IMessageProducer {
    const std::string topic;
public:
    IMessageProducer(std::string _topic):topic(_topic){}
    virtual ~IMessageProducer() = default;
    // write a message
    virtual bool write(const char* data, size_t len) = 0;
    virtual bool close() = 0;
};

class MessageProducer: public IMessageProducer {
    const objectstore::OID oid;
    objectstore::IObjectStoreService& oss;
public:
    MessageProducer(std::string _topic, objectstore::IObjectStoreService& _oss):
        IMessageProducer(_topic),
        oid(std::hash<std::string>{}(_topic)),
        oss(_oss) {
        std::cout << "producer@"<<_topic<<",oid="<<oid<<std::endl;
    }
    virtual bool write(const char* data, size_t len) {
        objectstore::Object object(this->oid,data,len);
        oss.bio_put(object);
        return true;
    }
    virtual bool close() {
        return true;
    }
};

class IMessageConsumer {
    const std::string topic;
public:
    IMessageConsumer(std::string _topic):topic(_topic){}
    virtual ~IMessageConsumer() = default;
    // set data handler
    virtual void setDataHandler(std::shared_ptr<IConnectionListener> dh) = 0;
    virtual std::shared_ptr<IConnectionListener> getDataHandler() = 0;
};

class MessageConsumer: public IMessageConsumer {
    std::shared_ptr<IConnectionListener> data_handler;
public:
    MessageConsumer(std::string _topic): IMessageConsumer(_topic) {}
    virtual void setDataHandler(std::shared_ptr<IConnectionListener> dh) {
        this->data_handler = dh;
    }
    virtual std::shared_ptr<IConnectionListener> getDataHandler() {
        return this->data_handler;
    }
};

class IConnection {
public:
    virtual bool start() = 0;
    virtual bool close() = 0;
    // IConnection owns the consumer
    virtual IMessageConsumer& createConsumer(const std::string & topic) = 0;
    // IConnection does not own the producer
    virtual std::unique_ptr<IMessageProducer> createProducer(const std::string & topic) = 0;
    virtual ~IConnection() = default;
};

class DataDistributionService: public IConnection {
private:
    mutable std::shared_mutex consumer_table_mutex;
    std::map<objectstore::OID,MessageConsumer> consumers;
    objectstore::IObjectStoreService &oss;

    // Constructor
    DataDistributionService(int argc, char ** argv);
    // singleton
    static std::unique_ptr<IConnection> dds;
public:
    virtual bool start();
    virtual bool close();
    virtual IMessageConsumer& createConsumer(const std::string & topic);
    virtual std::unique_ptr<IMessageProducer> createProducer(const std::string & topic);

    static bool initialize(int argc, char **argv);
    static IConnection& getDDS();
};

