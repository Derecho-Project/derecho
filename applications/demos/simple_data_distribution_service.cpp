#include <iostream>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <mutex>
#include <shared_mutex>
#include <memory>
#include <thread>
#include <chrono>

#include "objectstore/ObjectStore.hpp"
#include "conf/conf.hpp"

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

#define CONF_DDS_PRODUCER_TOPICS "DDS/producer/topics"
#define CONF_DDS_PRODUCER_FREQ_HZ "DDS/producer/message_freq_hz"
#define CONF_DDS_CONSUMER_TOPICS "DDS/consumer/topics"

class IConnectionListener {
public:
    virtual void onData(const char* data, size_t len) = 0;
    virtual void onError(uint64_t error_code) = 0;
};

class IMessageProducer {
    const std::string topic;
public:
    IMessageProducer(std::string _topic):topic(_topic){}
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
        oss.put(object);
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
};


class DataDistributionService: public IConnection {
private:
    mutable std::shared_mutex consumer_table_mutex;
    std::map<objectstore::OID,MessageConsumer> consumers;
    objectstore::IObjectStoreService &oss;

    // Constructor
    DataDistributionService(int argc, char ** argv):
        oss(objectstore::IObjectStoreService::getObjectStoreService(
                argc, argv,
                [this](const objectstore::OID& oid, const objectstore::Object& object) {
                    std::shared_lock lock(consumer_table_mutex); // read lock
                    if (consumers.find(oid)!=consumers.end() ) {
                        if (consumers.at(oid).getDataHandler()) {
                            // Note: this is on critical path!!! Make sure this
                            // will not block in production deployment.
                            consumers.at(oid).getDataHandler()->onData(object.blob.bytes,object.blob.size);
                        }
                    }
                }
            )
        ) {
    };
    // singleton
    static std::unique_ptr<IConnection> dds;
public:
    virtual bool start() { 
        return true; 
    };
    virtual bool close() {
        oss.leave();
        return true;
    };
    virtual IMessageConsumer& createConsumer(const std::string & topic) {
        if (!oss.isReplica()) {
            throw std::invalid_argument("Consumer on client node is not supported yet.");
        }
        objectstore::OID hash_oid = std::hash<std::string>{}(topic);
        std::unique_lock write_lock(consumer_table_mutex); // write lock
        std::cout <<"consumer@"<<topic<<",oid="<<hash_oid<<std::endl;
        if (consumers.find(hash_oid)==consumers.end()) {
            consumers.emplace(hash_oid,MessageConsumer(topic));
        } 
        return consumers.at(hash_oid);
    }
    virtual std::unique_ptr<IMessageProducer> createProducer(const std::string & topic) {
        std::unique_ptr<IMessageProducer> producer_ptr(new MessageProducer(topic,oss));
        return producer_ptr;
    }

    static bool initialize(int argc, char **argv);
    static IConnection& getDDS();
};

std::unique_ptr<IConnection> DataDistributionService::dds;

bool DataDistributionService::initialize(int argc, char **argv) {
    if (dds == nullptr) {
        dds.reset(new DataDistributionService(argc, argv));
    };
    return true;
}

IConnection& DataDistributionService::getDDS(){
    if (dds == nullptr) {
        throw new std::invalid_argument("DDS is not initialized on getDDS()!");
    }
    return *dds.get();
}

/**
   DDS Demo
   ========
 */

class ConsoleLogger : public IConnectionListener {
    const std::string prefix;
public:
    ConsoleLogger(const std::string _prefix): prefix(_prefix){}
    virtual void onData(const char* data, size_t len) {
        std::cout << prefix << " " << data << std::endl;
    }
    // This is not used yet.
    virtual void onError(uint64_t error_code) {
        std::cerr << prefix << " error code=0x" << std::hex 
            << error_code << std::dec << std::endl;
    };
};

int main (int argc, char **argv) {
    std::cout << "Starting simple data distribution service(DDS)." << std::endl;
    DataDistributionService::initialize(argc,argv);
    std::atomic<bool> stopped = false;
    std::vector<std::thread> producer_threads;

    std::string producer_topics,consumer_topics;
    float producer_msg_freq = 0.0; 
    // STEP 1. Print configuration information
    if (derecho::hasCustomizedConfKey(CONF_DDS_PRODUCER_TOPICS)) {
        producer_topics = derecho::getConfString(CONF_DDS_PRODUCER_TOPICS);
        producer_msg_freq = derecho::getConfFloat(CONF_DDS_PRODUCER_FREQ_HZ);
        std::cout << "Producers publish to: " << producer_topics << std::endl;
        std::cout << "\tmessage rate: " << producer_msg_freq << " hertz" << std::endl;
    }
    if (derecho::hasCustomizedConfKey(CONF_DDS_CONSUMER_TOPICS)) {
        consumer_topics = derecho::getConfString(CONF_DDS_CONSUMER_TOPICS);
        std::cout << "Consumers subscribe to: " << consumer_topics << std::endl;
    }
    std::cout << "Once started, press any key to exit." << std::endl;

    // STEP 2. creating consumers and producers
    if (!consumer_topics.empty()) {
        std::istringstream iss(consumer_topics);
        std::string topic;
        while(std::getline(iss,topic,',')) {
            IMessageConsumer &c = DataDistributionService::getDDS().createConsumer(topic);
            c.setDataHandler(std::make_shared<ConsoleLogger>("consumer@"+topic));
        }
    }
    if (!producer_topics.empty()) {
        std::istringstream iss(producer_topics);
        std::string topic;
        while(std::getline(iss,topic,',')) {
            std::unique_ptr<IMessageProducer> producer_ptr = DataDistributionService::getDDS().createProducer(topic);
            producer_threads.emplace_back(std::thread([&](std::unique_ptr<IMessageProducer> p,std::string topic)->void{
                    std::chrono::milliseconds interval((int)(1000.0/producer_msg_freq));
                    auto wake_time = std::chrono::system_clock::now() + interval;
                    uint32_t counter = 1;
                    std::string prefix("msg-");
                    while(!stopped){
                        // produce
                        std::string data(prefix+std::to_string(counter));
                        p->write(data.c_str(),data.length()+1);
                        std::cout << "producer@" << topic << " " << data << std::endl;
                        std::this_thread::sleep_until(wake_time);
                        wake_time += interval;
                        counter ++;
                    }
                },std::move(producer_ptr),topic));
        }
    }
    // STEP 3. wait
    char x;
    std::cin >> x;
    stopped = true;
    for (auto &th : producer_threads) {
      th.join();
    }
    DataDistributionService::getDDS().close();

    return 0;
}
