#include <iostream>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <memory>

#include "simple_data_distribution_service.hpp"

// DataDistributionService Constructor
DataDistributionService::DataDistributionService(int argc, char ** argv):
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

bool DataDistributionService::start() { 
    return true; 
};

bool DataDistributionService::close() {
    oss.leave();
    return true;
};

IMessageConsumer& DataDistributionService::createConsumer(const std::string & topic) {
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

std::unique_ptr<IMessageProducer> DataDistributionService::createProducer(const std::string & topic) {
    std::unique_ptr<IMessageProducer> producer_ptr(new MessageProducer(topic,oss));
    return producer_ptr;
}

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

