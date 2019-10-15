#pragma once
#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>  
#include <mutex>
#include <memory>
#include <grpcpp/grpcpp.h>
#include <frontend.grpc.pb.h>

namespace sospdemo {

/**
 * The front end subgroup type
 * The front end subgroup type does nothing since it provides no service to
 * other Derecho nodes.
 */
class FrontEnd: public mutils::ByteRepresentable,
                public derecho::GroupReference,
                public FrontEndService::Service {
protected:
    /**
     * photo tag -> shard number
     * all photo with an unknown tag will be directed to shard 0.
     */
    std::map<uint64_t,ssize_t> tap_to_shard;
    // TODO: replacethe std::vector "categorizers" with std::map "tap_to_shard" !!!
    std::vector<node_id_t> categorizers;

    std::atomic<bool> started;
    std::mutex service_mutex;
    std::unique_ptr<grpc::Server> server;

    /**
     * the workhorses
     */
    virtual grpc::Status Whatsthis(grpc::ServerContext* context, grpc::ServerReader<PhotoRequest>* reader, 
        PhotoReply* reply) override;
    virtual grpc::Status InstallModel(grpc::ServerContext* context, grpc::ServerReader<InstallModelRequest>* reader,
        ModelReply* reply) override;
    virtual grpc::Status RemoveModel(grpc::ServerContext* context, const RemoveModelRequest* request,
        ModelReply* reply) override;

    /**
     * Start the frontend web service
     */
    virtual void start();
    /**
     * Shutdonw the frontend web service
     */
    virtual void shutdown();

public:
    /**
     * Constructors
     */
    FrontEnd(){
        started = false;
        start();
    }
    FrontEnd(std::map<uint64_t,ssize_t>& rhs) {
        this->tap_to_shard = std::move(rhs);
        started = false;
        start();
    }
    /**
     * Destructor
     */
    virtual ~FrontEnd();
    /**
     * We don't need this for the frontend
     */
    static auto register_functions() { return std::tuple<>(); };

    DEFAULT_SERIALIZATION_SUPPORT(FrontEnd,tap_to_shard);
};

}
