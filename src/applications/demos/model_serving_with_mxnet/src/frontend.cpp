#include <frontend.hpp>
#include <backend.hpp>
#include <utils.hpp>
#include <blob.hpp>

namespace sospdemo {

    using grpc::Server;
    using grpc::ServerBuilder;
    using grpc::ServerContext;
    using grpc::Status;

    void FrontEnd::start() {
        // test if grpc server is started or not.
        if (started) {
            return;
        }
        // lock it
        std::lock_guard<std::mutex> lck(service_mutex);
        // we need to check it again.
        if (started) {
            return;
        }
        // get the frontend server addresses
        const std::string frontend_string = derecho::getConfString("SOSPDEMO/frontend");
        std::cout << "frontend=" << frontend_string << std::endl;
        std::vector<frontend_info_t> frontend_list = parse_frontend_list(frontend_string);
        node_id_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);

        std::string server_address;
        for (auto& fi: frontend_list) {
            if (fi.id == my_id){
                server_address = fi.ip_and_port;
                break;
            }
        }

        if (server_address.size() == 0) {
            std::cerr << "Failed to find frontend ip and port for my_id:" << my_id 
                << ". frontend = " << frontend_string << std::endl;
        }

        std::cout << "server_address=" << server_address << std::endl;

        // get the backend node ids
        categorizers = parse_node_list(derecho::getConfString("SOSPDEMO/backend"));

        // now, start the server
        ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(this);
        this->server = std::unique_ptr(builder.BuildAndStart());
        // print messages
        std::cout << "FrontEnd listening on " << server_address << std::endl;
    }

    Status FrontEnd::Whatsthis(ServerContext* context, 
        grpc::ServerReader<PhotoRequest>* reader,
        PhotoReply* reply) {

        // 1 - read the inference request
        // 1.1 - read metadata
        PhotoRequest request;
        if (!reader->Read(&request)) {
            std::cerr << "What's this: failed to read photo metadata 1." << std::endl;
            return Status::CANCELLED;
        }
        if (!request.has_metadata()) {
            std::cerr << "What's this: failed to read photo metadata 2." << std::endl;
            return Status::CANCELLED;
        }
        uint32_t tag = request.metadata().tag();
        uint32_t photo_size = request.metadata().photo_size();
        char *photo_data = (char*)malloc(photo_size);
        uint32_t offset = 0;
        // 1.2 - read the photo file.
        request.clear_metadata();
        try {
            // receive model data
            while (reader->Read(&request)) {
                if (request.photo_chunk_case() != PhotoRequest::kFileData) {
                    std::cerr << "What's this: fail to read model data 1." << std::endl;
                    throw -1;
                }
                if (static_cast<ssize_t>(offset + request.file_data().size()) > photo_size) {
                    std::cerr << "What's this: received more data than claimed " 
                              << photo_size << "." << std::endl;
                    throw -2;
                }
                std::memcpy(photo_data + offset, request.file_data().c_str(), request.file_data().size());
                offset += request.file_data().size();
            }
            if (offset != photo_size) {
                std::cerr << "What's this: the size of received data (" << offset << " bytes) "
                          << "does not match claimed (" << photo_size << " bytes)." << std::endl;
                throw -3;
            }
        } catch (...) {
            delete photo_data;
            return Status::CANCELLED;
        }
#ifndef NDEBUG
        std::cout << "What's this?" << std::endl;
        std::cout << "tag = " << tag << std::endl;
        std::cout << "photo size = " << photo_size << std::endl;
        std::cout.flush();
#endif//NDEBUG

        // 2 - pass it to the categorizer tier.
        derecho::ExternalCaller<BackEnd>& backend_handler = group->get_nonmember_subgroup<BackEnd>();
        node_id_t target = categorizers[0]; //TODO: add randomness for load-balancing.

#ifndef NDEBUG
        std::cout << "get the handle of backend subgroup. external caller is valid:" << backend_handler.is_valid() << std::endl;
        std::cout.flush();
#endif

        // 3 - post it to the categorizer tier
        BlobWrapper photo_data_wrapper(photo_data,photo_size);
        Photo photo(tag,photo_data_wrapper);
        derecho::rpc::QueryResults<Guess> result = backend_handler.p2p_send<RPC_NAME(inference)>(
            target,
            photo);
#ifndef NDEBUG
        std::cout << "p2p_send for inference returns." << std::endl;
        std::cout.flush();
#endif
        Guess ret = result.get().get(target);

#ifndef NDEBUG
        std::cout << "response from the backend group is received with ret = " << ret.guess << "." << std::endl;
        std::cout.flush();
#endif
        delete photo_data;

        // 4 - return Status::OK;
        reply->set_desc(ret.guess);

        return Status::OK;
    }

    Status FrontEnd::InstallModel(grpc::ServerContext* context,
        grpc::ServerReader<InstallModelRequest>* reader,
        ModelReply* reply) {
        // 1 - read the model
        // 1.1 - read the header
        InstallModelRequest request;
        if (!reader->Read(&request)) {
            std::cerr << "Install Model: fail to read model metadata 1." << std::endl;
            return Status::CANCELLED;
        }
        if (!request.has_metadata()) {
            std::cerr << "Install Model: fail to read model metadata 2." << std::endl;
            return Status::CANCELLED;
        }
        uint32_t tag = request.metadata().tag();
        uint32_t synset_size = request.metadata().synset_size();
        uint32_t symbol_size = request.metadata().symbol_size();
        uint32_t params_size = request.metadata().params_size();
        // 1.2 - read the model files.
        ssize_t data_size = synset_size + symbol_size + params_size;
        ssize_t offset = 0;
        char* model_data = new char[data_size];
        request.clear_metadata();
        try{
            // receive model data
            while (reader->Read(&request)) {
                if (request.model_chunk_case() != InstallModelRequest::kFileData) {
                    std::cerr << "Install Model: fail to read model data 1." << std::endl;
                    throw -1;
                }
                if (static_cast<ssize_t>(offset + request.file_data().size()) > data_size) {
                    std::cerr << "Install Model: received more data than claimed " 
                              << data_size << "." << std::endl;
                    throw -2;
                }
                std::memcpy(model_data + offset, request.file_data().c_str(), request.file_data().size());
                offset += request.file_data().size();
            }
            if (offset != data_size) {
                std::cerr << "Install Model: the size of received data (" << offset << " bytes) "
                          << "does not match claimed (" << data_size << " bytes)." << std::endl;
                throw -3;
            }
        } catch (...) {
            delete model_data;
            return Status::CANCELLED;
        }

#ifndef NDEBUG
        std::cout << "Install Model:" << std::endl;
        std::cout << "synset_size = " << synset_size << std::endl;
        std::cout << "symbol_size = " << symbol_size << std::endl;
        std::cout << "params_size = " << params_size << std::endl;
        std::cout << "total = " << data_size << " bytes, received = " << offset << "bytes" << std::endl;
        std::cout.flush();
#endif

        // 2 - find the shard
        // Currently, we use the one-shard implementation.
        derecho::ExternalCaller<BackEnd>& backend_handler = group->get_nonmember_subgroup<BackEnd>();
        node_id_t target = categorizers[0]; //TODO: add randomness for load-balancing.

#ifndef NDEBUG
        std::cout << "get the handle of backend subgroup. external caller is valid:" << backend_handler.is_valid() << std::endl;
        std::cout.flush();
#endif

        // 3 - post it to the categorizer tier
        BlobWrapper model_data_wrapper(model_data,data_size);
        derecho::rpc::QueryResults<int> result = backend_handler.p2p_send<RPC_NAME(install_model)>(
            target,
            tag,
            static_cast<ssize_t>(synset_size),
            static_cast<ssize_t>(symbol_size),
            static_cast<ssize_t>(params_size),
            model_data_wrapper
        );
#ifndef NDEBUG
        std::cout << "p2p_send for install_model returns." << std::endl;
        std::cout.flush();
#endif
        int ret = result.get().get(target);

#ifndef NDEBUG
        std::cout << "response from the backend group is received with ret = " << ret << "." << std::endl;
        std::cout.flush();
#endif
        delete model_data;

        // 4 - return Status::OK;
        reply->set_error_code(ret);
        if (ret == 0)
            reply->set_error_desc("install model successfully.");
        else
            reply->set_error_desc("some error occurs.");

        return Status::OK;
    }

    Status FrontEnd::RemoveModel(grpc::ServerContext* context, const RemoveModelRequest* request,
        ModelReply* reply) {
        // 1 - get the model tag
        uint32_t tag = request->tag();

        // 2 - find the shard
        // currently, we use the one-shard implementation.
        derecho::ExternalCaller<BackEnd>& backend_handler = group->get_nonmember_subgroup<BackEnd>();
        node_id_t target = categorizers[0]; // TODO: add randomness for load-balancing.

        // 3 - post it to the categorizer tier
        derecho::rpc::QueryResults<int> result = 
            backend_handler.p2p_send<RPC_NAME(remove_model)>(target,tag);
        int ret = result.get().get(target);

        reply->set_error_code(ret);
        if (ret == 0)
            reply->set_error_desc("remove model successfully.");
        else
            reply->set_error_desc("some error occurs.");

        return Status::OK;
    }

    void FrontEnd::shutdown() {
        // test if grpc server is started or not
        if (!started) {
            return;
        }
        // lock it
        std::lock_guard<std::mutex> lck(service_mutex);
        if (!started) {
            return;
        }
        // now shutdown the server.
        server->Shutdown();
        server->Wait();
    }

    FrontEnd::~FrontEnd() {
        shutdown();
    }
};
