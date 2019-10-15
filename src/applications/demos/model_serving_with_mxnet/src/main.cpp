#include <iostream>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <derecho/core/derecho.hpp>
#include <vector>
#include <frontend.hpp>
#include <backend.hpp>
#include <utils.hpp>

/**
 * Application: this tester demonstrates how to build an IoT application with Derecho.
 * The IoT application consists of two kind of nodes: server and web clients. The server
 * node run either a computation node identifying which flower is in the uploaded message
 * or a frontend dispatching request to one of the computation node. A web client sends
 * restful request to the server nodes to tell what's in the picture.
 */

static void print_help(const char* cmd) {
    std::cout << "Usage:" << cmd << " <mode> <mode specific args>\n"
        << "The mode could be one of the following:\n"
        << "    client - the web client.\n"
        << "    server - the server node. Configuration file determines if this is a computation node or a frontend server. \n"
        << "1) to start a server node:\n"
        << "    " << cmd << " server \n"
        << "2) to perform inference: \n"
        << "    " << cmd << " client inference <tag> <photo>\n"
        << "3) to install a model: \n"
        << "    " << cmd << " client installmodel <tag> <synset> <symbol> <params>\n"
        << "4) to remove a model: \n"
        << "    " << cmd << " client removemodel <tag>"
        << std::endl;
}

#define CONF_SOSPDEMO_FRONTEND_LIST "SOSPDEMO/frontend"
#define CONF_SOSPDEMO_MIN_FRONTEND  "SOSPDEMO/min_frontend"
#define CONF_SOSPDEMO_BACKEND_LIST  "SOSPDEMO/backend"
#define CONF_SOSPDEMO_MIN_BACKEND   "SOSPDEMO/min_backend"

/**
 * client sends request to server node.
 */

/**
 * validate a file for read.
 * @param filename
 * @return file size, negative number for invalid file
 */
inline ssize_t validate_readable_file(const char* filename) {
    struct stat st;

    if (stat(filename, &st) || access(filename,R_OK)) {
        return -1;
    }

    if ( (S_IFMT & st.st_mode) != S_IFREG) {
        return -2;
    }

    return static_cast<ssize_t>(st.st_size);
}

/**
 * A helper function uploading a file.
 * @param file filename
 * @param length length of the file
 * @param writer the client writer
 * @return number of bytes. Negative number for failure
 */
template <typename RequestType>
ssize_t file_uploader(const std::string& file, ssize_t length, 
    std::unique_ptr<grpc::ClientWriter<RequestType>>& writer) {

    int fd;
    void *file_data;
    const ssize_t chunk_size = (1ll<<15); // 32K chunking size

    // open and map file
    if ((fd = open(file.c_str(), O_RDONLY)) < 0) {
        std::cerr << "failed to open file(" << file << ") in readonly mode with " 
                  << "error:" << strerror(errno) << "." << std::endl;
        return -1;
    }

    if ((file_data = mmap(nullptr, length, PROT_READ, MAP_PRIVATE|MAP_POPULATE, fd, 0)) == MAP_FAILED) {
        std::cerr << "failed to map file(" << file << ") with " 
                  << "error:" << strerror(errno) << "." << std::endl;
        return -2;
    }

    // upload file
    ssize_t offset = 0;
    // sospdemo::InstallModelRequest request;
    RequestType request;

    while ((length - offset) > 0) {
        ssize_t size = chunk_size;
        if ((length - offset) < size ) {
            size = length - offset;
        }
        request.set_file_data(static_cast<void*>(static_cast<char*>(file_data) + offset),size);
        if(!writer->Write(request)) {
            std::cerr << "failed to upload file(" << file << ") at offset "
                      << offset << "." << std::endl;
            break;
        }
        offset += size;
    }

    // unmap and close file
    if (munmap(file_data, length)) {
        std::cerr << "failed to unmap file(" << file << ") with "
                  << "error:" << strerror(errno) << "." << std::endl;
        return -3;
    }
    if (close(fd)) {
        std::cerr << "failed to close file(" << file << ") with "
                  << "error:" << strerror(errno) << "." << std::endl;
        return -4;
    }

    return offset;
}

/**
 * Send an install model request to a front end node with gRPC.
 * @param stub_ - gRPC session
 * @param tag - model tag
 * @param synset_file - synset text file name
 * @param symbol_file - symbol json file name
 * @param params_file - parameter file name
 */
void client_install_model(std::unique_ptr<sospdemo::FrontEndService::Stub>& stub_,
    uint32_t tag,
    const std::string& synset_file,
    const std::string& symbol_file, 
    const std::string& params_file) {

    grpc::ClientContext context;
    sospdemo::ModelReply reply;

    // send metadata
    sospdemo::InstallModelRequest request;
    sospdemo::InstallModelRequest::ModelMetadata metadata;
    metadata.set_tag(tag);
    ssize_t synset_file_size = validate_readable_file(synset_file.c_str());
    if (synset_file_size < 0) {
        std::cerr << "invalid model synset file:" << synset_file << std::endl;
        return;
    }
    metadata.set_synset_size(static_cast<uint32_t>(synset_file_size));
    ssize_t symbol_file_size = validate_readable_file(symbol_file.c_str());
    if (symbol_file_size < 0) {
        std::cerr << "invalid model symbol file:" << symbol_file << std::endl;
        return;
    }
    metadata.set_symbol_size(static_cast<uint32_t>(symbol_file_size));
    ssize_t params_file_size = validate_readable_file(params_file.c_str());
    if (params_file_size < 0) {
        std::cerr << "invalid model params file:" << params_file << std::endl;
        return;
    }
    metadata.set_params_size(static_cast<uint32_t>(params_file_size));
    request.set_allocated_metadata(&metadata);

    std::unique_ptr<grpc::ClientWriter<sospdemo::InstallModelRequest>> writer = stub_->InstallModel(&context, &reply);
    if(!writer->Write(request)) {
        request.release_metadata();
        std::cerr << "fail to send install model metadata." << std::endl;
        return;
    }
    request.release_metadata();

    // send model files
    if (file_uploader(synset_file,synset_file_size,writer) != synset_file_size)
        return;
    if (file_uploader(symbol_file,symbol_file_size,writer) != symbol_file_size)
        return;
    if (file_uploader(params_file,params_file_size,writer) != params_file_size)
        return;

    // Finish up.
    writer->WritesDone();
    grpc::Status status = writer->Finish();

    if (status.ok()) {
        std::cerr << "error code:" << reply.error_code() << std::endl;
        std::cerr << "error description:" << reply.error_desc() << std::endl;
    } else {
        std::cerr << "grpc::Status::error_code:" << status.error_code() << std::endl;
        std::cerr << "grpc::Status::error_details:" << status.error_details() << std::endl;
        std::cerr << "grpc::Status::error_message:" << status.error_message() << std::endl;
    }
    
    return;
}


/**
 * Send an inference request to a front end node with gRPC.
 * @param stub_ - gRPC session 
 * @param tag - model tag
 * @param photo_file - photo file name
 */
void client_inference(std::unique_ptr<sospdemo::FrontEndService::Stub> &stub_,
    uint32_t tag,
    const std::string& photo_file) {

    sospdemo::PhotoRequest request;
    sospdemo::PhotoRequest::PhotoMetadata metadata;
    sospdemo::PhotoReply reply;
    grpc::ClientContext context;

    ssize_t photo_file_size = validate_readable_file(photo_file.c_str());
    if (photo_file_size < 0) {
        std::cerr << "invalid photo file:" << photo_file << std::endl;
        return;
    }

    // 1 - send metadata
    metadata.set_tag(tag);
    metadata.set_photo_size(photo_file_size);
    request.set_allocated_metadata(&metadata);
    std::unique_ptr<grpc::ClientWriter<sospdemo::PhotoRequest>> writer = stub_->Whatsthis(&context, &reply);
    if(!writer->Write(request)) {
        request.release_metadata();
        std::cerr << "fail to send inference metadata." << std::endl;
        return;
    }
    request.release_metadata();

    // 2 - send photo
    if (file_uploader(photo_file,photo_file_size,writer) != photo_file_size)
        return;

    // 3 - Finish up.
    writer->WritesDone();
    grpc::Status status = writer->Finish();

    if (status.ok()) {
        std::cerr << "photo description:" << reply.desc() << std::endl;
    } else {
        std::cerr << "grpc::Status::error_code:" << status.error_code() << std::endl;
        std::cerr << "grpc::Status::error_details:" << status.error_details() << std::endl;
        std::cerr << "grpc::Status::error_message:" << status.error_message() << std::endl;
    }
    
    return;
}

/**
 * Send an remove model request to a front end node with gRPC.
 * @param stub_ - gRPC session
 * @param tag - model tag
 */
void client_remove_model(std::unique_ptr<sospdemo::FrontEndService::Stub>& stub_,
    uint32_t tag) {

    grpc::ClientContext context;
    sospdemo::ModelReply reply;

    // send metadata
    sospdemo::RemoveModelRequest request;
    request.set_tag(tag);

    grpc::Status status = stub_->RemoveModel(&context, request, &reply);

    if (status.ok()) {
        std::cerr << "error code:" << reply.error_code() << std::endl;
        std::cerr << "error description:" << reply.error_desc() << std::endl;
    } else {
        std::cerr << "grpc::Status::error_code:" << status.error_code() << std::endl;
        std::cerr << "grpc::Status::error_details:" << status.error_details() << std::endl;
        std::cerr << "grpc::Status::error_message:" << status.error_message() << std::endl;
    }
    
    return;
}


void do_client(int argc, char** argv) {
    // briefly check the arguments
    if (argc < 3 || std::string("client").compare(argv[1])) {
        std::cerr << "Invalid command." << std::endl;
        print_help(argv[0]);
        return;
    }

    // load sospdemo configuration
    derecho::Conf::initialize(argc,argv);
    auto frontend_nodes = parse_frontend_list(derecho::getConfString(CONF_SOSPDEMO_FRONTEND_LIST));

    if (frontend_nodes.size() == 0) {
        std::cerr << "no frontend node is found in configuration file...exit." << std::endl;
        return;
    }
    std::string frontend = frontend_nodes[0].ip_and_port;
    std::cout << "Use frontend: " << frontend << std::endl;

    // prepare gRPC client
    std::unique_ptr<sospdemo::FrontEndService::Stub> stub_(sospdemo::FrontEndService::NewStub(
        grpc::CreateChannel(frontend_nodes[0].ip_and_port,grpc::InsecureChannelCredentials())));

    // parse the command
    if (std::string("inference").compare(argv[2]) == 0) {

        if (argc < 5) {
            std::cerr << "invalid inference command." << std::endl;
            print_help(argv[0]);
        } else {
            uint32_t tag = static_cast<uint32_t>(std::atoi(argv[3]));
            std::string photo_file(argv[4]);
            client_inference(stub_,tag,photo_file);
        }
    } else if (std::string("installmodel").compare(argv[2]) == 0) {
        if (argc < 7) {
            std::cerr << "invalid install model command." << std::endl;
            print_help(argv[0]);
        } else {
            uint32_t tag = static_cast<uint32_t>(std::atoi(argv[3]));
            std::string synset_file(argv[4]);
            std::string symbol_file(argv[5]);
            std::string params_file(argv[6]);
            client_install_model(stub_,tag,synset_file,symbol_file,params_file);
        }
    } else if (std::string("removemodel").compare(argv[2]) == 0) {
        if (argc < 4) {
            std::cerr << "invalid remove model command." << std::endl;
            print_help(argv[0]);
        } else {
            uint32_t tag = static_cast<uint32_t>(std::atoi(argv[3]));
            client_remove_model(stub_,tag);
        }
    } else {
        std::cerr << "Invalid client command:" << argv[2] << std::endl;
        print_help(argv[0]);
    }
}


/**
 * Start a server node
 */

void do_server(int argc, char** argv) {
    // load configuration
    derecho::Conf::initialize(argc,argv);
    auto frontend_nodes = parse_node_list(derecho::getConfString(CONF_SOSPDEMO_FRONTEND_LIST));
    auto backend_nodes = parse_node_list(derecho::getConfString(CONF_SOSPDEMO_BACKEND_LIST));
    uint32_t min_frontend = derecho::getConfUInt32(CONF_SOSPDEMO_MIN_FRONTEND);
    uint32_t min_backend = derecho::getConfUInt32(CONF_SOSPDEMO_MIN_BACKEND);

    // 1 - create subgroup info
    derecho::SubgroupInfo si {
        // subgroup allocation callback
        [frontend_nodes,backend_nodes,min_frontend,min_backend] (
            const std::vector<std::type_index>& subgroup_type_order, // subgroup types
            const std::unique_ptr<derecho::View>& prev_view, // previous view
            derecho::View& curr_view // current view
        ) {
            derecho::subgroup_allocation_map_t subgroup_allocation;
            std::vector<node_id_t> active_frontend_nodes,active_backend_nodes;
            for (size_t i = 0; i < curr_view.members.size(); i++ ) {
                const node_id_t id = curr_view.members[i];
                if (curr_view.failed[i]) {
                    continue;
                }
                else if (std::find(frontend_nodes.begin(), frontend_nodes.end(), id) != frontend_nodes.end()) {
                    active_frontend_nodes.push_back(id);
                }
                else if (std::find(backend_nodes.begin(), backend_nodes.end(), id) != backend_nodes.end()) {
                    active_backend_nodes.push_back(id);
                }
            }
            if (active_frontend_nodes.size() < min_frontend || active_backend_nodes.size() < min_backend) {
                throw derecho::subgroup_provisioning_exception(); // the view is not enough.
            }
            // prepare allocation
            for (const auto& subgroup_type: subgroup_type_order) {
                if (subgroup_type == std::type_index(typeid(sospdemo::FrontEnd))) {
                    derecho::subgroup_shard_layout_t subgroup_vector(1);
                    subgroup_vector[0].emplace_back(curr_view.make_subview(active_frontend_nodes,derecho::Mode::ORDERED,{},"FRONTEND"));
                    curr_view.next_unassigned_rank += active_frontend_nodes.size();
                    subgroup_allocation.emplace(subgroup_type, std::move(subgroup_vector));
                } else { // subgroup type is typeid(sospdemo::BackEnd) TODO: enable shard.
                    derecho::subgroup_shard_layout_t subgroup_vector(1);
                    subgroup_vector[0].emplace_back(curr_view.make_subview(active_backend_nodes,derecho::Mode::ORDERED,{},"BACKEND"));
                    curr_view.next_unassigned_rank += active_backend_nodes.size();
                    subgroup_allocation.emplace(subgroup_type, std::move(subgroup_vector));
                }
            }
            return subgroup_allocation;
        }
    };

    // 2 - prepare factories
    auto frontend_factory = [](persistent::PersistentRegistry*){return std::make_unique<sospdemo::FrontEnd>();};
    auto backend_factory = [](persistent::PersistentRegistry*){return std::make_unique<sospdemo::BackEnd>();};

    // 3 - create the group
    derecho::Group<sospdemo::FrontEnd,sospdemo::BackEnd> group({}, si, nullptr, std::vector<derecho::view_upcall_t>{}, frontend_factory, backend_factory);
    std::cout << "Finished constructing derecho group." << std::endl;

    // 4 - waiting for an input to leave
    std::cout << "Press any key to stop." << std::endl;
    std::cin.get();
    group.barrier_sync();
    group.leave(true);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        print_help(argv[0]);
        return -1;
    } else if (std::string(argv[1]) == "client") {
        do_client(argc,argv);
    } else if (std::string(argv[1]) == "server") {
        do_server(argc,argv);
    } else {
        std::cerr << "Unknown mode:" << argv[1] << std::endl;
        print_help(argv[0]);
    }
    return 0;
}
