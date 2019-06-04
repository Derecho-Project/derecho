#include <derecho/objectstore/ObjectStore.hpp>
#include <derecho/conf/conf.hpp>
#include <functional>
#include <iostream>
#include <sstream>

#define NUM_APP_ARGS (1)

int main(int argc, char** argv) {
    if((argc < (NUM_APP_ARGS + 1)) || ((argc > (NUM_APP_ARGS + 1)) && strcmp("--", argv[argc - NUM_APP_ARGS - 1]))) {
        std::cerr << "Usage: " << argv[0] << " [ derecho-config-list -- ] <aio|bio>" << std::endl;
        return -1;
    }

    bool use_aio = false;
    if(strcmp("aio", argv[argc - NUM_APP_ARGS]) == 0) {
        use_aio = true;
    } else if(strcmp("bio", argv[1]) != 0) {
        std::cerr << "unrecognized argument:" << argv[argc - NUM_APP_ARGS] << ". Using bio (blocking io) instead." << std::endl;
    }

    derecho::Conf::initialize(argc, argv);
    std::cout << "Starting object store service..." << std::endl;
    // oss - objectstore service
    auto& oss = objectstore::IObjectStoreService::getObjectStoreService(argc, argv,
                                                                        [&](const objectstore::OID& oid, const objectstore::Object& object) {
                                                                            std::cout << "watcher: " << oid << "->" << object << std::endl;
                                                                        });
    // print some message
    std::cout << "Object store service started. \n\tIs replica:" << std::boolalpha << oss.isReplica()
              << "\n\tUsing aio API:" << use_aio
              << std::noboolalpha << "." << std::endl;

    bool bNextCommand = true;  // waiting for the next command.

    // prepare the commandline tool:
    std::map<std::string, std::pair<std::string, std::function<bool(std::string&)>>> commands = {
            {"put",  // command
             {
                     "put <oid> <string>",  // help info
                     [&oss, use_aio](std::string args) -> bool {
                         std::istringstream ss(args);
                         std::string oid, odata;
                         ss >> oid >> odata;
                         objectstore::Object object(std::stol(oid), odata.c_str(), odata.length() + 1);
                         try {
                             if(use_aio) {
                                 // asynchronous api
                                 derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> results = oss.aio_put(object);
                                 decltype(results)::ReplyMap& replies = results.get();
                                 std::cout << "aio returns:" << std::endl;
                                 for(auto& reply_pair : replies) {
                                     std::tuple<persistent::version_t,uint64_t> res = reply_pair.second.get();
                                     std::cout << reply_pair.first << ": version=" << std::get<0>(res) 
                                               << ", timestamp=" << std::get<1>(res) << std::endl;
                                 }
                             } else {
                                 std::tuple<persistent::version_t,uint64_t> result = oss.bio_put(object);
                                 // synchronous api
                                 std::cout << "version:" << std::get<0>(result) 
                                           << ", timestamp:" << std::get<1>(result) << std::endl;
                             }
                         } catch(...) {
                             return false;
                         }
                         return true;
                     }}},  // put
            {"get",        // command
             {
                     "get <oid> [version]",  // help info
                     [&oss, use_aio](std::string& args) -> bool {
                         char* argcopy = strdup(args.c_str());
                         char* token = std::strtok(argcopy, " ");
                         uint64_t oid = std::stol(token);
                         version_t ver = INVALID_VERSION;
                         if((token = std::strtok(nullptr, " ")) != nullptr) {
                             ver = std::stol(token);
                         }
                         try {
                             if(use_aio) {
                                 // asynchronous api
                                 derecho::rpc::QueryResults<const objectstore::Object> results = oss.aio_get(oid, ver);
                                 decltype(results)::ReplyMap& replies = results.get();
                                 std::cout << "aio returns:" << std::endl;
                                 for(auto& reply_pair : replies) {
                                     std::cout << reply_pair.first << ":" << reply_pair.second.get() << std::endl;
                                 }
                             } else {
                                 // synchronous api
                                 objectstore::Object obj = oss.bio_get(oid, ver);
                                 std::cout << obj << std::endl;
                             }
                         } catch(...) {
                             free(argcopy);
                             return false;
                         }
                         free(argcopy);
                         return true;
                     }}},
            {"tget",
             {
                 "tget <oid> <unix time in us>", // help info
                 [&oss, use_aio](std::string& args) -> bool {
                     std::istringstream ss(args);
                     objectstore::OID oid;
                     uint64_t ts_us;
                     ss >> oid >> ts_us;
                     try {
                         if(use_aio) {
                             // asynchronous api
                             derecho::rpc::QueryResults<const objectstore::Object> results = oss.aio_get(oid,ts_us);
                             decltype(results)::ReplyMap& replies = results.get();
                             std::cout << "aio returns:" << std::endl;
                             for(auto& reply_pair : replies) {
                                 std::cout << reply_pair.first << ":" << reply_pair.second.get() << std::endl;
                             }
                         } else {
                             // synchronous api
                             objectstore::Object obj = oss.bio_get(oid, ts_us);
                             std::cout << obj << std::endl;
                         }
                     } catch(...) {
                         return false;
                     }
                     return true;
                 }}},
            {"remove",  // command
             {
                     "remove <oid>",  // help info
                     [&oss, use_aio](std::string& args) -> bool {
                         try {
                             if(use_aio) {
                                 // asynchronous api
                                 derecho::rpc::QueryResults<std::tuple<version_t,uint64_t>> results = oss.aio_remove(std::stol(args));
                                 decltype(results)::ReplyMap& replies = results.get();
                                 std::cout << "aio returns:" << std::endl;
                                 for(auto& reply_pair : replies) {
                                     std::tuple<version_t,uint64_t> res = reply_pair.second.get();
                                     std::cout << reply_pair.first << ": version=" << std::get<0>(res)
                                               << ", timestamp=" << std::get<1>(res) << std::endl;
                                 }
                             } else {
                                 std::tuple<version_t,uint64_t> ver = oss.bio_remove(std::stol(args));
                                 std::cout << "version:" << std::get<0>(ver)
                                           << ", timestamp:" << std::get<1>(ver) << std::endl;
                             }
                         } catch(...) {
                             return false;
                         }
                         return true;
                     }}},
            {"leave",  // command
             {
                     "leave",  // help info
                     [&oss, &bNextCommand](std::string&) -> bool {
                         oss.leave();
                         bNextCommand = false;
                         return true;
                     }}}};

    std::function<void()> help = [commands]() {
        std::cout << "Commands:" << std::endl;
        for(auto& cmd_entry : commands) {
            std::cout << "\t" << cmd_entry.second.first << std::endl;
        }
    };

    help();
    // main command loop
    while(bNextCommand) {
        std::string line;
        std::cout << "cmd>";
        std::getline(std::cin, line, '\n');
        std::string::size_type first_space_pos = line.find(' ');
        std::string command;
        std::string arguments;
        if(first_space_pos == std::string::npos) {
            command = line;
        } else {
            command = line.substr(0, first_space_pos);
            arguments = line.substr(first_space_pos + 1);
        }
        if(commands.find(command) == commands.end()) {
            help();
        } else {
            bool bRet = commands[command].second(arguments);
            std::cout << "Result:" << std::boolalpha << bRet << std::noboolalpha
                      << std::endl;
        }
    }
    return 0;
}
