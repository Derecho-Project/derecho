#include <iostream>
#include <functional>
#include <sstream>
#include "ObjectStore.hpp"
#include "conf/conf.hpp"

#define NUM_APP_ARGS (1)

int main(int argc, char **argv) {
    if ( (argc < (NUM_APP_ARGS + 1)) || 
         ((argc > (NUM_APP_ARGS+1)) && strcmp("--", argv[argc - NUM_APP_ARGS - 1])) ) {
        std::cerr << "Usage: " << argv[0] << " [ derecho-config-list -- ] <aio|bio>" << std::endl;
        return -1;
    }

    bool use_aio = false;
    if ( strcmp("aio",argv[argc - NUM_APP_ARGS]) == 0 ) {
        use_aio = true;
    } else if ( strcmp("bio",argv[1]) == 0 ) {
        use_aio = false;
    } else {
        std::cerr << "unrecognized argument:" << argv[argc - NUM_APP_ARGS] << ". Using bio (blocking io) instead" << std::endl;
    }

    derecho::Conf::initialize(argc,argv);
    std::cout << "Starting object store service..." << std::endl;
    // oss - objectstore service
    auto& oss = objectstore::IObjectStoreService::getObjectStoreService(argc, argv, 
        [&](const objectstore::OID& oid, const objectstore::Object& object){
            std::cout << "watcher: " << oid << "->" << object << std::endl;
        });
    // print some message
    std::cout << "Object store service started. \n\tIs replica:" << std::boolalpha << oss.isReplica()
              << "\n\tUsing aio API:" << use_aio
              << std::noboolalpha << "." << std::endl;

    bool bNextCommand = true; // waiting for the next command.

    // prepare the commandline tool:
    std::map<std::string,std::pair<std::string,std::function<bool(std::string&)>>> commands = {
        {
            "put", // command
            {
                "put <oid> <string>", // help info
                [&oss,use_aio](std::string args)->bool {
                    std::istringstream ss(args);
                    std::string oid,odata;
                    ss >> oid >> odata;
                    objectstore::Object object(std::stol(oid),odata.c_str(),odata.length()+1);
                    try{
                        if ( use_aio ) {
                            // asynchronous api
                            derecho::rpc::QueryResults<bool> results = oss.aio_put(object);
                            decltype(results)::ReplyMap& replies = results.get();
                            for (auto& reply_pair: replies) {
                                std::cout << reply_pair.first << reply_pair.second.get() << std::endl;
                            }
                        } else {
                            // synchronous api
                            std::cout << "bio put:" << std::boolalpha << oss.bio_put(object)
                                      << std::noboolalpha << std::endl;
                        }
                    } catch (...) {
                        return false;
                    }
                    return true;
                }
            }
        }, // put
        {
            "get", // command
            {
                "get <oid>", // help info
                [&oss](std::string& args)->bool {
                    try{
                        objectstore::Object obj = oss.bio_get(std::stol(args));
                        std::cout << obj << std::endl;
                    } catch (...) {
                        return false;
                    }
                    return true;
                }
            }
        },
        {
            "remove", // command
            {
                "remove <oid>", // help info
                [&oss](std::string& args)->bool {
                    try{
                        return oss.bio_remove(std::stol(args));
                    } catch (...) {
                        return false;
                    }
                }
            }
        },
        {
            "leave", // command
            {
                "leave", // help info
                [&oss,&bNextCommand](std::string&)->bool {
                    oss.leave();
                    bNextCommand = false;
                    return true;
                }
            }
        }
    };

    std::function<void()> help = [commands](){
        std::cout << "Commands:" << std::endl;
        for (auto& cmd_entry : commands) {
            std::cout << "\t" << cmd_entry.second.first <<std::endl;
        }
    };

    help();
    // main command loop
    while (bNextCommand) {
        std::string line;
        std::cout << "cmd>";
        std::getline(std::cin,line,'\n');
        std::string::size_type first_space_pos = line.find(' ');
        std::string command;
        std::string arguments;
        if (first_space_pos == std::string::npos) {
            command = line;
        } else {
            command = line.substr(0,first_space_pos);
            arguments = line.substr(first_space_pos+1);
        }
        if (commands.find(command)==commands.end()) {
            help();
        } else {
           bool bRet = commands[command].second(arguments);
           std::cout << "Result:" << std::boolalpha << bRet << std::noboolalpha
                     << std::endl;
        }
    }
    return 0;
}
