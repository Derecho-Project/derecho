#include <derecho/config.h>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <derecho/conf/conf.hpp>
#include <getopt.h>
#include <rpc/client.h>
#include <iostream>
#include <tuple>

namespace derecho {
const char*  help_string_args =
"--(s)erver <host>      Specify the member registry hostname/ip, defaulted to 'localhost'\n"
"--(p)ort <port>        Specify the port number of the member registry, defaulted to 50182\n"
"--(h)elp               Print this information.\n";

__attribute__ ((visibility("hidden")))
void print_help(const char* cmd) {
    std::cout << "Leader registry cli tool." << std::endl;
    std::cout << "=========================" << std::endl;
    std::cout << "Usage: " << cmd << " [options]"
              << std::endl;
    std::cout << help_string_args << std::endl;
}

__attribute__ ((visibility("hidden")))
struct option long_options[] = {
    {"server",      required_argument,  0,  's'},
    {"port",        required_argument,  0,  'p'},
    {"help",        no_argument,        0,  'h'},
    {0,0,0,0}
};

int main(int argc, char** argv) {
    std::string server =    getConfString(Conf::DERECHO_MEMBER_REGISTRY_IP);
    uint16_t    port =      getConfUInt16(Conf::DERECHO_MEMBER_REGISTRY_PORT);

    while(true) {
        int option_index = 0;
        int c = getopt_long(argc, argv, "s:p:h", long_options, &option_index);
        if (c == -1) {
            break;
        }
        switch (c) {
        case 's':
            server = optarg;
            break;
        case 'p':
            port = static_cast<uint16_t>(std::stoi(optarg));
            break;
        case 'h':
            print_help(argv[0]);
            return 0;
        case '?':
        default:
            std::cerr << "Skipping unknown argument." << std::endl;
        }
    }

    ::rpc::client client(server,port);
    auto members = client.call("get").as<std::vector<std::tuple<std::string,uint16_t>>>();
    std::cout << members.size() << " active members:" << std::endl;
    for (auto& member : members) {
        std::cout << std::get<0>(member) << ":" << std::get<1>(member) << std::endl;
    }

    return 0;
}

}

int main(int argc, char** argv) {
    return derecho::main(argc,argv);
}
