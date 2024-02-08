#include <derecho/config.h>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <derecho/conf/conf.hpp>
#include <iostream>
#include <getopt.h>
#include <rpc/server.h>
#include <rpc/rpc_error.h>
#include <tuple>
#include <vector>
#include <mutex>
#include <memory>
#include <signal.h>
#include <unistd.h>

namespace derecho {

const char* help_string_args = 
"--(p)ort <port>        Specify the port number of the member registry, defaulted to 50182\n"
"--(d)aemon             Running in daemonized mode.\n"
"--(h)elp               Print this information.\n";

__attribute__ ((visibility("hidden")))
void print_help(const char* cmd) {
    std::cout << "Running the member registry." << std::endl;
    std::cout << "============================" << std::endl;
    std::cout << "Usage: " << cmd << " [options]" << std::endl;
    std::cout << help_string_args << std::endl;
}

__attribute__ ((visibility("hidden")))
struct option long_options[] = {
    {"port",            required_argument,  0,  'p'},
    {"help",            no_argument,        0,  'h'},
    {0,0,0,0}
};

class MemberRegistryState {
private:
    std::mutex  state_mutex;
    std::vector<std::tuple<std::string,uint16_t>> members;
public:
    /**
     * Get
     * @brief Get the current leader
     * @return A tuple of ip, gms port, and external port.
     */
    __attribute__ ((visibility("hidden")))
    auto get() {
        const std::lock_guard<std::mutex> lock(state_mutex);
        return this->members;
    }
    /**
     * Put
     * @brief Put the current leader
     * @return a bool to tell if this operation is successful or not.
     */
    __attribute__ ((visibility("hidden")))
    bool put(const std::vector<std::tuple<std::string,uint16_t>>& _members) {
        const std::lock_guard<std::mutex> lock(state_mutex);
        this->members = _members;
        return true;
    }
};


__attribute__ ((visibility("hidden")))
sighandler_t default_term_handler = nullptr;

__attribute__ ((visibility("hidden")))
std::unique_ptr<::rpc::server> server;

__attribute__ ((visibility("hidden")))
void term_handler(int signum) {
    assert(signum == SIGTERM);

    if (server) {
        server->close_sessions();
        server->stop();
    }

    if (default_term_handler != nullptr) {
        default_term_handler(signum);
    }
}

int main(int argc, char** argv) {

    uint16_t    port = getConfUInt16(Conf::DERECHO_MEMBER_REGISTRY_PORT);
    bool        daemonized = false;

    while(true) {
        int option_index = 0;
        int c = getopt_long(argc,argv,"p:dh",long_options,&option_index);
        if (c == -1) {
            break;
        }
        switch (c) {
        case 'p':
            port = static_cast<uint16_t>(std::stoi(optarg));
            break;
        case 'd':
            daemonized = true;
            break;
        case 'h':
            print_help(argv[0]);
            return 0;
        case '?':
        default:
            std::cout << "skipping unknown argument." << std::endl;
        }
    }

    MemberRegistryState member_registry_state{};

    server = std::make_unique<::rpc::server>(port);
    server->bind("get",[&member_registry_state]()->std::vector<std::tuple<std::string,uint16_t>> {
            return member_registry_state.get();
        });

    server->bind("put",[&member_registry_state](std::vector<std::tuple<std::string,uint16_t>> members)->bool {
            return member_registry_state.put(members);
        });


    if (daemonized) {
        default_term_handler = signal(SIGTERM,term_handler);
        if (daemon(1,0) == -1) {
            std::cerr << "Failed to daemonize with error:" << errno << "." << std::endl;
        }
        server->run();
    } else {
        server->run();
    }

    return 0;
}

}// derecho

int main(int argc, char** argv) {
    return derecho::main(argc, argv);
}
