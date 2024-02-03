#include <derecho/config.h>
#include <derecho/conf/conf.hpp>
#include <iostream>
#include <getopt.h>
#include <rpc/server.h>
#include <rpc/rpc_error.h>
#include <tuple>
#include <mutex>

namespace derecho {

const char* help_string_args = 
"--(p)ort <port>        Specify the port number of the leader registry, defaulted to 50182\n"
"--leader_(i)p <ipaddress>\n"
"                       Specify the ip address of the initial leader node.\n"
"--leader_(g)ms_port <port>\n"
"                       Specify the gms port of the initial leader node.\n"
"--(h)elp               Print this information.\n";

static void print_help(const char* cmd) {
    std::cout << "Running the leader registry." << std::endl;
    std::cout << "============================" << std::endl;
    std::cout << "Usage: " << cmd << " [options]" << std::endl;
    std::cout << help_string_args << std::endl;
}

static struct option long_options[] = {
    {"port",            required_argument,  0,  'p'},
    {"leader_ip",       required_argument,  0,  'i'},
    {"leader_gms_port", required_argument,  0,  'g'},
    {"help",            no_argument,        0,  'h'},
    {0,0,0,0}
};

static bool check_leader_ip_and_port(const std::string& ip,const uint16_t port) {
    if (ip.empty() || port == 0) {
        return false;
    }
    return true;
}

class LeaderRegistryState {
private:
    std::mutex  state_mutex;
    std::string ip;
    uint16_t    gms_port;
    uint16_t    ext_port;
public:
    /**
     * Get
     * @brief Get the current leader
     * @return A tuple of ip, gms port, and external port.
     */
    std::tuple<std::string,uint16_t,uint16_t> get() {
        const std::lock_guard<std::mutex> lock(state_mutex);
        return {ip,gms_port,ext_port};
    }
    /**
     * Put
     * @brief Put the current leader
     * @return a bool to tell if this operation is successful or not.
     */
    bool put(std::string ip,uint16_t gms_port,uint16_t ext_port) {
        const std::lock_guard<std::mutex> lock(state_mutex);
        this->ip = ip;
        this->gms_port = gms_port;
        this->ext_port = ext_port;
        return true;
    }
    /**
     * Constructor
     * @param init_ip       The initial ip address
     * @param init_gms_port The initial gms port
     */
    LeaderRegistryState(const std::string init_ip,const uint16_t init_gms_port):
        ip(init_ip),
        gms_port(init_gms_port),
        ext_port(0) {}
};

int main(int argc, char** argv) {

    uint16_t    port = LEADER_REGISTRY_PORT;
    std::string leader_ip = "";
    uint16_t    leader_gms_port = 0;

    while(true) {
        int option_index = 0;
        int c = getopt_long(argc,argv,"p:i:g:h",long_options,&option_index);
        if (c == -1) {
            break;
        }
        switch (c) {
        case 'p':
            port = static_cast<uint16_t>(std::stoi(optarg));
            break;
        case 'i':
            leader_ip = optarg;
            break;
        case 'g':
            leader_gms_port = static_cast<uint16_t>(std::stoi(optarg));
            break;
        case 'h':
            print_help(argv[0]);
            return 0;
        case '?':
        default:
            std::cout << "skipping unknown argument." << std::endl;
        }
    }

    if (!check_leader_ip_and_port(leader_ip,leader_gms_port)) {
        std::cerr << "Invalid leader ip(" << leader_ip << ") and gms port(" 
                  << leader_gms_port << ")." << std::endl;
        return -1;
    }

    LeaderRegistryState leader_registry_state(leader_ip,leader_gms_port);

    ::rpc::server   server(port);
    server.bind("get",[&leader_registry_state]()->std::tuple<std::string,uint16_t,uint16_t> {
            return leader_registry_state.get();
        });

    server.bind("put",[&leader_registry_state](std::string ip, uint16_t gms_port, uint16_t ext_port)->bool {
            // TODO: check client ip/port
            return leader_registry_state.put(ip,gms_port,ext_port);
        });

    server.async_run(1);

    //TODO: command line API.
    server.stop();

    return 0;
}

}// derecho

int main(int argc, char** argv) {
    return derecho::main(argc, argv);
}
