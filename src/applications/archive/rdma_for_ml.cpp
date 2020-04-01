#include <cstdlib>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <unistd.h>
#include <vector>

#include <derecho/core/derecho.hpp>

#include <derecho/sst/detail/poll_utils.hpp>
#include <derecho/sst/sst.hpp>

using namespace derecho;
using namespace sst;
using namespace std;

class MLSST : public SST<MLSST> {
public:
    MLSST(const std::vector<uint32_t>& members, uint32_t my_id, uint32_t dimension)
            : SST<MLSST>(this, SSTParams{members, my_id}),
              ml_parameters(dimension) {
        SSTInit(ml_parameters, round);
    }
    SSTFieldVector<double> ml_parameters;
    SSTField<uint64_t> round;
};

void print(const MLSST& sst) {
    for(uint row = 0; row < sst.get_num_rows(); ++row) {
        for(uint param = 0; param < sst.ml_parameters.size(); ++param) {
            cout << sst.ml_parameters[row][param] << " ";
        }
        cout << endl;

        cout << sst.round[row] << endl;
    }
    cout << endl;
}

int main(int argc, char* argv[]) {
    srand(getpid());

    if(argc < 3) {
        cout << "Usage: " << argv[0] << " <num_nodes> <num_params>" << endl;
        return -1;
    }

    // the number of nodes for this test
    const uint32_t num_nodes = std::stoi(argv[1]);
    const uint32_t num_params = std::stoi(argv[1]);

    uint32_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    
    // input the ip addresses
    std::map<uint32_t, std::pair<std::string, uint16_t>> ip_addrs_and_ports;
    for(uint i = 0; i < num_nodes; ++i) {
        std::string ip;
	uint16_t port;
        std::cout << "Input the IP address for node " << i << ": ";
        std::cin >> ip;
	std::cout << "Input the port number for node " << i << ": ";
	std::cin >> port;
        ip_addrs_and_ports[i] = {ip, port};
    }

    // initialize the rdma resources
#ifdef USE_VERBS_API
    verbs_initialize(ip_addrs_and_ports, {}, my_id);
#else
    lf_initialize(ip_addrs_and_ports, {}, my_id);
#endif

    // form a group with a subset of all the nodes
    std::vector<uint32_t> members(num_nodes);
    for(unsigned int i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    MLSST sst(members, my_id, num_params);
    uint32_t my_rank = sst.get_local_index();
    // initialization
    for(uint param = 0; param < sst.ml_parameters.size(); ++param) {
        sst.ml_parameters[my_rank][param] = 0;
    }
    sst.round[my_rank] = 0;
    sst.sync_with_members();

    uint32_t server_rank = 0;

    if(my_rank == server_rank) {
        std::function<bool(const MLSST&)> round_complete = [my_rank, server_rank](const MLSST& sst) {
            for(uint row = 0; row < sst.get_num_rows(); ++row) {
                // ignore server row
                if(row == server_rank) {
                    continue;
                }
                if(sst.round[row] == sst.round[my_rank]) {
                    return false;
                }
            }
            return true;
        };

        std::function<void(MLSST&)> compute_average = [my_rank, server_rank](MLSST& sst) {
            print(sst);
            for(uint param = 0; param < sst.ml_parameters.size(); ++param) {
                double sum = 0;
                for(uint row = 0; row < sst.get_num_rows(); ++row) {
                    // ignore server row
                    if(row == server_rank) {
                        continue;
                    }
                    sum += sst.ml_parameters[row][param];
                }
                sst.ml_parameters[my_rank][param] = sum / (sst.get_num_rows() - 1);
            }
            sst.put_with_completion((char*)std::addressof(sst.ml_parameters[0][0]) - sst.getBaseAddress(), sizeof(sst.ml_parameters[0][0]) * sst.ml_parameters.size());
            sst.round[my_rank]++;
            sst.put_with_completion((char*)std::addressof(sst.round[0]) - sst.getBaseAddress(), sizeof(sst.round[0]));
        };

        sst.predicates.insert(round_complete, compute_average, PredicateType::RECURRENT);
    }

    else {
        std::function<bool(const MLSST&)> server_done = [my_rank, server_rank](const MLSST& sst) {
            return sst.round[server_rank] == sst.round[my_rank];
        };

        std::function<void(MLSST&)> compute_new_parameters = [my_rank](MLSST& sst) {
            print(sst);
            for(uint param = 0; param < sst.ml_parameters.size(); ++param) {
                sst.ml_parameters[my_rank][param] = rand() % 100;
            }
            sst.put_with_completion((char*)std::addressof(sst.ml_parameters[0][0]) - sst.getBaseAddress(), sizeof(sst.ml_parameters[0][0]) * sst.ml_parameters.size());
            sst.round[my_rank]++;
            sst.put_with_completion((char*)std::addressof(sst.round[0]) - sst.getBaseAddress(), sizeof(sst.round[0]));
        };

        sst.predicates.insert(server_done, compute_new_parameters, PredicateType::RECURRENT);
    }

    while(true) {
    }
}
