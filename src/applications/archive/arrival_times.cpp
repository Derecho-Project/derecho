#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <pthread.h>
#include <limits>

#include <derecho/core/derecho.hpp>
#include <derecho/sst/detail/poll_utils.hpp>
#include <derecho/sst/sst.hpp>
//Since all SST instances are named sst, we can use this convenient hack
#define LOCAL sst.get_local_index()

using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::ofstream;
using std::string;
using std::vector;

using namespace sst;

class mySST : public SST<mySST> {
public:
    mySST(const vector<uint32_t>& _members, uint32_t my_rank, long long unsigned int _msg_size = 1000) : SST<mySST>(this, SSTParams{_members, my_rank}),
                                                                msg(_msg_size) 
    {
        SSTInit(msg);
    }
    SSTFieldVector<char> msg;
};

int main() {
    // input number of nodes and the local node rank
    std::cout << "Enter node_rank and num_nodes" << std::endl;
    uint32_t node_rank, num_nodes;
    cin >> node_rank >> num_nodes;

    // input message size
    std::cout << "Enter message size" << std::endl;
    long long unsigned int msg_size;
    cin >> msg_size;

    // input message size
    std::cout << "Enter message number" << std::endl;
    uint msg_number;
    cin >> msg_number;

    std::cout << "Input the IP addresses" << std::endl;
    uint16_t port = 32567;
    // input the ip addresses
    map<uint32_t, std::pair<std::string, uint16_t>> ip_addrs_and_ports;
    for(uint i = 0; i < num_nodes; ++i) {
      std::string ip;
      cin >> ip;
      ip_addrs_and_ports[i] = {ip, port};
    }
    std::cout << "Using the default port value of " << port << std::endl;

    // initialize the rdma resources
#ifdef USE_VERBS_API
    verbs_initialize(ip_addrs_and_ports, node_rank);
#else
    lf_initialize(ip_addrs_and_ports, node_rank);
#endif

    // form a group with a subset of all the nodes
    vector<uint32_t> members(num_nodes);
    for(unsigned int i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    // create a new shared state table with all the members
    mySST sst(members, node_rank, msg_size);
    
    //messages sent/received
    uint sent = 0;
    uint last_received = 0;

    // initalization (?)
    for(uint i = 0; i < msg_size; i++)
        sst.msg[node_rank][i] = 0;
    sst.put((char*)std::addressof(sst.msg[0][0]) - sst.getBaseAddress(), msg_size*sizeof(char));
    

    //Vector of arrival times for each message
    std::vector<struct timespec> arrival_times(msg_number);

    //sender trigger
    auto sender_trigger =  [msg_size, msg_number, &sent] (mySST& sst) {
        
        sst.msg[0][msg_size-1] = (sst.msg[0][msg_size-1] + 1) % std::numeric_limits<char>::max();
        sst.put((char*)std::addressof(sst.msg[0][0]) - sst.getBaseAddress(), msg_size*sizeof(char));            
        ++sent;
        
        if(sent == msg_number) {
            sync(1);
        
#ifdef USE_VERBS_API
            verbs_destroy();
#else
            lf_destroy();
#endif
            exit(0);
        }
    };
    
    // receiver trigger
    // @param arrival_times: vector. Index i corresponds to the arrival of message i-th
    // @param msg_size: size of a single message
    // @param msg_number: total number of messages expected from the sender
    // @param last_received: index of the last received message
    // @param sent: actual number of messages received so far
    auto receiver_trigger = [&arrival_times, msg_size, msg_number, &last_received, &sent] (mySST& sst) {
        
        //read
        uint num_received = sst.msg[0][msg_size-1];

        //check if is new
        /* So far, I use a %256 arithmetic. So if I miss many messages,
         * it is possible that num received > last_received, but still
         * I have a message which is new. Then, we have a problem: what
         * if I have exactly 256 messages lost? I can't detect that there
         * is a new message. That's why I want to change this way to 
         * send the sequence number. But for now, that is.
         */
        if (num_received == last_received)
            return;

        // std::cout << num_received << " " << last_received << endl;

	// find the next number after sent which %256 will give num_received
	if (num_received > last_received) {
	  sent += (num_received - last_received);
	}
	else {
	  sent += (256 - last_received + num_received);
	}
        //Here I received a new message
        clock_gettime(CLOCK_REALTIME, &arrival_times[sent]);
        last_received = num_received;
        
        /* Also here: if I missed messages, how can I be sure
         * that I will enter this condition?
         */
        if(sent == msg_number) {                     
            double sum = 0.0;
            double time;
            ofstream fout;
            fout.open("arrival_times_record", ofstream::app);
            // compute the average and print values
            for(uint i = 1; i < sent; ++i) {
                time = ((arrival_times[i].tv_sec * 1e9 + arrival_times[i].tv_nsec) - (arrival_times[i-1].tv_sec * 1e9 + arrival_times[i-1].tv_nsec)) / 1e9;
                sum += time;
                fout << time << endl;
            }
                                
            fout << "Average inter-arrival time (" << msg_number << " msgs): " << (sum / msg_number)/1e9 << endl;
            fout.close();
            sync(0);        
#ifdef USE_VERBS_API
            verbs_destroy();
#else
            lf_destroy();
#endif
            exit(0);
        }
    };

    //sender
    if(LOCAL == 0) {
        sst.predicates.insert([](const mySST& sst){/*sleep(1);*/ return true;}, sender_trigger, PredicateType::RECURRENT);
    }
    //receiver
    else {  
        //start time
        clock_gettime(CLOCK_REALTIME, &arrival_times[0]);
        sst.predicates.insert([](const mySST& sst){return true;}, receiver_trigger, PredicateType::RECURRENT);
    }

    while(true) {
    }
    return 0;
}
