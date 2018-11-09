#ifndef INITIALIZE_H
#define INITIALIZE_H

#include <cstdint>
#include <iterator>
#include <map>
#include <string>
#include <unordered_set>

std::map<uint32_t, std::string> initialize(uint32_t& node_rank, uint32_t& num_nodes);

/**
 * Utility method for initializing experiments. Prompts the terminal for the
 * group leader's IP and GMS port, writing the results
 * into the two parameters.
 */
void query_node_info(ip_addr_t& leader_ip, uint16_t& leader_gms_port);

//Just sticking this here for now (because it's only used by experiments),
//but it should really go in a generic "utilities" header
template <typename InputIt, typename OutputIt, typename Elem>
OutputIt unordered_intersection(InputIt first, InputIt last, std::unordered_set<Elem> filter, OutputIt output) {
    while(first != last) {
        if(filter.count(*first) > 0) {
            *output++ = *first;
        }
        first++;
    }
    return output;
}

#endif
