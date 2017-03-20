#ifndef INITIALIZE_H
#define INITIALIZE_H

#include <cstdint>
#include <map>
#include <string>
#include <unordered_set>
#include <iterator>

std::map<uint32_t, std::string> initialize(uint32_t& node_rank, uint32_t& num_nodes);

/**
 * Utility method for initializing experiments. Prompts the terminal for the
 * current node's ID and IP, and the group leader's IP, writing the results
 * into the three parameters.
 */
void query_node_info(uint32_t& node_id, std::string& node_ip, std::string& leader_ip);

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
