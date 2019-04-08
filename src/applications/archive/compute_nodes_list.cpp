#include <cassert>

#include "compute_nodes_list.hpp"

using namespace std;

list<pair<int32_t, int32_t>> compute_sequence(int32_t start, int32_t end) {
    if(start + 2 == end) {
        return {{start, start + 1}};
    }
    auto list1 = compute_sequence(start, (start + end) / 2);
    auto list2 = compute_sequence((start + end) / 2, end);
    list1.splice(list1.end(), list2);
    for(int32_t j = 0; j < (end - start) / 2; j++) {
        for(int32_t i = 0; i < (end - start) / 2; i++) {
            list1.push_back(
                    {start + i, (end + start) / 2 + ((i + j) % ((end - start) / 2))});
        }
    }
    return list1;
}

vector<int32_t> compute_nodes_list(int32_t my_id, int32_t num_nodes) {
    assert(my_id >= 0 && my_id < num_nodes);
    int32_t n = num_nodes;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n++;
    auto sequence_list = compute_sequence(0, n);
    vector<int32_t> nodes_list;
    for(auto p : sequence_list) {
        if(p.first == my_id) {
            if(p.second < num_nodes) {
                nodes_list.push_back(p.second);
            }
        } else if(p.second == my_id) {
            if(p.first < num_nodes) {
                nodes_list.push_back(p.first);
            }
        }
    }
    return nodes_list;
}
