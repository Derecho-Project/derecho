#include <functional>
#include <iostream>
#include <stdlib.h>

#include "../sst.h"
#include "../verbs.h"

using std::cin;
using std::cout;
using std::endl;
using std::vector;
using std::map;

class mySST : public sst::SST<mySST> {
public:
    mySST(const vector<uint32_t>& _members, uint32_t my_id) : SST<mySST>(this, sst::SSTParams{_members, my_id}) {
      SSTInit(a);
    }
    sst::SSTField<int> a;
};

int main() {
    using namespace sst;

    // input number of nodes and the local node id
    uint32_t num_nodes, my_id;
    cin >> num_nodes >> my_id;

    // input the ip addresses
    map<uint32_t, std::string> ip_addrs;
    for(size_t i = 0; i < num_nodes; ++i) {
        cin >> ip_addrs[i];
    }

    // initialize the rdma resources
    sst::verbs_initialize(ip_addrs, my_id);

    vector<uint32_t> members(num_nodes);
    for(uint i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    mySST sst(members, my_id);
    sst.a(my_id, 0);
    sst.put();
    // will add sync later
    cout << "First input" << endl;
    int n;
    cin >> n;

    auto f = [my_id](const mySST& sst) {
      return sst.a[1 - my_id] != 0;
    };

    if(my_id == 0) {
        auto g = [my_id](mySST& sst) {
            sst.a[my_id] = 1;
            sst.put();
            cout << "Exiting" << endl;
            exit(0);
        };

        sst.predicates.insert(f, g);
    }

    else {
        auto g = [my_id](mySST& sst) {
            cout << "Exiting" << endl;
            exit(0);
        };

        sst.predicates.insert(f, g);

        cout << "Second input" << endl;
        int k;
        cin >> k;
        sst.a(my_id, 1);
        sst.put();
    }

    cout << "Waiting" << endl;
    while(true) {
    }
}
