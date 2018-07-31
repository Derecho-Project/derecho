#include "../sst.h"
#include "../verbs.h"

using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::vector;

class mySST : public sst::SST<mySST> {
public:
    mySST(const vector<uint32_t>& _members, uint32_t my_id) : SST(this, sst::SSTParams{_members, my_id}) {
        SSTInit(a);
    }
    sst::SSTField<int> a;
};

int main() {
    // input number of nodes and the local node id
    uint32_t num_nodes, my_id;
    cin >> my_id >> num_nodes;

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
    int b = 5 + my_id;
    sst.a(my_id, b);
    sst.put();
    sst::sync(1 - my_id);
    int n;
    cin >> n;
    for(uint i = 0; i < num_nodes; ++i) {
        cout << sst.a(i) << endl;
    }
    while(true) {
    }
}
