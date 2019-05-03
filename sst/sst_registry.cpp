#include "sst_registry.hpp"

namespace sst {
std::set<_SST*> SSTRegistry::ssts;
std::mutex SSTRegistry::ssts_mutex;
std::thread SSTRegistry::predicate_thread(SSTRegistry::evaluate);

void SSTRegistry::register_sst(_SST* sst) {
    std::lock_guard<std::mutex> lock(ssts_mutex);
    ssts.insert(sst);
}

void SSTRegistry::deregister_sst(_SST* sst) {
    std::lock_guard<std::mutex> lock(ssts_mutex);
    ssts.erase(sst);
}

void SSTRegistry::evaluate() {
    SSTRegistry::predicate_thread.detach();
    while(true) {
        std::lock_guard<std::mutex> lock(ssts_mutex);
        for(auto sst : ssts) {
            sst->evaluate();
        }
    }
}

} /* namespace sst */
