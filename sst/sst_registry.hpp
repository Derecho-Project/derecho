#pragma once

#include <mutex>
#include <set>
#include <thread>

namespace sst {
class _SST;
class SSTRegistry;

class _SST {
    friend class SSTRegistry;
    virtual void evaluate() = 0;
};

class SSTRegistry {
    static std::set<_SST*> ssts;
    static std::mutex ssts_mutex;
    static std::thread predicate_thread;

public:
    static void register_sst(_SST* sst);
    static void deregister_sst(_SST* sst);
    static void evaluate();
};

}  // namespace sst
