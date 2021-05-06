#include <derecho/core/detail/evaluation_policy.hpp>

namespace derecho {
std::pair<subgroup_id_t, PREDICATE_TYPE> round_robin_policy(const DerechoSST& sst) {
    subgroup_id_t subgroup_num = 0;
    static uint64_t count = 0;
    PREDICATE_TYPE type = static_cast<PREDICATE_TYPE>(count++ % 3);
    return std::pair{subgroup_num, type};
}
}  // namespace derecho
