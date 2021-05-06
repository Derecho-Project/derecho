#pragma once

#include <functional>

#include "derecho_sst.hpp"
#include "derecho_internal.hpp"

namespace derecho {
enum PREDICATE_TYPE {
    SEND = 0,
    RECEIVE,
    DELIVERY,
    // the following three are irrelevant for our purposes
    PERSISTENCE,
    VERIFIED,
    RDMC_SEND
};

std::pair<subgroup_id_t, PREDICATE_TYPE> round_robin_policy(const DerechoSST& sst);

static const std::function<std::pair<subgroup_id_t, PREDICATE_TYPE>(const DerechoSST&)> evaluation_policy = [](const DerechoSST& sst) {
    return round_robin_policy(sst);
};
}  // namespace derecho
