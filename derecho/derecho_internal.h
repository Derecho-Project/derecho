/**
 * This file include all common types internal to derecho and 
 * not necessarily being known by a client program.
 *
 */
#pragma once
#ifndef DERECHO_INTERNAL_H
#define DERECHO_INTERNAL_H

#include <functional>
#include <map>
#include <sys/types.h>

#include "persistent/HLC.hpp"

namespace derecho {
// for messages
using subgroup_id_t = uint32_t;
using message_id_t = int64_t;
// This should be visible to the user.
using persistence_version_t = int64_t;

// for persistence manager
using persistence_manager_make_version_func_t = std::function<void(
        const subgroup_id_t &,
        const persistence_version_t &,
        const HLC &)>;
using persistence_manager_post_persist_func_t = std::function<void(
        const subgroup_id_t &,
        const persistence_version_t &)>;
using persistence_manager_callbacks_t = std::tuple<persistence_manager_make_version_func_t, persistence_manager_post_persist_func_t>;
}

#endif  //DERECHO_INTERNAL_H
