/**
 * @file raw_subgroup.h
 *
 * @date Feb 17, 2017
 */

#pragma once

#include "derecho_exception.h"
#include "derecho_internal.h"
#include "view_manager.h"

namespace derecho {

class RawSubgroup {
private:
    const node_id_t node_id;
    const subgroup_id_t subgroup_id;
    ViewManager& group_view_manager;
    bool valid;

public:
    RawSubgroup(node_id_t node_id,
                subgroup_id_t subgroup_id,
                ViewManager& view_manager)
            : node_id(node_id),
              subgroup_id(subgroup_id),
              group_view_manager(view_manager),
              valid(true) {}

    RawSubgroup(node_id_t node_id, ViewManager& view_manager) : node_id(node_id),
                                                                subgroup_id(0),
                                                                group_view_manager(view_manager),
                                                                valid(false) {}

    /**
     * @return True if this RawSubgroup is a valid reference to a raw subgroup,
     * false if it is "empty" because this node is not a member of the raw
     * subgroup this object represents.
     */
    bool is_valid() const { return valid; }

    uint64_t compute_global_stability_frontier();

    /**
     * Submits a call to send as the next ordered
     * multicast to the subgroup.
     */
    void send(unsigned long long int payload_size, const std::function<void(char* buf)>& msg_generator);
};
}  // namespace derecho
