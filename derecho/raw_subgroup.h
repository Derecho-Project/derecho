/**
 * @file raw_subgroup.h
 *
 * @date Feb 17, 2017
 * @author edward
 */

#pragma once

#include "view_manager.h"

namespace derecho {

class RawSubgroup {
private:
    const node_id_t node_id;
    const subgroup_id_t subgroup_id;
    ViewManager& group_view_manager;

public:
    RawSubgroup(node_id_t node_id, subgroup_id_t subgroup_id, ViewManager& view_manager) :
        node_id(node_id),
        subgroup_id(subgroup_id),
        group_view_manager(view_manager) {}

    /**
     * Gets a pointer into the send buffer for multicasts to this subgroup.
     * @param payload_size The size of the payload that the caller intends to
     * send, in bytes.
     * @param pause_sending_turns
     * @return
     */
    char* get_sendbuffer_ptr(unsigned long long int payload_size, int pause_sending_turns = 0);

    /**
     * Submits the contents of the send buffer to be sent on the next ordered
     * multicast to the subgroup.
     */
    void send();
};

}
