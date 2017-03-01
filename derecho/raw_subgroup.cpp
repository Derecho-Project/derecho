/**
 * @file raw_subgroup.cpp
 *
 * @date Feb 17, 2017
 * @author edward
 */

#include "raw_subgroup.h"

namespace derecho {

char* RawSubgroup::get_sendbuffer_ptr(unsigned long long int payload_size, int pause_sending_turns) {
    return group_view_manager.get_sendbuffer_ptr(subgroup_id, payload_size, pause_sending_turns, false);
}

void RawSubgroup::send() {
    group_view_manager.send(subgroup_id);
}
}
