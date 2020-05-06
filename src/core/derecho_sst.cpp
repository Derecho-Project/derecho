#include <atomic>
#include <cstring>
#include <derecho/core/detail/derecho_sst.hpp>

namespace derecho {

void DerechoSST::init_local_row_from_previous(const DerechoSST& old_sst, const int row, const int num_changes_installed) {
    const int local_row = get_local_index();
    static thread_local std::mutex copy_mutex;
    std::unique_lock<std::mutex> lock(copy_mutex);
    //Copy elements [changes_installed...n] of the old changes array to the beginning of the new changes array
    memcpy(const_cast<ChangeProposal*>(changes[local_row]),
           const_cast<const ChangeProposal*>(old_sst.changes[row] + num_changes_installed),
           (old_sst.changes.size() - num_changes_installed) * sizeof(ChangeProposal));
    //Do the same thing with the joiner_ips arrays and joiner_xxx_ports arrays
    memcpy(const_cast<uint32_t*>(joiner_ips[local_row]),
           const_cast<const uint32_t*>(old_sst.joiner_ips[row] + num_changes_installed),
           (old_sst.joiner_ips.size() - num_changes_installed) * sizeof(uint32_t));
    memcpy(const_cast<uint16_t*>(joiner_gms_ports[local_row]),
           const_cast<const uint16_t*>(old_sst.joiner_gms_ports[row] + num_changes_installed),
           (old_sst.joiner_gms_ports.size() - num_changes_installed) * sizeof(uint16_t));
    memcpy(const_cast<uint16_t*>(joiner_rpc_ports[local_row]),
           const_cast<const uint16_t*>(old_sst.joiner_rpc_ports[row] + num_changes_installed),
           (old_sst.joiner_rpc_ports.size() - num_changes_installed) * sizeof(uint16_t));
    memcpy(const_cast<uint16_t*>(joiner_sst_ports[local_row]),
           const_cast<const uint16_t*>(old_sst.joiner_sst_ports[row] + num_changes_installed),
           (old_sst.joiner_sst_ports.size() - num_changes_installed) * sizeof(uint16_t));
    memcpy(const_cast<uint16_t*>(joiner_rdmc_ports[local_row]),
           const_cast<const uint16_t*>(old_sst.joiner_rdmc_ports[row] + num_changes_installed),
           (old_sst.joiner_rdmc_ports.size() - num_changes_installed) * sizeof(uint16_t));
    memcpy(const_cast<uint16_t*>(joiner_external_ports[local_row]),
           const_cast<const uint16_t*>(old_sst.joiner_external_ports[row] + num_changes_installed),
           (old_sst.joiner_external_ports.size() - num_changes_installed) * sizeof(uint16_t));
    for(size_t i = 0; i < suspected.size(); ++i) {
        suspected[local_row][i] = false;
    }
    for(size_t i = 0; i < global_min_ready.size(); ++i) {
        global_min_ready[local_row][i] = false;
    }
    for(size_t i = 0; i < global_min.size(); ++i) {
        global_min[local_row][i] = 0;
    }
    num_changes[local_row] = old_sst.num_changes[row];
    num_committed[local_row] = old_sst.num_committed[row];
    num_acked[local_row] = old_sst.num_acked[row];
    num_installed[local_row] = old_sst.num_installed[row] + num_changes_installed;
    wedged[local_row] = false;
}

void DerechoSST::init_local_change_proposals(const int other_row) {
    const int local_row = get_local_index();
    static thread_local std::mutex copy_mutex;
    std::unique_lock<std::mutex> lock(copy_mutex);
    memcpy(const_cast<ChangeProposal*>(changes[local_row]),
           const_cast<const ChangeProposal*>(changes[other_row]),
           changes.size() * sizeof(ChangeProposal));
    memcpy(const_cast<uint32_t*>(joiner_ips[local_row]),
           const_cast<const uint32_t*>(joiner_ips[other_row]),
           joiner_ips.size() * sizeof(uint32_t));
    memcpy(const_cast<uint16_t*>(joiner_gms_ports[local_row]),
           const_cast<const uint16_t*>(joiner_gms_ports[other_row]),
           joiner_gms_ports.size() * sizeof(uint16_t));
    memcpy(const_cast<uint16_t*>(joiner_rpc_ports[local_row]),
           const_cast<const uint16_t*>(joiner_rpc_ports[other_row]),
           joiner_rpc_ports.size() * sizeof(uint16_t));
    memcpy(const_cast<uint16_t*>(joiner_sst_ports[local_row]),
           const_cast<const uint16_t*>(joiner_sst_ports[other_row]),
           joiner_sst_ports.size() * sizeof(uint16_t));
    memcpy(const_cast<uint16_t*>(joiner_rdmc_ports[local_row]),
           const_cast<const uint16_t*>(joiner_rdmc_ports[other_row]),
           joiner_rdmc_ports.size() * sizeof(uint16_t));
    memcpy(const_cast<uint16_t*>(joiner_external_ports[local_row]),
           const_cast<const uint16_t*>(joiner_external_ports[other_row]),
           joiner_external_ports.size() * sizeof(uint16_t));
    num_changes[local_row] = num_changes[other_row];
    num_committed[local_row] = num_committed[other_row];
    num_acked[local_row] = num_acked[other_row];
    num_installed[local_row] = num_installed[other_row];
}

std::string DerechoSST::to_string() const {
    std::stringstream s;
    uint num_rows = get_num_rows();
    for(uint row = 0; row < num_rows; ++row) {
        s << "row=" << row << " ";
        s << "vid=" << vid[row] << " ";
        s << "suspected={ ";
        for(unsigned int n = 0; n < suspected.size(); n++) {
            s << (suspected[row][n] ? "T" : "F") << " ";
        }

        s << "}, num_changes=" << num_changes[row] << ", num_committed="
          << num_committed[row] << ", num_installed=" << num_installed[row];
        s << ", changes={ ";
        for(int n = 0; n < (num_changes[row] - num_installed[row]); ++n) {
            s << "(" << changes[row][n].change_id << "," << changes[row][n].leader_id << ") ";
        }
        s << "}, num_acked= " << num_acked[row] << ", num_received={ ";
        for(unsigned int n = 0; n < num_received.size(); n++) {
            s << num_received[row][n] << " ";
        }
        s << "}, joiner_ips={ ";
        for(int n = 0; n < (num_changes[row] - num_installed[row]); ++n) {
            s << joiner_ips[row][n] << " ";
        }
        s << "}, joiner_gms_ports={ ";
        for(int n = 0; n < (num_changes[row] - num_installed[row]); ++n) {
            s << joiner_gms_ports[row][n] << " ";
        }
        s << "}, joiner_rpc_ports={ ";
        for(int n = 0; n < (num_changes[row] - num_installed[row]); ++n) {
            s << joiner_rpc_ports[row][n] << " ";
        }
        s << "}, joiner_sst_ports={ ";
        for(int n = 0; n < (num_changes[row] - num_installed[row]); ++n) {
            s << joiner_sst_ports[row][n] << " ";
        }
        s << "}, joiner_rdmc_ports={ ";
        for(int n = 0; n < (num_changes[row] - num_installed[row]); ++n) {
            s << joiner_rdmc_ports[row][n] << " ";
        }
        s << "}, joiner_external_ports={ ";
        for(int n = 0; n < (num_changes[row] - num_installed[row]); ++n) {
            s << joiner_external_ports[row][n] << " ";
        }
        s << "}, seq_num={ ";
        for(unsigned int n = 0; n < seq_num.size(); n++) {
            s << seq_num[row][n] << " ";
        }
        s << "}"
          << ", delivered_num={ ";
        for(unsigned int n = 0; n < delivered_num.size(); n++) {
            s << delivered_num[row][n] << " ";
        }
        s << "}"
          << ", wedged = " << (wedged[row] ? "T" : "F") << ", global_min = { ";
        for(unsigned int n = 0; n < global_min.size(); n++) {
            s << global_min[row][n] << " ";
        }

        s << "}, global_min_ready= { ";
        for(uint n = 0; n < global_min_ready.size(); n++) {
            s << global_min_ready[row] << " ";
        }
        s << "}"
          << ", rip = " << rip[row] << std::endl;
    }
    return s.str();
}

namespace gmssst {

/**
 * Thread-safe setter for DerechoSST members of type ChangeProposal
 * @param member A reference to a ChangeProposal in the SST
 * @param value The value to assign to the ChangeProposal
 */
void set(volatile ChangeProposal& member, const ChangeProposal& value) {
    member.change_id = value.change_id;
    member.leader_id = value.leader_id;
    member.end_of_view = value.end_of_view;
    std::atomic_signal_fence(std::memory_order_acq_rel);
}

/**
 * Thread-safe setter for DerechoSST members that use SSTFieldVector<char> to
 * represent strings. Conveniently hides away strcpy and c_str().
 * @param string_array A pointer to the first element of the char array
 * @param value The value to set the array to, as a C++ string
 */
void set(volatile char* string_array, const std::string& value) {
    strcpy(const_cast<char*>(string_array), value.c_str());
    std::atomic_signal_fence(std::memory_order_acq_rel);
}

/**
 * Thread-safe increment of an integer member of GMSTableRow; ensures there is
 * a std::atomic_signal_fence after updating the value.
 * @param member A reference to the member to increment.
 */
void increment(volatile int& member) {
    member++;
    std::atomic_signal_fence(std::memory_order_acq_rel);
}

bool equals(const volatile char* string_array, const std::string& value) {
    return strcmp(const_cast<const char*>(string_array), value.c_str()) == 0;
}

}  // namespace gmssst
}  // namespace derecho
