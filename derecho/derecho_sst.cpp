#include <cstring>
#include <atomic>
#include "derecho_sst.h"

namespace derecho {

void DerechoSST::init_local_row_from_previous(const DerechoSST& old_sst, const int row, const int num_changes_installed) {
    const int local_row = get_local_index();
    static thread_local std::mutex copy_mutex;
    std::unique_lock<std::mutex> lock(copy_mutex);
    //Copy elements [changes_installed...n] of the old changes array to the beginning of the new changes array
    memcpy(const_cast<node_id_t*>(changes[local_row]),
           const_cast<const node_id_t*>(old_sst.changes[row] + num_changes_installed),
           (old_sst.changes.size() - num_changes_installed) * sizeof(node_id_t));
    //Do the same thing with the joiner_ips arrays
    memcpy(const_cast<uint32_t*>(joiner_ips[local_row]),
           const_cast<const uint32_t*>(old_sst.joiner_ips[row] + num_changes_installed),
           (old_sst.joiner_ips.size() - num_changes_installed) * sizeof(uint32_t));
    for(size_t i = 0; i < suspected.size(); ++i) {
        suspected[local_row][i] = false;
        globalMinReady[local_row][i] = false;
    }
    for (size_t i = 0; i < globalMin.size(); ++i) {
        globalMin[local_row][i] = 0;
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
    memcpy(const_cast<node_id_t*>(changes[local_row]),
           const_cast<const node_id_t*>(changes[other_row]),
           changes.size() * sizeof(node_id_t));
    num_changes[local_row] = num_changes[other_row];
    num_committed[local_row] = num_committed[other_row];
    num_acked[local_row] = num_acked[other_row];
    num_installed[local_row] = num_installed[other_row];
}

std::string DerechoSST::to_string() const {
    std::stringstream s;
    uint num_rows = get_num_rows();
    for(uint row = 0; row < num_rows; ++row) {
        s << "Row=" << row << " ";
        s << "Vid=" << vid[row] << " ";
        s << "Suspected={ ";
        for(unsigned int n = 0; n < suspected.size(); n++) {
            s << (suspected[row][n] ? "T" : "F") << " ";
        }

        s << "}, num_changes=" << num_changes[row] << ", num_committed="
          << num_committed[row] << ", num_installed=" << num_installed[row];
        s << ", Changes={ ";
        for(int n = 0; n < (num_changes[row] - num_installed[row]); ++n) {
            s << changes[row][n] << " ";
        }
        s << "}, num_acked= " << num_acked[row] << ", nReceived={ ";
        for(unsigned int n = 0; n < num_received.size(); n++) {
            s << num_received[row][n] << " ";
        }
        s << "}"
          << ", Wedged = " << (wedged[row] ? "T" : "F") << ", GlobalMin = { ";
        for(unsigned int n = 0; n < globalMin.size(); n++) {
            s << globalMin[row][n] << " ";
        }

        s << "}, GlobalMinReady= { ";
        for(uint n = 0; n < globalMinReady.size(); n++) {
            s << globalMinReady[row] << " ";
        }
        s << "}" << std::endl;
    }
    return s.str();
}

namespace gmssst {

/**
 * Thread-safe setter for GMSTableRow members, specialized to the cstring type.
 * Conveniently hides away strcpy and c_str().
 * @param element A reference to the cstring member to set
 * @param value The value to set it to, as a C++ string.
 */
void set(volatile cstring& element, const std::string& value) {
    strcpy(const_cast<cstring&>(element), value.c_str());
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

/**
 * Equals operator for the C-strings used by GMSTableRow and C++ strings.
 * Hides the ugly const_cast to get rid of volatile marker.
 *
 * @param element A reference to a cstring
 * @param value A std::string value to compare it to.
 * @return True if the strings are equal
 */
bool equals(const volatile cstring& element, const std::string& value) {
    return strcmp(const_cast<const cstring&>(element), value.c_str()) == 0;
}

bool equals(const volatile char* string_array, const std::string& value) {
    return strcmp(const_cast<const char*>(string_array), value.c_str()) == 0;
}

}  // namespace gmssst
}  // namespace derecho
