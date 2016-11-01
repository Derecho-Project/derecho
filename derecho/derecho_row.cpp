#include "derecho_row.h"
#include <cstring>
#include <atomic>

namespace derecho {

/**
 * Transfers data from the specified row of an existing DerechoSST to the local
 * row of this one. Specifically, copies changes, nChanges, nCommitted, and
 * nAcked, and initializes the other SST fields to 0/false.
 * @param existing_sst The SST instance to copy data from
 * @param row The target row in that SST instance (from which data will be copied)
 */
void DerechoSST::init_local_row_from_existing(const DerechoSST& existing_sst, const int row) {
    const int local_row = get_local_index();
    static thread_local std::mutex copy_mutex;
    std::unique_lock<std::mutex> lock(copy_mutex);
    memcpy(const_cast<node_id_t*>(changes[local_row]),
           const_cast<const node_id_t*>(existing_sst.changes[row]),
           existing_sst.changes.size() * sizeof(node_id_t));
    for(size_t i = 0; i < suspected.size(); ++i) {
        suspected[local_row][i] = false;
        globalMin[local_row][i] = 0;
    }
    nChanges[local_row] = existing_sst.nChanges[row];
    nCommitted[local_row] = existing_sst.nCommitted[row];
    nAcked[local_row] = existing_sst.nAcked[row];
    wedged[local_row] = false;
    globalMinReady[local_row] = false;
}

void DerechoSST::init_local_row_at_vid(const int vid) {
    int my_row = get_local_index();
    this->vid[my_row] = vid;
    for(size_t i = 0; i < suspected.size(); ++i) {
        suspected[my_row][i] = false;
        globalMin[my_row][i] = 0;
        changes[my_row][i] = 0;
    }
    memset(const_cast<char*>(joiner_ip[my_row]), 0, MAX_STRING_LEN);
    nChanges[my_row] = vid;
    nCommitted[my_row] = vid ;
    nAcked[my_row] = vid;
    wedged[my_row] = false;
    globalMinReady[my_row] = false;

}

std::string DerechoSST::to_string() const {
    const int row = get_local_index();
    std::stringstream s;
    s << "Vid=" << vid[row] << " ";
    s << "Suspected={ ";
    for(unsigned int n = 0; n < suspected.size(); n++) {
        s << (suspected[row][n] ? "T" : "F") << " ";
    }

    s << "}, nChanges=" << nChanges[row] << ", nCommitted=" << nCommitted[row];
    s << ", Changes={ ";
    for(int n = nCommitted[row]; n < nChanges[row]; n++) {
        s << changes[row][n % changes.size()];
    }
    s << " }, nAcked= " << nAcked[row] << ", nReceived={ ";
    for(unsigned int n = 0; n < nReceived.size(); n++) {
        s << nReceived[row][n] << " ";
    }

    s << "}"
      << ", Wedged = " << (wedged[row] ? "T" : "F") << ", GlobalMin = { ";
    for(unsigned int n = 0; n < globalMin.size(); n++) {
        s << globalMin[row][n] << " ";
    }

    s << "}, GlobalMinReady=" << globalMinReady[row] << "\n";
    return s.str();
}

namespace gmssst {

/**
 * Thread-safe setter for GMSTableRow members, specialized to the cstring type.
 * Conveniently hides away strcpy and c_str().
 * @param element A reference to the cstring member to set
 * @param value The value to set it to, as a C++ string.
 */
void set(volatile cstring& element, const std::string &value) {
    strcpy(const_cast<cstring &>(element), value.c_str());
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
