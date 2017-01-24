#ifndef DERECHO_ROW_H_
#define DERECHO_ROW_H_

#include <atomic>
#include <cstring>
#include <cstdint>
#include <mutex>
#include <string>
#include <sstream>
#include "sst/sst.h"

namespace derecho {

using ip_addr = std::string;
using node_id_t = uint32_t;

const int MAX_STRING_LEN = 50;
using cstring = char[MAX_STRING_LEN];

using sst::SSTField;
using sst::SSTFieldVector;
/**
 * The GMS and derecho_group will share the same SST for efficiency. This class
 * defines all the fields in this SST.
 */
class DerechoSST : public sst::SST<DerechoSST> {
public:
    // derecho_group members. Copy-pasted from derecho_group.h's
    // MessageTrackingRow
    /** This variable is the highest sequence number that has been received
     * in-order by this node; if a node updates seq_num, it has received all
     * messages up to seq_num in the global round-robin order. */
    SSTField<long long int> seq_num;
    /** This represents the highest sequence number that has been received
     * by every node, as observed by this node. If a node updates stable_num,
     * then it believes that all messages up to stable_num in the global
     * round-robin order have been received by every node. */
    SSTField<long long int> stable_num;
    /** This represents the highest sequence number that has been delivered
     * at this node. Messages are only delievered once stable, so it must be
     * at least stable_num. */
    SSTField<long long int> delivered_num;
    /** This represents the highest sequence number that has been persisted
     * to disk at this node, if persistence is enabled. Messages are only
     * persisted to disk once delivered to the application. */
    SSTField<long long int> persisted_num;

    /** View ID associated with this SST. VIDs monotonically increase as views change. */
    SSTField<int> vid;
    /** Array of same length as View::members, where each bool represents
     * whether the corresponding member is suspected to have failed */
    SSTFieldVector<bool> suspected;
    /** An array of the same length as View::members, containing a list of
     * proposed changes to the view that have not yet been installed. The number
     * of valid elements is num_changes - num_installed, which should never exceed
     * View::num_members/2.
     * If request i is a Join, changes[i] is not in current View's members.
     * If request i is a Departure, changes[i] is in current View's members. */
    SSTFieldVector<node_id_t> changes;
    /** If changes[i] is a Join, joiner_ips[i] is the IP address of the joining
     *  node, packed into an unsigned int in network byte order. This
     *  representation is necessary because SST doesn't support variable-length
     *  strings. */
    SSTFieldVector<uint32_t> joiner_ips;
    /** How many changes to the view have been proposed. Monotonically increases.
     * num_changes - num_committed is the number of pending changes, which should never
     * exceed the number of members in the current view. If num_changes == num_committed
     * == num_installed, no changes are pending. */
    SSTField<int> num_changes;
    /** How many proposed view changes have reached the commit point. */
    SSTField<int> num_committed;
    /** How many proposed changes have been seen. Incremented by a member
     * to acknowledge that it has seen a proposed change.*/
    SSTField<int> num_acked;
    /** How many previously proposed view changes have been installed in the
     * current view. Monotonically increases, lower bound on num_committed. */
    SSTField<int> num_installed;
    /** Local count of number of received messages by sender.  For each
     * sender k, nReceived[k] is the number received (a.k.a. "locally stable").
     */
    SSTFieldVector<long long int> nReceived;
    /** Set after calling rdmc::wedged(), reports that this member is wedged.
     * Must be after nReceived!*/
    SSTField<bool> wedged;
    /** Array of how many messages to accept from each sender, K1-able*/
    SSTFieldVector<int> globalMin;
    /** Must come after GlobalMin */
    SSTField<bool> globalMinReady;

    /**
     * Constructs an SST, and initializes the GMS fields to "safe" initial values
     * (0, false, etc.). Initializing the derecho_group fields is left to derecho_group.
     * @param parameters The SST parameters, which will be forwarded to the
     * standard SST constructor.
     */
    DerechoSST(const sst::SSTParams& parameters)
            : sst::SST<DerechoSST>(this, parameters),
              suspected(parameters.members.size()),
              changes(parameters.members.size()),
              joiner_ips(parameters.members.size()),
              nReceived(parameters.members.size()),
              globalMin(parameters.members.size()) {
        SSTInit(seq_num, stable_num, delivered_num,
                persisted_num, vid, suspected, changes, joiner_ips,
                num_changes, num_committed, num_acked, num_installed,
                nReceived, wedged, globalMin, globalMinReady);
        //Once superclass constructor has finished, table entries can be initialized
        for(int row = 0; row < get_num_rows(); ++row) {
            vid[row] = 0;
            for(size_t i = 0; i < parameters.members.size(); ++i) {
                suspected[row][i] = false;
                globalMin[row][i] = 0;
                changes[row][i] = 0;
            }
            memset(const_cast<uint32_t*>(joiner_ips[row]), 0, parameters.members.size());
            num_changes[row] = 0;
            num_committed[row] = 0;
            num_acked[row] = 0;
            wedged[row] = false;
            globalMinReady[row] = false;
        }
    }

    /**
     * Initializes the local row of this SST based on the specified row of the
     * previous View's SST. Copies num_changes, num_committed, and num_acked,
     * adds num_changes_installed to the previous value of num_installed, copies
     * (num_changes - num_changes_installed) elements of changes, and initializes
     * the other SST fields to 0/false.
     * @param old_sst The SST instance to copy data from
     * @param row The target row in that SST instance (from which data will be copied)
     * @param num_changes_installed The number of changes that were applied
     * when changing from the previous view to this one
     */
    void init_local_row_from_previous(const DerechoSST& old_sst, const int row, const int num_changes_installed);

    /**
     * Copies currently proposed changes and the various counter values associated
     * with them to the local row from some other row (i.e. the group leader's row).
     * @param other_row The row to copy values from.
     */
    void init_local_change_proposals(const int other_row);

    /**
     * Creates a string representation of the local row (not the whole table).
     * This should be converted to an ostream operator<< to follow standards.
     */
    std::string to_string() const;
};

namespace gmssst {

/**
 * Thread-safe setter for DerechoSST members; ensures there is a
 * std::atomic_signal_fence after writing the value.
 * @param e A reference to a member of GMSTableRow.
 * @param value The value to set that reference to.
 */
template <typename Elem>
void set(volatile Elem& e, const Elem& value) {
    e = value;
    std::atomic_signal_fence(std::memory_order_acq_rel);
}

/**
 * Thread-safe setter for DerechoSST members; ensures there is a
 * std::atomic_signal_fence after writing the value.
 * @param e A reference to a member of GMSTableRow.
 * @param value The value to set that reference to.
 */
template <typename Elem>
void set(volatile Elem& e, volatile const Elem& value) {
    e = value;
    std::atomic_signal_fence(std::memory_order_acq_rel);
}

/**
 * Thread-safe setter for DerechoSST members that are arrays; takes a lock
 * before running memcpy, and then ensures there is an atomic_signal_fence.
 * The first {@code length} members of {@code value} are copied to {@code array}.
 * @param array A pointer to the first element of an array that should be set
 * to {@code value}, obtained by calling SSTFieldVector::operator[]
 * @param value A pointer to the first element of an array to read values from
 * @param length The number of array elements to copy
 */
template <typename Elem>
void set(volatile Elem* array, volatile Elem* value, const size_t length) {
    static thread_local std::mutex set_mutex;
    {
        std::lock_guard<std::mutex> lock(set_mutex);
        memcpy(const_cast<Elem*>(array), const_cast<Elem*>(value),
               length * sizeof(Elem));
    }
    std::atomic_signal_fence(std::memory_order_acq_rel);
}
/**
 * Thread-safe setter for DerechoSST members that are arrays; takes a lock
 * before running memcpy, and then ensures there is an atomic_signal_fence.
 * This version copies the entire array, and assumes both arrays are the same
 * length.
 *
 * @param e A reference to an array-type member of GMSTableRow
 * @param value The array whose contents should be copied to this member
 */
template <typename Arr, size_t Len>
void set(volatile Arr(&e)[Len], const volatile Arr(&value)[Len]) {
    static thread_local std::mutex set_mutex;
    {
        std::lock_guard<std::mutex> lock(set_mutex);
        memcpy(const_cast<Arr(&)[Len]>(e), const_cast<const Arr(&)[Len]>(value),
               Len * sizeof(Arr));
        // copy_n just plain doesn't work, claiming that its argument types are
        // "not assignable"
        //        std::copy_n(const_cast<const Arr (&)[Len]>(value), Len,
        //        const_cast<Arr (&)[Len]>(e));
    }
    std::atomic_signal_fence(std::memory_order_acq_rel);
}

/**
 * Thread-safe setter for DerechoSST members that are arrays; takes a lock
 * before running memcpy, and then ensures there is an atomic_signal_fence.
 * This version only copies the first num elements of the source array.
 * @param dst
 * @param src
 * @param num
 */
template <size_t L1, size_t L2, typename Arr>
void set(volatile Arr(&dst)[L1], const volatile Arr(&src)[L2], const size_t& num) {
    static thread_local std::mutex set_mutex;
    {
        std::lock_guard<std::mutex> lock(set_mutex);
        memcpy(const_cast<Arr(&)[L2]>(dst), const_cast<const Arr(&)[L1]>(src),
               num * sizeof(Arr));
    }
    std::atomic_signal_fence(std::memory_order_acq_rel);
}

void set(volatile char* string_array, const std::string& value);

void set(volatile cstring& element, const std::string& value);

void increment(volatile int& member);

bool equals(const volatile cstring& element, const std::string& value);

bool equals(const volatile char& string_array, const std::string& value);

}  // namespace gmssst

}  // namespace derecho

#endif /* DERECHO_ROW_H_ */
