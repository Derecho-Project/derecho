#ifndef DERECHO_ROW_H_
#define DERECHO_ROW_H_

#include <atomic>
#include <cstring>
#include <mutex>
#include <string>
#include <sstream>

namespace derecho {

using ip_addr = std::string;
using node_id_t = uint32_t;

using cstring = char[50];

/**
 * The GMS and derecho_group will share the same SST for efficiency
 */
template <unsigned int N>
struct DerechoRow {
    // derecho_group members. Copy-pasted from derecho_group.h's
    // MessageTrackingRow
    /** This variable is the highest sequence number that has been received
     * in-order by this node; if a node updates seq_num, it has received all
     * messages up to seq_num in the global round-robin order. */
    long long int seq_num;
    /** This represents the highest sequence number that has been received
     * by every node, as observed by this node. If a node updates stable_num,
     * then it believes that all messages up to stable_num in the global
     * round-robin order have been received by every node. */
    long long int stable_num;
    /** This represents the highest sequence number that has been delivered
     * at this node. Messages are only delievered once stable, so it must be
     * at least stable_num. */
    long long int delivered_num;
    /** This represents the highest sequence number that has been persisted
     * to disk at this node, if persistence is enabled. Messages are only
     * persisted to disk once delivered to the application. */
    long long int persisted_num;

    /** View ID associated with this SST */
    int vid;
    /** Array of same length as View::members, where each bool represents
     * whether the corresponding member is suspected to have failed */
    bool suspected[N];
    /** An array of nChanges proposed changes to the view (the number of non-
     * empty elements is nChanges). The total number of changes never exceeds
     * N/2. If request i is a Join, changes[i] is not in current View's
     * members. If request i is a Departure, changes[i] is in current View's
     * members. */
    node_id_t changes[N];  // If the total never exceeds N/2, why is this N?
    /** If the next pending view change include a join, this is the IP address
     *  of the joining node. */
    cstring joiner_ip;
    /** How many changes to the view are pending. */
    int nChanges;
    /** How many proposed view changes have reached the commit point. */
    int nCommitted;
    /** How many proposed changes have been seen. Incremented by a member
     * to acknowledge that it has seen a proposed change.*/
    int nAcked;
    /** Local count of number of received messages by sender.  For each
     * sender k, nReceived[k] is the number received (a.k.a. "locally stable").
     */
    long long int nReceived[N];
    /** Set after calling rdmc::wedged(), reports that this member is wedged.
     * Must be after nReceived!*/
    bool wedged;
    /** Array of how many messages to accept from each sender, K1-able*/
    int globalMin[N];
    /** Must come after GlobalMin */
    bool globalMinReady;
};

template <unsigned int N>
using GMSTableRow = DerechoRow<N>;

// static_assert(std::is_pod<DerechoRow<10>>::value, "Error! Row type must be
// POD.");

/**
 * Utility functions for manipulating GMSTableRow objects; SST rows can't have
 * instance methods, but this one needs them.
 */
namespace gmssst {

/**
 * Thread-safe setter for GMSTableRow members; ensures there is a
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
 * Thread-safe setter for GMSTableRow members; ensures there is a
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
 * Thread-safe setter for GMSTableRow members that are arrays; takes a lock
 * before running memcpy, and then ensures there is an atomic_signal_fence.
 * This version copies the entire array, and assumes both arrays are the same
 *length.
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
        // "not
        // assignable"
        //        std::copy_n(const_cast<const Arr (&)[Len]>(value), Len,
        //        const_cast<Arr (&)[Len]>(e));
    }
    std::atomic_signal_fence(std::memory_order_acq_rel);
}

/**
 * Thread-safe setter for GMSTableRow members that are arrays; takes a lock
 * before running memcpy, and then ensures there is an atomic_signal_fence.
 * This version only copies the first num elements of the source array.
 * @param dst
 * @param src
 * @param num
 */
template <size_t L1, size_t L2, typename Arr>
void set(volatile Arr(&dst)[L1], const volatile Arr(&src)[L2],
         const size_t& num) {
    static thread_local std::mutex set_mutex;
    {
        std::lock_guard<std::mutex> lock(set_mutex);
        memcpy(const_cast<Arr(&)[L2]>(dst), const_cast<const Arr(&)[L1]>(src),
               num * sizeof(Arr));
    }
    std::atomic_signal_fence(std::memory_order_acq_rel);
}

/**
 * Constructs a DerechoRow for a system that does not start at View ID 0 (which
 * is the case when recovering from logs). Initializes the "committed" and
 * "acked" counters to the given VID so that all views up to that ID are
 * considered already stable.
 *
 * @param newRow The instance of DerechoRow to initialize
 * @param vid The VID of the current View upon initialization
 */
template <unsigned int N>
void init(volatile DerechoRow<N>& newRow, const int vid) {
    newRow.vid = vid;
    for(size_t i = 0; i < N; ++i) {
        newRow.suspected[i] = false;
        newRow.globalMin[i] = 0;
        // This will be initialized by DerechoGroup
        //        newRow.nReceived[i] = -1;
        newRow.changes[i] = 0;
    }
    newRow.nChanges = vid;
    newRow.nCommitted = vid;
    newRow.nAcked = vid;
    newRow.wedged = false;
    newRow.globalMinReady = false;
}

/**
 * Pseudo-constructor for DerechoRow, to work around the fact that POD can't
 * have constructors. Ensures that all members are initialized to a safe initial
 * value (i.e. suspected[] is all false), including setting changes[] to empty
 * C-strings.
 * @param newRow The instance of DerechoRow to initialize.
 */
template <unsigned int N>
void init(volatile DerechoRow<N>& newRow) {
    init(newRow, 0);
}

/**
 * Transfers data from one DerechoRow instance to another; specifically, copies
 * changes, nChanges, nCommitted, and nAcked.
 * @param newRow The instance to initialize.
 * @param existingRow The instance to copy data from.
 */
template <unsigned int N>
void init_from_existing(volatile GMSTableRow<N>& newRow,
                        const volatile GMSTableRow<N>& existingRow) {
    static thread_local std::mutex copy_mutex;
    std::unique_lock<std::mutex> lock(copy_mutex);
    memcpy(const_cast<node_id_t*>(newRow.changes),
           const_cast<const node_id_t*>(existingRow.changes),
           N * sizeof(node_id_t));
    for(size_t i = 0; i < N; ++i) {
        newRow.suspected[i] = false;
        newRow.globalMin[i] = 0;
        // This will be initialized by DerechoGroup
        //        newRow.nReceived[i] = -1;
    }
    newRow.nChanges = existingRow.nChanges;
    newRow.nCommitted = existingRow.nCommitted;
    newRow.nAcked = existingRow.nAcked;
    newRow.wedged = false;
    newRow.globalMinReady = false;
}

template <unsigned int N>
std::string to_string(volatile const DerechoRow<N>& row) {
    std::stringstream s;
    s << "Vid=" << row.vid << " ";
    s << "Suspected={ ";
    for(unsigned int n = 0; n < N; n++) {
        s << (row.suspected[n] ? "T" : "F") << " ";
    }

    s << "}, nChanges=" << row.nChanges << ", nCommitted=" << row.nCommitted;
    s << ", Changes={ ";
    for(int n = row.nCommitted; n < row.nChanges; n++) {
        s << row.changes[n % N];
    }
    s << " }, nAcked= " << row.nAcked << ", nReceived={ ";
    for(unsigned int n = 0; n < N; n++) {
        s << row.nReceived[n] << " ";
    }

    s << "}"
      << ", Wedged = " << (row.wedged ? "T" : "F") << ", GlobalMin = { ";
    for(unsigned int n = 0; n < N; n++) {
        s << row.globalMin[n] << " ";
    }

    s << "}, GlobalMinReady=" << row.globalMinReady << "\n";
    return s.str();
}

void set(volatile cstring& element, const std::string& value);

void increment(volatile int& member);

bool equals(const volatile cstring& element, const std::string& value);

}  // namespace gmssst

}  // namespace derecho

#endif /* DERECHO_ROW_H_ */
