#include "derecho_row.h"
#include <cstring>
#include <atomic>

namespace derecho {
namespace gmssst {

/**
 * Thread-safe setter for GMSTableRow members, specialized to the cstring type.
 * Conveniently hides away strcpy and c_str().
 * @param element A reference to the cstring member to set
 * @param value The value to set it to, as a C++ string.
 */
void set(volatile cstring &element, const std::string &value) {
    strcpy(const_cast<cstring &>(element), value.c_str());
    std::atomic_signal_fence(std::memory_order_acq_rel);
}

/**
 * Thread-safe increment of an integer member of GMSTableRow; ensures there is
 * a std::atomic_signal_fence after updating the value.
 * @param member A reference to the member to increment.
 */
void increment(volatile int &member) {
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
bool equals(const volatile cstring &element, const std::string &value) {
    return strcmp(const_cast<const cstring &>(element), value.c_str()) == 0;
}

}  // namespace gmssst
}  // namespace derecho
