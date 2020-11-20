#include <memory>
#include <typeindex>
#include <vector>

#include <derecho/core/derecho.hpp>

/**
 * Enumeration of sender modes that some tests can run in. The options indicate
 * the number of members of the group that will be marked as senders.
 */
enum class PartialSendMode {
    ALL_SENDERS = 0,
    HALF_SENDERS,
    ONE_SENDER
};

/**
 * A subgroup allocation function that creates a single subgroup containing all
 * members of the View, but with only some of the members marked as senders. If
 * constructed with the HALF_SENDERS option, the highest-ranked half of the
 * View's members will be senders; if constructed with the ONE_SENDER option,
 * only the highest-ranked member of the View will be a sender.
 */
class PartialSendersAllocator {
    const int min_size;
    const PartialSendMode senders_option;
    const derecho::Mode ordering_mode;
    //Since the "senders" vector uses int instead of bool, these constants make it more readable
    static const int TRUE;
    static const int FALSE;

public:
    /**
     * Constructs a PartialSendersAllocator that requires the group to be a
     * certain minimum size and will use the specified behavior for marking
     * members as senders. It can also optionally set the "derecho::Mode"
     * (delivery order mode) of the group, which defaults to ORDERED.
     */
    PartialSendersAllocator(int min_size,
                            PartialSendMode senders_option,
                            derecho::Mode ordering_mode = derecho::Mode::ORDERED)
            : min_size(min_size), senders_option(senders_option), ordering_mode(ordering_mode) {}

    derecho::subgroup_allocation_map_t operator()(const std::vector<std::type_index>& subgroup_type_order,
                                                  const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view);
};