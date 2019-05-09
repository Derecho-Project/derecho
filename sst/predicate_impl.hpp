#pragma once

#include "predicate.hpp"

namespace sst {
/**
 * Inserts the predicate trigger pair in the right list based on the predicate type.
 * @param predicate The predicate to insert.
 * @param trigger The trigger to execute when the predicate is true.
 * @param type The type of predicate being inserted; default is
 * PredicateType::RECURRENT
 */
template <class DerivedSST>
typename Predicates<DerivedSST>::pred_handle Predicates<DerivedSST>::insert(const pred_t& pred, const trig_t& trig, PredicateType type) {
    std::lock_guard<std::recursive_mutex> lock(predicate_mutex);
    predicates.push_back({pred, trig, type});
    return pred_handle(--predicates.end());
}

template <class DerivedSST>
void Predicates<DerivedSST>::remove(pred_handle& handle) {
    std::lock_guard<std::recursive_mutex> lock(predicate_mutex);
    if(!handle.is_valid()) {
        return;
    }
    handle.valid = false;
    to_remove.push_back(handle.iter);
}

// template <class DerivedSST>
// void Predicates<DerivedSST>::clear() {
//     std::lock_guard<std::recursive_mutex> lock(predicate_mutex);
//     using ptr_to_pred = std::unique_ptr<std::pair<pred, trig>>;
//     std::for_each(one_time_predicates.begin(), one_time_predicates.end(),
//                   [](ptr_to_pred& ptr) { ptr.reset(); });
//     std::for_each(recurrent_predicates.begin(), recurrent_predicates.end(),
//                   [](ptr_to_pred& ptr) { ptr.reset(); });
// }

}  // namespace sst
