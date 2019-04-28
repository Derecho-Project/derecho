#pragma once

#include "predicate.hpp"

namespace sst {
/**
 * Inserts the predicate trigger pair in the right list based on the predicate type.
 * @param predicate The predicate to insert.
 * @param trigger The trigger to execute when the predicate is true.
 * @param type The type of predicate being inserted; default is
 * PredicateType::ONE_TIME
 */
template <class DerivedSST>
auto Predicates<DerivedSST>::insert(pred predicate, trig trigger, PredicateType type) -> pred_handle {
    std::lock_guard<std::mutex> lock(predicate_mutex);
    if(type == PredicateType::ONE_TIME) {
        one_time_predicates.push_back(std::make_unique<std::pair<pred, std::shared_ptr<trig>>>(
                predicate, std::make_shared<trig>(trigger)));
        return pred_handle(--one_time_predicates.end(), type);
    } else {
        recurrent_predicates.push_back(std::make_unique<std::pair<pred, std::shared_ptr<trig>>>(
                predicate, std::make_shared<trig>(trigger)));
        return pred_handle(--recurrent_predicates.end(), type);
    }
}

template <class DerivedSST>
void Predicates<DerivedSST>::remove(pred_handle& handle) {
    std::lock_guard<std::mutex> lock(predicate_mutex);
    if(!handle.is_valid()) {
        return;
    }
    handle.iter->reset();
    handle.valid = false;
}

template <class DerivedSST>
void Predicates<DerivedSST>::clear() {
    std::lock_guard<std::mutex> lock(predicate_mutex);
    using ptr_to_pred = std::unique_ptr<std::pair<pred, std::shared_ptr<trig>>>;
    std::for_each(one_time_predicates.begin(), one_time_predicates.end(),
                  [](ptr_to_pred& ptr) { ptr.reset(); });
    std::for_each(recurrent_predicates.begin(), recurrent_predicates.end(),
                  [](ptr_to_pred& ptr) { ptr.reset(); });
}

}  // namespace sst
