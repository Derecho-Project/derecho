#pragma once

#include <algorithm>
#include <functional>
#include <list>
#include <mutex>
#include <utility>


namespace sst {

template <class DerivedSST>
class SST;

/** Enumeration defining the kinds of predicates an SST can handle. */
enum class PredicateType {
    /** One-time predicates only fire once; they are deleted once they become true. */
    ONE_TIME,
    /** Recurrent predicates persist as long as the SST instance and fire their
   * triggers every time they are true. */
    RECURRENT,
    /** Transition predicates persist as long as the SST instance, but only fire
   * their triggers when they transition from false to true. */
    TRANSITION
};

template <class DerivedSST>
class Predicates {
    using pred = std::function<bool(const DerivedSST&)>;
    using trig = std::function<void(DerivedSST&)>;
    using pred_list = std::list<std::unique_ptr<std::pair<pred, std::shared_ptr<trig>>>>;
    /** Predicate list for one-time predicates. */
    pred_list one_time_predicates;
    /** Predicate list for recurrent predicates */
    pred_list recurrent_predicates;
    /** Predicate list for transition predicates */
    pred_list transition_predicates;
    /** Contains one entry for every predicate in `transition_predicates`, in parallel. */
    std::list<bool> transition_predicate_states;
    // SST needs to read these predicate lists directly
    friend class SST<DerivedSST>;

    std::mutex predicate_mutex;

public:
    class pred_handle {
        bool valid;
        typename pred_list::iterator iter;
        PredicateType type;
        friend class Predicates;

    public:
        pred_handle() : valid(false), type(PredicateType::ONE_TIME) {}
        pred_handle(typename pred_list::iterator iter, PredicateType type)
                : valid{true}, iter{iter}, type{type} {}
        pred_handle(pred_handle&) = delete;
        pred_handle(pred_handle&& other)
                : pred_handle(std::move(other.iter), other.type) {
            other.valid = false;
        }
        pred_handle& operator=(pred_handle&) = delete;
        pred_handle& operator=(pred_handle&& other) {
            iter = std::move(other.iter);
            type = other.type;
            valid = true;
            other.valid = false;
            return *this;
        }
        bool is_valid() const {
            return valid && (*iter);
        }
    };

    /** Inserts a single (predicate, trigger) pair to the appropriate predicate list. */
    pred_handle insert(pred predicate, trig trigger,
                       PredicateType type = PredicateType::ONE_TIME);

    /** Inserts a predicate with a list of triggers (which will be run in
     * sequence) to the appropriate predicate list. */
    pred_handle insert(pred predicate, const std::list<trig>& triggers,
                       PredicateType type = PredicateType::ONE_TIME) {
        return insert(predicate, [triggers](DerivedSST& t) {
            for(const auto& trigger : triggers)
                trigger(t);
        },
                      type);
    }

    /** Removes a (predicate, trigger) pair previously registered with insert(). */
    void remove(pred_handle& pred);

    /** Deletes all predicates, including evolvers and their triggers. */
    void clear();
};

/**
 * This is a convenience method for when the predicate has only one trigger; it
 * automatically chooses the right list based on the predicate type. To insert
 * a predicate with multiple triggers, use std::list::insert() directly on the
 * appropriate predicate list member.
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
    } else if(type == PredicateType::RECURRENT) {
        recurrent_predicates.push_back(std::make_unique<std::pair<pred, std::shared_ptr<trig>>>(
                predicate, std::make_shared<trig>(trigger)));
        return pred_handle(--recurrent_predicates.end(), type);
    } else {
        transition_predicates.push_back(std::make_unique<std::pair<pred, std::shared_ptr<trig>>>(
                predicate, std::make_shared<trig>(trigger)));
        transition_predicate_states.push_back(false);
        return pred_handle(--transition_predicates.end(), type);
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
    std::for_each(transition_predicates.begin(), transition_predicates.end(),
                  [](ptr_to_pred& ptr) { ptr.reset(); });
}

} /* namespace sst */
