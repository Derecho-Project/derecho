#ifndef PREDICATES_H
#define PREDICATES_H

#include <functional>
#include <list>
#include <utility>
#include <mutex>
#include <algorithm>

#include "sst.h"

namespace sst {

/** Enumeration defining the kinds of predicates an SST can handle. */
enum class PredicateType {
    /** One-time predicates only fire once; they are deleted once they become
       true. */
    ONE_TIME,
    /** Recurrent predicates persist as long as the SST instance and fire their
     * triggers every time they are true. */
    RECURRENT,
    /** Transition predicates persist as long as the SST instance, but only fire
     * their triggers when they transition from false to true. */
    TRANSITION
};

enum class Mode;
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
class SST;

/**
 * Predicates container for SST. Declared as a member oF SST so it can inherit
 * SST's template parameters.
 */
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
class SST<Row, ImplMode, NameEnum, RowExtras>::Predicates {
    /** Type definition for a predicate: a boolean function that takes an SST as
     * input. */
    using pred = std::function<bool(const SST &)>;
    /** Type definition for a trigger: a void function that takes an SST as
     * input.
     */
    using trig = std::function<void(SST &)>;
    /** Type definition for a list of predicates, where each predicate is
     * paired with a callback, held by pointer (to enable cheap copying) */
    using pred_list =
        std::list<std::unique_ptr<std::pair<pred, std::shared_ptr<trig>>>>;

    using evolver = std::function<pred(const SST &, int)>;
    using evolve_trig = std::function<void(SST &, int)>;

    /** Predicate list for one-time predicates. */
    pred_list one_time_predicates;
    /** Predicate list for recurrent predicates */
    pred_list recurrent_predicates;
    /** Predicate list for transition predicates */
    pred_list transition_predicates;
    /** Contains one entry for every predicate in `transition_predicates`, in
     * parallel. */
    std::list<bool> transition_predicate_states;
    // SST needs to read these predicate lists directly
    friend class SST;

    std::mutex predicate_mutex;

public:
    /**
     * An opaque handle for a predicate registered with the Predicates class.
     * Can be used (only once) to delete the predicate it refers to. Move-only.
     */
    class pred_handle {
        bool is_valid;
        typename pred_list::iterator iter;
        PredicateType type;
        friend class Predicates;

    public:
        pred_handle() : is_valid(false) {}
        pred_handle(typename pred_list::iterator iter, PredicateType type)
            : is_valid{true}, iter{iter}, type{type} {}
        pred_handle(pred_handle &) = delete;
        pred_handle(pred_handle &&other)
            : pred_handle(std::move(other.iter), other.type) {
            other.is_valid = false;
        }
        pred_handle &operator=(pred_handle &) = delete;
        pred_handle &operator=(pred_handle &&other) {
            iter = std::move(other.iter);
            type = other.type;
            is_valid = true;
            other.is_valid = false;
            return *this;
        }
    };

    std::vector<std::unique_ptr<std::pair<pred, int>>> evolving_preds;

    std::vector<std::unique_ptr<evolver>> evolvers;

    std::vector<std::list<evolve_trig>> evolving_triggers;

    /** Inserts a single (predicate, trigger) pair to the appropriate predicate
     * list. */
    pred_handle insert(pred predicate, trig trigger,
                       PredicateType type = PredicateType::ONE_TIME);

    /** Inserts a predicate with a list of triggers (which will be run in
     * sequence) to the appropriate predicate list. */
    pred_handle insert(pred predicate, const std::list<trig> &triggers,
                       PredicateType type = PredicateType::ONE_TIME) {
        return insert(predicate, [triggers](SST &sst) {
            for(const auto &trigger : triggers) trigger(sst);
        }, type);
    }

    /** Removes a (predicate, trigger) pair previously registered with insert().
     */
    void remove(pred_handle &pred);

    /** Inserts a single (name, predicate, evolve) to the appropriate predicate
     * list. */
    void insert(NameEnum name, pred predicate, evolver evolve,
                std::list<evolve_trig> triggers);

    void add_triggers(NameEnum name, std::list<evolve_trig> triggers);

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
template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
auto SST<Row, ImplMode, NameEnum, RowExtras>::Predicates::insert(
    pred predicate, trig trigger, PredicateType type) -> pred_handle {
    std::lock_guard<std::mutex> lock(predicate_mutex);
    if(type == PredicateType::ONE_TIME) {
        one_time_predicates.push_back(
            std::make_unique<std::pair<pred, std::shared_ptr<trig>>>(
                predicate, std::make_shared<trig>(trigger)));
        return pred_handle(--one_time_predicates.end(), type);
    } else if(type == PredicateType::RECURRENT) {
        recurrent_predicates.push_back(
            std::make_unique<std::pair<pred, std::shared_ptr<trig>>>(
                predicate, std::make_shared<trig>(trigger)));
        return pred_handle(--recurrent_predicates.end(), type);
    } else {
        transition_predicates.push_back(
            std::make_unique<std::pair<pred, std::shared_ptr<trig>>>(
                predicate, std::make_shared<trig>(trigger)));
        transition_predicate_states.push_back(false);
        return pred_handle(--transition_predicates.end(), type);
    }
}

template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::Predicates::insert(
    NameEnum name, pred predicate, evolver evolve,
    std::list<evolve_trig> triggers) {
    std::lock_guard<std::mutex> lock(predicate_mutex);
    constexpr int min = std::tuple_size<SST::named_functions_t>::value;
    int index = static_cast<int>(name) - min;
    assert(index >= 0);
    assert(evolving_preds.size() == evolvers.size());
    assert(evolving_preds.size() == evolving_triggers.size());
    if(evolving_preds.size() <= index) {
        evolving_preds.resize(index + 1);
        evolvers.resize(index + 1);
        evolving_triggers.resize(index + 1);
    }
    evolvers[index] = std::make_unique<evolver>(evolve);
    evolving_preds[index] =
        std::make_unique<std::pair<pred, int>>({predicate, 0});
    evolving_triggers[index] = triggers;
}

template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::Predicates::add_triggers(
    NameEnum name, std::list<evolve_trig> triggers) {
    std::lock_guard<std::mutex> lock(predicate_mutex);
    constexpr int min = std::tuple_size<SST::named_functions_t>::value;
    int index = static_cast<int>(name) - min;
    assert(index >= 0);
    assert(index < evolving_preds.size());
    evolving_triggers[index].insert(evolving_triggers[index].end(),
                                    triggers.begin(), triggers.end());
}

template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::Predicates::remove(
    pred_handle &handle) {
    std::lock_guard<std::mutex> lock(predicate_mutex);
    if(!handle.is_valid) {
        return;
    }
    handle.iter->reset();
    handle.is_valid = false;
}

template <class Row, Mode ImplMode, typename NameEnum, typename RowExtras>
void SST<Row, ImplMode, NameEnum, RowExtras>::Predicates::clear() {
    std::lock_guard<std::mutex> lock(predicate_mutex);
    using ptr_to_pred = std::unique_ptr<std::pair<pred, std::shared_ptr<trig>>>;
    std::for_each(one_time_predicates.begin(), one_time_predicates.end(),
                  [](ptr_to_pred &ptr) { ptr.reset(); });
    std::for_each(recurrent_predicates.begin(), recurrent_predicates.end(),
                  [](ptr_to_pred &ptr) { ptr.reset(); });
    std::for_each(transition_predicates.begin(), transition_predicates.end(),
                  [](ptr_to_pred &ptr) { ptr.reset(); });
    evolving_preds.clear();
    evolving_triggers.clear();
    evolvers.clear();
}

} /* namespace sst */

#endif /* PREDICATES_H */
