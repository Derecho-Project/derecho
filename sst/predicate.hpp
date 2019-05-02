#pragma once

#include <algorithm>
#include <functional>
#include <list>
#include <mutex>
#include <utility>

#include "sst.hpp"

namespace sst {

template <class DerivedSST>
class SST;

/** Enumeration defining the kinds of predicates an SST can handle. */
enum class PredicateType {
    /** One-time predicates only fire once; they are deleted once they become true. */
    ONE_TIME,
    /** Recurrent predicates persist as long as the SST instance and fire their
   * triggers every time they are true. */
    RECURRENT
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
                       PredicateType type = PredicateType::RECURRENT);

    /** Removes a (predicate, trigger) pair previously registered with insert(). */
    void remove(pred_handle& pred);

    /** Deletes all predicates, including evolvers and their triggers. */
    void clear();
};

} /* namespace sst */

#include "predicate_impl.hpp"
