#pragma once

#include <algorithm>
#include <functional>
#include <list>
#include <mutex>
#include <utility>
#include <vector>

#include "sst.hpp"

namespace sst {

template <class DerivedSST>
class SST;

/** Enumeration defining the kinds of predicates an SST can handle. */
enum class PredicateType {
    /** One-time predicates only fire once; they are deleted once they become true. */
    ONE_TIME,
    /** Recurrent predicates persist as long as the SST instance (or until they are removed) and 
     * fire their triggers every time they are true. */
    RECURRENT
};

template <class DerivedSST>
class Predicates {
    // SST needs to read these predicate lists directly
    friend class SST<DerivedSST>;

    using pred_t = std::function<bool(const DerivedSST&)>;
    using trig_t = std::function<void(DerivedSST&)>;

    class Predicate {
    public:
        const pred_t pred;
        const trig_t trig;
        const PredicateType type;
        Predicate(const pred_t& pred, const trig_t& trig, PredicateType& type)
                : pred(pred),
                  trig(trig),
                  type(type) {
        }
    };

    using pred_list = std::list<Predicate>;

    /** Predicate list for all predicates */
    pred_list predicates;

    std::recursive_mutex predicate_mutex;

    std::vector<typename pred_list::iterator> to_remove;

public:
    class pred_handle {
        bool valid;
        typename pred_list::iterator iter;
        friend class Predicates;

    public:
        pred_handle() : valid(false) {}
        pred_handle(typename pred_list::iterator iter)
                : valid{true}, iter{iter} {}
        pred_handle(pred_handle&) = delete;
        pred_handle(pred_handle&& other)
                : pred_handle(std::move(other.iter)) {
            other.valid = false;
        }
        pred_handle& operator=(pred_handle&) = delete;
        pred_handle& operator=(pred_handle&& other) {
            iter = std::move(other.iter);
            valid = true;
            other.valid = false;
            return *this;
        }
        bool is_valid() const {
            return valid && (*iter);
        }
    };

    /** Inserts a single (predicate, trigger) pair to the appropriate predicate list. */
    pred_handle insert(const pred_t& pred, const trig_t& trig,
                       PredicateType type = PredicateType::RECURRENT);

    /** Removes a (predicate, trigger) pair previously registered with insert(). */
    void remove(pred_handle& handle);

    // /** Deletes all predicates, including evolvers and their triggers. */
    // void clear();
};

} /* namespace sst */

#include "predicate_impl.hpp"
