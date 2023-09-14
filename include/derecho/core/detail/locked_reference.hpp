/**
 * @file locked_reference.hpp
 *
 * @date Mar 13, 2017
 */

#pragma once

#include <mutex>

namespace derecho {

/**
 * A little helper class that wraps together a reference and a lock
 * on a related mutex. This keeps the lock in scope as long as a client
 * has access to the reference, which ensures that the mutex stays locked while
 * the data it guards is being accessed.
 */
template <typename LockType, typename T>
class LockedReference {
private:
    T& reference;
    LockType lock;

public:
    LockedReference(T& real_reference, typename LockType::mutex_type& mutex)
            : reference(real_reference), lock(mutex) {}

    T& get() {
        return reference;
    }
};
}  // namespace derecho
