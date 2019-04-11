#pragma once
#include <cstdint>

namespace derecho {

/**
 * A runtime constant (computed once during static initialization) that
 * represents the current running version of the Derecho library as a platform-
 * dependent hash code. This can be used to check that two copies of a Derecho
 * program are using the same version of the library *and* were compiled for
 * the same architecture, since the hash codes will not match if they are
 * computed on different architectures.
 */
extern uint64_t my_version_hashcode;

/**
 * The function that computes the Derecho version hashcode. It should not be
 * necessary to call this function at runtime, since its output will not change
 * and the value is already available in the global variable my_version_hashcode.
 * @return The current Derecho version hashcode.
 */
uint64_t version_hashcode();

}  // namespace derecho
