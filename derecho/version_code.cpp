#include "version_code.h"
#include <functional>

/** A simple hash-combine function to "mix" two hashcodes */
uint64_t mix(uint64_t v1, uint64_t v2) {
    return v1 * 37 ^ v2;
}

#define ISGNU 0
#define ISCLANG (1 << 32)
#if defined __GNUC__ && defined __GNUC_MINOR__
const uint64_t compiler = ISGNU | ((__GNUC__ << 16) + __GNUC_MINOR__);
#else
const uint64_t  compiler = ISCLANG | (__clang_major__ << 16) + __clang_minor__ );
#endif

// These next assume that this is Derecho release 0.9.0
// In practice GitHub should be managing the three numbers and we should use
// the feature that substitutes them into the text file here.
#define DERECHO_MAJOR 0L       // Replace with a major number managed by GitHub
#define DERECHO_MINOR 9L       // Replace with a minor version number managed by GitHub
#define DERECHO_PATCHLEVEL 0L  // Replace with a patch version number managed by GitHub
#define DERECHO_VERSION ((((DERECHO_MAJOR << 16) + DERECHO_MINOR) << 32) + DERECHO_PATCHLEVEL)

/*
 * The following variables are for detection of Endian order for integers.
 * The reason we do this at runtime and NOT compile time is to catch cases
 * where code is compiled for platform A but then copied (incorrectly) to
 * platform B.  Of course it might simply seg fault.  But if it manages to launch
 * this way of checking would notice.
 */
char int16_array[2]{1, 2};
char int32_array[4]{1, 2, 3, 4};
char int64_array[8]{1, 2, 3, 4, 5, 6, 7, 8};

// Runtime detection of floating point storage order
float a_float = 123.4560001;
double a_double = 654.3210000987;

// Runtime measurement of structure padding rules
char** dnull_ptr = 0;

struct s1 {
    char something;
    int16_t the_int;
};

const uint64_t int16_offset = reinterpret_cast<const uint64_t>(
        &reinterpret_cast<struct s1*>(dnull_ptr)->the_int);

struct s2 {
    char something;
    int32_t the_int;
};

const uint64_t int32_offset = reinterpret_cast<const uint64_t>(
        &reinterpret_cast<struct s2*>(dnull_ptr)->the_int);

struct s3 {
    char something;
    int64_t the_int;
};

const uint64_t int64_offset1 = reinterpret_cast<const uint64_t>(
        &reinterpret_cast<struct s3*>(dnull_ptr)->the_int);

struct s4 {
    char something;
    float fsomething;
    int64_t the_int;
};

const uint64_t int64_offset2 = reinterpret_cast<const uint64_t>(
        &reinterpret_cast<struct s4*>(dnull_ptr)->the_int);

struct s5 {
    char something;
    double dsomething;
    int64_t the_int;
};

const uint64_t int64_offset3 = reinterpret_cast<const uint64_t>(
        &reinterpret_cast<struct s5*>(dnull_ptr)->the_int);

uint64_t int_offset_hash = mix(int64_offset3,
                               mix(int64_offset2,
                                   mix(int64_offset1,
                                       mix(int32_offset, int16_offset))));

/*
 * The above definitions and variables can be in "global namespace" because they
 * are not used outside this file, but the final hashcode and function must be
 * properly encapsulated in a namespace because they are externally visible.
 */

namespace derecho {

// This function combines all the measurements defined above, using the "mix" function
uint64_t version_hashcode() {
    return mix(mix(mix(mix(mix(mix(mix(std::hash<uint64_t>()(compiler),
                                       std::hash<uint64_t>()(DERECHO_VERSION)),
                                   *reinterpret_cast<uint16_t*>(&int16_array)),
                               *reinterpret_cast<uint32_t*>(&int32_array)),
                           *reinterpret_cast<uint64_t*>(&int64_array)),
                       *reinterpret_cast<uint32_t*>(&a_float)),
                   *reinterpret_cast<uint64_t*>(&a_double)),
               int_offset_hash);
}

// Run the function once and cache the result in a global variable
uint64_t my_version_hashcode = version_hashcode();

}  // namespace derecho
