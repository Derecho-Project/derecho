#include <derecho/core/detail/version_code.hpp>
#include <derecho/core/git_version.hpp>
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

const uint64_t derecho_version = ((((static_cast<uint64_t>(derecho::MAJOR_VERSION) << 16)
                                    + derecho::MINOR_VERSION)
                                   << 32)
                                  + derecho::COMMITS_AHEAD_OF_VERSION);

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

struct s1 {
    char something;
    int16_t the_int;
};
const uint64_t int16_offset = (uint64_t)offsetof(struct s1, the_int);

struct s2 {
    char something;
    int32_t the_int;
};
const uint64_t int32_offset = (uint64_t)offsetof(struct s2, the_int);

struct s3 {
    char something;
    int64_t the_int;
};

const uint64_t int64_offset1 = (uint64_t)offsetof(struct s3, the_int);

struct s4 {
    char something;
    float fsomething;
    int64_t the_int;
};
const uint64_t int64_offset2 = (uint64_t)offsetof(struct s4, the_int);

struct s5 {
    char something;
    double dsomething;
    int64_t the_int;
};

const uint64_t int64_offset3 = (uint64_t)offsetof(struct s5, the_int);

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
                                       std::hash<uint64_t>()(derecho_version)),
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
