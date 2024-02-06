#ifndef MESSAGE_HPP
#define MESSAGE_HPP

#include <cstdint>
#include <utility>

/**
 * @cond DoxygenSuppressed
 */

struct ParsedTag {
    uint8_t reserved;
    uint8_t padding; // now used for libfabric opcode.
    uint16_t group_number;
    uint32_t target;
};

inline ParsedTag parse_tag(uint64_t t) {
    return ParsedTag{0, 0, (uint16_t)((t & 0x0000ffff00000000ull) >> 32),
                     (uint32_t)(t & 0x00000000ffffffffull)};
}
inline uint64_t form_tag(uint16_t group_number, uint32_t target) {
    return (((uint64_t)group_number) << 32) | (uint64_t)target;
}

struct ParsedImmediate {
    uint16_t total_blocks;
    uint16_t block_number;
};

inline ParsedImmediate parse_immediate(uint32_t imm) {
    return ParsedImmediate{(uint16_t)((imm & 0xffff0000) >> 16),
                           (uint16_t)(imm & 0x0000ffff)};
}
inline uint32_t form_immediate(uint16_t total_blocks, uint16_t block_number) {
    return ((uint32_t)total_blocks) << 16 | ((uint32_t)block_number);
}

/**
 * @endcond
 */

#endif
