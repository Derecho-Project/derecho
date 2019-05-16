#include "sst.hpp"

namespace sst {
_SSTField::_SSTField(const size_t field_length) : base(nullptr),
                                                  row_length(0),
                                                  field_length(field_length) {
}

size_t _SSTField::set_base(volatile char* const base) {
    this->base = base;
    return padded_length(field_length);
}

const char* _SSTField::get_base_address() {
    return const_cast<char*>(base);
}

void _SSTField::set_row_length(const size_t row_length) {
    this->row_length = row_length;
}

void _SSTField::set_num_rows(const uint32_t num_rows) {
    this->num_rows = num_rows;
}
}  // namespace sst
