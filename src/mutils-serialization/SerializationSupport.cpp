#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <string.h>
using namespace std;

namespace mutils {

std::size_t to_bytes(const ByteRepresentable& b, uint8_t* v) {
    return b.to_bytes(v);
}

std::size_t bytes_size(const ByteRepresentable& b) {
    return b.bytes_size();
}

std::size_t to_bytes(const std::string& b, uint8_t* v) {
    memcpy(v, b.c_str(), b.length() + 1);
    return b.length() + 1;
}

std::size_t bytes_size(const std::string& b) {
    return b.length() + 1;
}

#ifdef MUTILS_DEBUG
void ensure_registered(ByteRepresentable& b, DeserializationManager& dm) {
    b.ensure_registered(dm);
}
#endif

context_ptr<marshalled>
marshalled::from_bytes_noalloc(DeserializationManager const* const, uint8_t* v) {
    return context_ptr<marshalled>((marshalled*)v);
}

std::function<void(uint8_t const* const, std::size_t)> post_to_buffer(std::size_t& index, uint8_t* dest_buf) {
    return [&index, dest_buf](uint8_t const* const read_buf, std::size_t size) {
        memcpy(dest_buf + index, read_buf, size);
        index += size;
    };
}

void post_object(const std::function<void(uint8_t const* const, std::size_t)>& f, const ByteRepresentable& br) {
    br.post_object(f);
}

void post_object(const std::function<void(uint8_t const* const, std::size_t)>& f, const std::string& str) {
    f((const uint8_t*)(str.data()), str.length() + 1);
}

std::size_t to_bytes_v(uint8_t*) {
    return 0;
}

std::size_t from_bytes_v(DeserializationManager*, uint8_t const* const) {
    return 0;
}

std::size_t from_bytes_noalloc_v(DeserializationManager*, uint8_t const* const) {
    return 0;
}

}  // namespace mutils
