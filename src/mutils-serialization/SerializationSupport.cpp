#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <string.h>
using namespace std;

/**
 * @cond DoxygenSuppreseed
 */

namespace mutils {

std::size_t to_bytes(const ByteRepresentable& b, uint8_t* buffer) {
    return b.to_bytes(buffer);
}

std::size_t bytes_size(const ByteRepresentable& b) {
    return b.bytes_size();
}

std::size_t to_bytes(const std::string& str, uint8_t* buffer) {
    memcpy(buffer, str.c_str(), str.length() + 1);
    return str.length() + 1;
}

std::size_t bytes_size(const std::string& str) {
    return str.length() + 1;
}

#ifdef MUTILS_DEBUG
void ensure_registered(ByteRepresentable& b, DeserializationManager& dm) {
    b.ensure_registered(dm);
}
#endif

context_ptr<marshalled>
marshalled::from_bytes_noalloc(DeserializationManager const* const, uint8_t* buffer) {
    return context_ptr<marshalled>((marshalled*)buffer);
}

std::function<void(uint8_t const* const, std::size_t)> post_to_buffer(std::size_t& index, uint8_t* dest_buf) {
    return [&index, dest_buf](uint8_t const* const read_buf, std::size_t size) {
        memcpy(dest_buf + index, read_buf, size);
        index += size;
    };
}

void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer, const ByteRepresentable& br) {
    br.post_object(consumer);
}

void post_object(const std::function<void(uint8_t const* const, std::size_t)>& consumer, const std::string& str) {
    consumer((const uint8_t*)(str.data()), str.length() + 1);
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
/**
 * @endcond
 */
