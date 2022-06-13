#include <derecho/mutils-serialization/SerializationSupport.hpp>

#include <cstdint>
#include <functional>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <vector>

using callback_function_t = std::function<void(int64_t, const std::vector<int>&)>;

int main(int argc, char** argv) {
    std::vector<int> vector_data(256);
    std::iota(vector_data.begin(), vector_data.end(), 1);
    int64_t int_data = 6666666;

    // Serialize an int64 and a std::vector into a buffer
    std::size_t buf_size = mutils::bytes_size(vector_data) + mutils::bytes_size(int_data);
    uint8_t* byte_buffer = new uint8_t[buf_size];
    std::size_t buf_offset = 0;
    mutils::to_bytes(int_data, byte_buffer + buf_offset);
    buf_offset += mutils::bytes_size(int_data);
    mutils::to_bytes(vector_data, byte_buffer + buf_offset);
    buf_offset += mutils::bytes_size(vector_data);

    uint8_t const* const read_only_buf_ptr = byte_buffer;

    // Can we deserialize the vector normally?
    auto deserialized_vector = mutils::from_bytes<std::vector<int>>(nullptr, read_only_buf_ptr + mutils::bytes_size(int_data));
    std::cout << "Vector after from_bytes: " << std::endl;
    for(const auto& e : *deserialized_vector) {
        std::cout << e << " ";
    }
    std::cout << std::endl;
    // Can we deserialize with from_bytes_noalloc?
    auto context_vector = mutils::from_bytes_noalloc<std::vector<int>>(nullptr, read_only_buf_ptr + mutils::bytes_size(int_data));
    std::cout << "Vector after from_bytes_noalloc: " << std::endl;
    for(const auto& e : *context_vector) {
        std::cout << e << " ";
    }
    std::cout << std::endl;
    // What about from_bytes_noalloc with an explicit third argument?
    auto const_context_vector = mutils::from_bytes_noalloc<const std::vector<int>>(nullptr, read_only_buf_ptr + mutils::bytes_size(int_data),
                                                                                   mutils::context_ptr<const std::vector<int>>{});
    std::cout << "Vector after from_bytes_noalloc: " << std::endl;
    for(const auto& e : *const_context_vector) {
        std::cout << e << " ";
    }
    std::cout << std::endl;
    // Attempt to call a std::function with the integer and vector as arguments
    callback_function_t callback_fun = [](int64_t int_arg, const std::vector<int>& vector_arg) {
        std::cout << "Got callback with integer " << int_arg << " and vector: " << std::endl;
        for(std::size_t i = 0; i < vector_arg.size(); ++i) {
            std::cout << vector_arg[i] << " ";
        }
        std::cout << std::endl;
    };
    mutils::deserialize_and_run(nullptr, read_only_buf_ptr, callback_fun);

    delete[] byte_buffer;
}