/**
 * @file openssl_exception.cpp
 *
 */

#include <derecho/openssl/openssl_exception.hpp>

namespace openssl {

/**
 * A plain C function matching the type expected by ERR_print_errors_cb that
 * appends each provided error string to a std::stringstream.
 */
int openssl_error_callback(const char* str, size_t len, void* user_data) {
    std::stringstream* the_stringstream = reinterpret_cast<std::stringstream*>(user_data);
    (*the_stringstream) << str << std::endl;
    return 0;
}

std::string openssl_errors_to_string() {
    std::stringstream error_str;
    ERR_print_errors_cb(openssl_error_callback, &error_str);
    return error_str.str();
}

std::string get_error_string(unsigned long error_code, const std::string& extra_message) {
    const size_t buf_size = 512; //I hope this is big enough
    char string_buf[buf_size];
    ERR_error_string_n(error_code, string_buf, buf_size);
    std::stringstream string_builder;
    string_builder << extra_message << " " << string_buf;
    return string_builder.str();
}

}  // namespace openssl