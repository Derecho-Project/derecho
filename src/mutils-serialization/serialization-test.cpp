#include <derecho/mutils-serialization/SerializationSupport.hpp>

#include <cassert>
#include <cstdint>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

struct PODStruct {
    int field_one;
    int field_two;
    bool a_bool;
};

struct TestObject : public mutils::ByteRepresentable {
    int test_int;
    std::string test_string;
    std::pair<std::uint32_t, std::uint64_t> test_pair;
    std::tuple<int, std::string, std::uint64_t> test_tuple;
    std::set<int> test_set;
    std::list<int> test_list;
    std::vector<int> test_vector;
    std::map<int, std::string> test_map;
    std::unordered_map<int, std::string> test_hashmap;
    PODStruct test_pod;
    TestObject(int i, const std::string& str, const std::pair<std::uint32_t, std::uint64_t> p,
               const std::tuple<int, std::string, std::uint64_t>& t, const std::set<int>& s,
               const std::list<int>& l, const std::vector<int>& v, const std::map<int, std::string>& m,
               const std::unordered_map<int, std::string>& u, const PODStruct& pod)
        : test_int(i),
          test_string(str),
          test_pair(p),
          test_tuple(t),
          test_set(s),
          test_list(l),
          test_vector(v),
          test_map(m),
          test_hashmap(u),
          test_pod(pod) {}

    // Mostly use DEFAULT_SERIALIZATION_SUPPORT, but don't use the default from_bytes_noalloc
    DEFAULT_SERIALIZE(test_int, test_string, test_pair, test_tuple,
                      test_set, test_list, test_vector, test_map, test_hashmap, test_pod);
    DEFAULT_DESERIALIZE(TestObject, test_int, test_string, test_pair, test_tuple,
                        test_set, test_list, test_vector, test_map, test_hashmap, test_pod);
    // Provide a from_bytes_noalloc that recursively calls from_bytes_noalloc so we actually test from_bytes_noalloc
    static mutils::context_ptr<TestObject> from_bytes_noalloc(mutils::DeserializationManager* dsm, uint8_t const* buf) {
        auto int_ptr = mutils::from_bytes_noalloc<int>(dsm, buf);
        std::size_t bytes_read = mutils::bytes_size(*int_ptr);
        auto string_ptr = mutils::from_bytes_noalloc<std::string>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*string_ptr);
        auto pair_ptr = mutils::from_bytes_noalloc<std::pair<std::uint32_t, std::uint64_t>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*pair_ptr);
        auto tuple_ptr = mutils::from_bytes_noalloc<std::tuple<int, std::string, std::uint64_t>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*tuple_ptr);
        auto set_ptr = mutils::from_bytes_noalloc<std::set<int>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*set_ptr);
        auto list_ptr = mutils::from_bytes_noalloc<std::list<int>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*list_ptr);
        auto vec_ptr = mutils::from_bytes_noalloc<std::vector<int>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*vec_ptr);
        auto map_ptr = mutils::from_bytes_noalloc<std::map<int, std::string>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*map_ptr);
        auto hashmap_ptr = mutils::from_bytes_noalloc<std::unordered_map<int, std::string>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*hashmap_ptr);
        auto pod_ptr = mutils::from_bytes_noalloc<PODStruct>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*pod_ptr);
        return mutils::context_ptr<TestObject>(new TestObject(*int_ptr, *string_ptr, *pair_ptr, *tuple_ptr, *set_ptr,
                                                              *list_ptr, *vec_ptr, *map_ptr, *hashmap_ptr, *pod_ptr));
    };
    static mutils::context_ptr<const TestObject> from_bytes_noalloc_const(mutils::DeserializationManager* dsm, uint8_t const* buf) {
        auto int_ptr = mutils::from_bytes_noalloc<const int>(dsm, buf);
        std::size_t bytes_read = mutils::bytes_size(*int_ptr);
        auto string_ptr = mutils::from_bytes_noalloc<const std::string>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*string_ptr);
        auto pair_ptr = mutils::from_bytes_noalloc<const std::pair<std::uint32_t, std::uint64_t>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*pair_ptr);
        auto tuple_ptr = mutils::from_bytes_noalloc<const std::tuple<int, std::string, std::uint64_t>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*tuple_ptr);
        auto set_ptr = mutils::from_bytes_noalloc<const std::set<int>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*set_ptr);
        auto list_ptr = mutils::from_bytes_noalloc<const std::list<int>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*list_ptr);
        auto vec_ptr = mutils::from_bytes_noalloc<const std::vector<int>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*vec_ptr);
        auto map_ptr = mutils::from_bytes_noalloc<const std::map<int, std::string>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*map_ptr);
        auto hashmap_ptr = mutils::from_bytes_noalloc<const std::unordered_map<int, std::string>>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*hashmap_ptr);
        auto pod_ptr = mutils::from_bytes_noalloc<const PODStruct>(dsm, buf + bytes_read);
        bytes_read += mutils::bytes_size(*pod_ptr);
        return mutils::context_ptr<const TestObject>(new TestObject(*int_ptr, *string_ptr, *pair_ptr, *tuple_ptr, *set_ptr,
                                                                    *list_ptr, *vec_ptr, *map_ptr, *hashmap_ptr, *pod_ptr));
    }

    void ensure_registered(mutils::DeserializationManager&){};
};

bool operator==(const TestObject& lhs, const TestObject& rhs) {
    return lhs.test_int == rhs.test_int &&
           lhs.test_string == rhs.test_string &&
           lhs.test_pair == rhs.test_pair &&
           lhs.test_tuple == rhs.test_tuple &&
           lhs.test_set == rhs.test_set &&
           lhs.test_list == rhs.test_list &&
           lhs.test_vector == rhs.test_vector &&
           lhs.test_map == rhs.test_map &&
           lhs.test_hashmap == rhs.test_hashmap &&
           lhs.test_pod.field_one == rhs.test_pod.field_one &&
           lhs.test_pod.field_two == rhs.test_pod.field_two &&
           lhs.test_pod.a_bool == rhs.test_pod.a_bool;
}

int test_object_function(const TestObject& arg) {
    std::cout << "Called test_object_function with this object: " << std::endl;
    std::cout << "{ " << arg.test_int << ", " << arg.test_string << ", (" << arg.test_pair.first << "," << arg.test_pair.second << "), ("
              << std::get<0>(arg.test_tuple) << ", " << std::get<1>(arg.test_tuple) << ", " << std::get<2>(arg.test_tuple) << "), {";
    int set_sum = 0;
    for(const auto& i : arg.test_set) {
        set_sum += i;
        std::cout << i << ",";
    }
    std::cout << "}, {";
    for(const auto& i : arg.test_list) {
        std::cout << i << ",";
    }
    std::cout << "}, [";
    for(const auto& i : arg.test_vector) {
        std::cout << i << ",";
    }
    std::cout << "], {";
    for(const auto& entry : arg.test_map) {
        std::cout << "{" << entry.first << " => " << entry.second << "}, ";
    }
    std::cout << "}, {";
    for(const auto& entry : arg.test_hashmap) {
        std::cout << "{" << entry.first << " => " << entry.second << "}, ";
    }
    std::cout << "}, PODStruct{" << arg.test_pod.field_one << ", " << arg.test_pod.field_two << ", " << arg.test_pod.a_bool << "} }" << std::endl;
    return set_sum;
}

int main(int argc, char** argv) {
    mutils::DeserializationManager dsm{{}};
    TestObject obj(123456789, "Foo", {666666, 88888888},
                   {-987654321, "Bar", 12345678909876543210ull},
                   {5, 6, 7, 8}, {1, 2, 3, 4}, {9, 9, 9, 9, 9},
                   {{1, "One"}, {2, "Two"}, {3, "Three"}},
                   {{4, "Four"}, {5, "Five"}, {6, "Six"}, {7, "Seven"}},
                   PODStruct{-1, -2, false});
    std::unique_ptr<TestObject> deserialized_obj;
    std::size_t buffer_size = mutils::bytes_size(obj);
    // Scope for test_buffer
    {
        uint8_t test_buffer[buffer_size];
        mutils::to_bytes(obj, test_buffer);
        mutils::context_ptr<TestObject> temp_deserialized_obj = mutils::from_bytes_noalloc<TestObject>(&dsm, test_buffer);
        assert(obj == *temp_deserialized_obj);
        // deserialize_and_run can't take bare functions, it can only take lambdas
        auto result = mutils::deserialize_and_run(&dsm, test_buffer, [](const TestObject& arg) { return test_object_function(arg); });
        assert(result == (5 + 6 + 7 + 8));
        deserialized_obj = mutils::from_bytes<TestObject>(&dsm, test_buffer);
    }
    assert(obj == *deserialized_obj);

    std::cout << "Test passed" << std::endl;
}
