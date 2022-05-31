#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <functional>

using namespace mutils;

int main() {
	std::vector<int> v{1,2,3,4};
	auto size = bytes_size(v);
	uint8_t c[size];
	bzero(c,size);
	to_bytes(v,c);
/*
    std::function<void (const std::vector<int>&)> fun1 = [&](const std::vector<int>& v2){
			assert(v2 == v);
		};
	deserialize_and_run(nullptr,c,fun1);
*/
	deserialize_and_run(nullptr,c,[&](const int& v2) {
			assert(v2 == v.size());
            return;
		});

	deserialize_and_run(nullptr,c,[&](const int& size, const int& v0, const int& v1, const int &v2, const int &v3){
			assert(size == (int)v.size());
			assert(v0 == v.at(0));
			assert(v1 == v.at(1));
			assert(v2 == v.at(2));
			assert(v3 == v.at(3));
            return;
		});
}
