#include "SerializationSupport.hpp"

using namespace mutils;

int main() {
	std::vector<int> v{1,2,3,4};
	auto size = bytes_size(v);
	char c[size];
	bzero(c,size);
	to_bytes(v,c);
	deserialize_and_run<std::vector<int> >(nullptr,c,[&](std::vector<int>& v2){
			assert(v2 == v);
		});

	deserialize_and_run<int >(nullptr,c,[&](int& v2){
			assert(v2 == v.size());
		});

	deserialize_and_run(nullptr,c,[&](int& size, int& v0, int& v1, int &v2, int &v3){
			assert(size == v.size());
			assert(v0 == v.at(0));
			assert(v1 == v.at(1));
			assert(v2 == v.at(2));
			assert(v3 == v.at(3));
		});
}
