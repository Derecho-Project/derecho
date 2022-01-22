#include "SerializationSupport.hpp"
#include "SerializationMacros.hpp"
#include <cstdlib>

using namespace mutils;
using namespace std;

struct TestSerialization : public ByteRepresentable{
	const int a;
	const long b;

	TestSerialization(int a, long b):a(a),b(b){}

	DEFAULT_SERIALIZATION_SUPPORT(TestSerialization,a,b);
};

int main(){
	DeserializationManager dsm{{}};
	{
		int i = 3;
		auto size = bytes_size(i);
		TestSerialization ts{1,1};
		uint8_t *c = (uint8_t*) malloc(ts.bytes_size());
		to_bytes(ts,c);
		auto ts2 = from_bytes<TestSerialization>(&dsm,c);
		free(c);
		assert(ts.a == ts2->a);
		assert(ts.b == ts2->b);
	}

	{
		int foo;
		uint8_t* c = (uint8_t*) malloc(bytes_size(foo));
		to_bytes(foo,c);
		auto foo2 = from_bytes<int>(&dsm,c);
		free(c);
		assert(foo == *foo2);
	}

}
