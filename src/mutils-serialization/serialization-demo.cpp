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
		char *c = (char*) malloc(ts.bytes_size());
		to_bytes(ts,c);
		auto ts2 = from_bytes<TestSerialization>(&dsm,c);
		free(c);
		assert(ts.a == ts2->a);
		assert(ts.b == ts2->b);
	}

	{
		int foo;
		char* c = (char*) malloc(bytes_size(foo));
		to_bytes(foo,c);
		auto foo2 = from_bytes<int>(&dsm,c);
		free(c);
		assert(foo == *foo2);
	}
	
}
