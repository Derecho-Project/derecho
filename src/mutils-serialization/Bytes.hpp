#pragma once
#include "SerializationSupport.hpp"
#include "../mutils-networking/connection.hpp"

namespace mutils{

/*
  Intended as a way to alias C-style byte arrays for serialization.  Does not own its own memory; user
  must ensure underlying array is not freed while Bytes lives, and is responsible for freeing 
  underlying array when Bytes is destroyed.  
 */
	struct Bytes : public ByteRepresentable{
		
		char const * const bytes;
		const std::size_t size;
		
		Bytes(decltype(bytes) b, decltype(size) s)
			:bytes(b),size(s){}
		
		std::size_t to_bytes(char* v) const{
			((std::size_t*)(v))[0] = size;
			memcpy(v + sizeof(size),bytes,size);
			return size + sizeof(size);
		}
		
		std::size_t bytes_size() const {
			return size + sizeof(size);
		}
		
		void post_object(const std::function<void (char const * const,std::size_t)>& f) const{
			f((char*)&size,sizeof(size));
			f(bytes,size);
		}
		
		void ensure_registered(DeserializationManager&){}

		//from_bytes is disabled in this implementation, because it's intended only for nocopy-aware scenarios
		template<typename T, typename V>
		static std::unique_ptr<Bytes> from_bytes(T*, V*){
			static_assert(std::is_same<T,V>::value,"Error: from_bytes disabled for mutils::Bytes. See comment in source.");
		}
		
		static context_ptr<Bytes> from_bytes_noalloc(DeserializationManager *, char const * const v)  {
			return context_ptr<Bytes>{new Bytes(v + sizeof(std::size_t),((std::size_t*)(v))[0])};
		}

	};
}
