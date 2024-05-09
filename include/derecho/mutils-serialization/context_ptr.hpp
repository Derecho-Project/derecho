#pragma once

#include <derecho/config.h>
#include <memory>
#include <type_traits>

namespace mutils{

    template<typename T>
    struct ContextDeleter : public std::conditional_t<std::is_pod<T>::value,
                                                      ContextDeleter<void>,
                                                      std::default_delete<T> > {
    };

    template<>
    struct ContextDeleter<void> {
        void operator()(...){}
    };               

    
    template<typename T>
    using context_ptr = std::unique_ptr<T,ContextDeleter<T> >;

}
