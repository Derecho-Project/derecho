#pragma once

#include <type_traits>
#include <tuple>
#include <functional>

namespace sst {
namespace util {

#define restrict(x) typename ignore = typename std::enable_if<(x)>::type

	template <typename T>
	struct function_traits
		: public function_traits<decltype(&T::operator())>
	{};
	// For generic types, directly use the result of the signature of its 'operator()'

	template <typename ClassType, typename ReturnType, typename... Args>
	struct function_traits<ReturnType(ClassType::*)(Args...) const>
	// we specialize for pointers to member function
	{
		enum { arity = sizeof...(Args) };
		// arity is the number of arguments.

		typedef ReturnType result_type;

		template <size_t i>
		struct arg
		{
			typedef typename std::tuple_element<i, std::tuple<Args...>>::type type;
			// the i-th argument is equivalent to the i-th tuple element of a tuple
			// composed of those arguments.
		};

		template<typename T>
		static std::function<ReturnType (Args...)> as_function(T t){
			return t;
		}

		typedef ReturnType (*fp_t) (Args...);

		typedef std::tuple<Args...> args_tuple;

		template<typename T>	
		static ReturnType (*as_fp(T t)) (Args...) {
			return t;
		}

	};

	template <typename ClassType, typename ReturnType>
	struct function_traits<ReturnType(ClassType::*)() const>
	{
		enum { arity = 0 };
		typedef ReturnType result_type;
		template<typename T>
		static std::function<ReturnType ()> as_function(T t){
			return t;
		}

		template<typename T>	
		static ReturnType (*as_fp(T t)) () {
			return t;
		}
	};

	template<typename R, typename... Args>
	std::function<R (Args...)> convert(R (*f)(Args...))
	{
		return f;
	}


	template<typename F, restrict(!std::is_function<F>::value)>
	auto convert(F f) 
	{
		return function_traits<F>::as_function(f);
	}



	template<typename R, typename... Args>
	constexpr R (*convert_fp(R (*f)(Args...)))(Args...) 
	{
		return f;
	}


	template<typename F, restrict(!std::is_function<F>::value)>
	auto convert_fp(F f) 
	{
		return function_traits<F>::as_fp(f);
	}
}
}
