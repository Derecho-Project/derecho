//getting this out of the file; too busy
template<typename...> struct ContainsRemoteInvocableClass;

	template<typename Handler, typename fst, typename... rest>
	struct ContainsRemoteInvocableClass<Handler, fst, rest...> :
		public ContainsRemoteInvocableClass<fst,rest...>
	{

	public:
		std::unique_ptr<Handler> impl;
		using specialized_to = typename Handler::specialized_to;

		ContainsRemoteInvocableClass(std::unique_ptr<Handler> h, std::unique_ptr<fst> h2, std::unique_ptr<rest>... h3)
			:ContainsRemoteInvocableClass<fst,rest...>(std::move(h2),std::move(h3)...),
			impl(std::move(h)){}

		ContainsRemoteInvocableClass(std::unique_ptr<Handler> h, ContainsRemoteInvocableClass<fst,rest...> old)
			:ContainsRemoteInvocableClass<fst,rest...>(std::move(old)),
			impl(std::move(h)){}

		ContainsRemoteInvocableClass(ContainsRemoteInvocableClass&& old)
			:ContainsRemoteInvocableClass<fst,rest...>(std::move(old)),
			impl(std::move(old.impl)){}
		
		ContainsRemoteInvocableClass(const ContainsRemoteInvocableClass&) = delete;

		auto& for_class(specialized_to* np) {
			return impl->for_class(np);
		}

		using ContainsRemoteInvocableClass<fst,rest...>::for_class;
	};

	template<class Handler>
	struct ContainsRemoteInvocableClass<Handler> {

		std::unique_ptr<Handler> impl;
		using specialized_to = typename Handler::specialized_to;

		ContainsRemoteInvocableClass(std::unique_ptr<Handler> h)
			:impl(std::move(h)){}
		
		ContainsRemoteInvocableClass(ContainsRemoteInvocableClass&& old)
			:impl(std::move(old.impl)){}
		
		ContainsRemoteInvocableClass(const ContainsRemoteInvocableClass&) = delete;

		auto& for_class(specialized_to* np) {
			return impl->for_class(np);
		}
	};

template<class Handler>
auto build_CRIC(std::unique_ptr<Handler> h){
	return std::make_unique<ContainsRemoteInvocableClass<Handler> >(std::move(h));
}

template<typename Extend, typename... Existing>
auto extend_CRIC(ContainsRemoteInvocableClass<Existing...> existing, std::unique_ptr<Extend> extend) {
	return std::make_unique<ContainsRemoteInvocableClass<Extend,Existing...> >(std::move(extend),std::move(existing));
};
