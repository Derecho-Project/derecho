#ifndef ROUTING_LSDB_ROW_H_
#define ROUTING_LSDB_ROW_H_

namespace sst {

namespace experiments {

/**
 * Represents a row of the link-state database.
 */
template<unsigned int SIZE>
struct LSDB_Row {
	volatile int link_cost[SIZE];
	volatile int barrier;
};

}

}

#endif /* ROUTING_LSDB_ROW_H_ */
