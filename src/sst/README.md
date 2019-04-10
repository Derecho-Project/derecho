# SST: Shared State Table

**SST** defines a **state table** for its members containing a row for each node which stores the local state of each object. The state variables are defined as a C++ _POD struct_ with no pointers. Nodes can register **predicates** (properties on the state table) locally with the SST which fire functions called **triggers** which perform local computation and update the local row, if necessary. SST can be used in reads mode, which uses one-sided RDMA reads to update the table, or writes mode, which uses one-sided RDMA writes to update the table. It optimizes for RDMA operations and abstracts them from the programmer, thus providing a convenient interface to code applications to run over RDMA networks.


