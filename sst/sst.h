template <class DerivedSST>
class SST {
  // pointer to the user's derived SST object
  DerivedSST* derived_ptr;
  
  // members of the SST
  std::vector<node_id> members;

  // row index owned by the local node
  uint32_t my_row;

  // memory region ids for the rows of the SST
  std::vector<MemoryRegion> row_mr_ids;

  // pointer to memory where the SST rows are stored
  volatile char* table;
  
  // length of each row in this SST
  size_t row_len;

  // everything else is the same
public:
  SST(DerivedSST* derived_ptr, std::vector<node_id> members, node_id my_id);
  void update_remote_rows();
};
