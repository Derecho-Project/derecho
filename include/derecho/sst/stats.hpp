#pragma once

#include <map>

// stats for a single id
class StatBlock {
public:
  StatBlock();
  uint32_t evaluated;
  uint32_t fired;
};

class PredicateStatistics {
public:
  PredicateStatistics();
  uint32_t get_id();
  void fired(uint32_t);
  void evaluated(uint32_t);
private:
  uint32_t next_id;
  std::map<uint32_t, StatBlock> table;
};