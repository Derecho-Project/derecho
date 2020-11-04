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
  // PredicateStatistics();
  static uint32_t get_id();
  static void fired(uint32_t);
  static void evaluated(uint32_t);
private:
  static uint32_t next_id;
  static std::map<uint32_t, StatBlock> table;
};