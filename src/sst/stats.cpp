#include <derecho/sst/stats.hpp>

uint32_t next_id = 0;
std::map<uint32_t, StatBlock> table;

StatBlock::StatBlock() {
  evaluated = 0;
  fired = 0;
}

PredicateStatistics::PredicateStatistics() {
}
uint32_t PredicateStatistics::get_id() {
  StatBlock s;
  table[next_id] = s;
  return next_id++;
}
void PredicateStatistics::fired(uint32_t id) {
  table[id].fired++;
}
void PredicateStatistics::evaluated(uint32_t id) {
  table[id].evaluated++;
}