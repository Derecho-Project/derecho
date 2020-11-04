#include <derecho/sst/stats.hpp>

uint32_t PredicateStatistics::next_id = 0;
std::map<uint32_t, StatBlock> PredicateStatistics::table;

StatBlock::StatBlock() {
  evaluated = 0;
  fired = 0;
}

uint32_t PredicateStatistics::get_id() {
  StatBlock s;
  PredicateStatistics::table[next_id] = s;
  return PredicateStatistics::next_id++;
}
void PredicateStatistics::fired(uint32_t id) {
  PredicateStatistics::table[id].fired++;
}
void PredicateStatistics::evaluated(uint32_t id) {
  PredicateStatistics::table[id].evaluated++;
}