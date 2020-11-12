#include <derecho/sst/stats.hpp>

uint32_t PredicateStatistics::next_id = 0;
std::map<uint32_t, StatBlock> PredicateStatistics::table;

StatBlock::StatBlock() {
  evaluated = 0;
  fired = 0;
}

std::string StatBlock::to_string() {
  return "Evaluated " + std::to_string(evaluated) + "\tFired: " + std::to_string(fired);
}

uint32_t PredicateStatistics::gen_id() {
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

std::string PredicateStatistics::to_string() {
  std::map<uint32_t, StatBlock>::iterator it;
  std::string s;
  for (it = PredicateStatistics::table.begin(); it != PredicateStatistics::table.end(); it++) {
    s += "ID: " + std::to_string(it->first) + "\n" + (it->second).to_string();
  }
  return s;
}