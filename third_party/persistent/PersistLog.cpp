#include "PersistLog.hpp"

namespace ns_persistent {

  PersistLog::PersistLog(const string &name)
  noexcept(true): m_sName(name) {
  }

  PersistLog::~PersistLog() noexcept(true){
  }
}
