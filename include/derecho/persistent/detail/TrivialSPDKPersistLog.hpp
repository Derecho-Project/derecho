#include "spdk/env.h"
#include "spdk/nvme.h"
#include <string>


using namespace std;
namespace persistent {

namespace spdk {


class TrivialSPDKPersistLog{
public:
    TrivialSPDKPersistLog(const std::string& name); 
    virtual ~TrivialSPDKPersistLog(); 

};

}
}
