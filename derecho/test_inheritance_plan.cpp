#include "derecho_caller.h"

int test1(int i) {
    std::cout << "called with " << i << std::endl;
    return i;
}

unsigned long test2(int i, double d, Opcode c) {
    std::cout << "called with " << i << "::" << d << "::" << c.id << std::endl;
    return i + d + c.id;
}

auto test3(const std::vector<Opcode> &oc) {
    std::cout << "called with " << oc << std::endl;
    return oc.front();
}

int main() {
    auto hndlers1 = handlers(13, 0, test1, 0, test2, 0, test3);
    /*
    handlers1 = {0,test1} + {0,test2} + {0,test3};
    HANDLERS[0] += test1;
    HANDLERS[0] += test2;
    HANDLERS[0] += test3;*/
    auto hndlers2 = handlers(14, 0, test1, 0, test2, 0, test3);
    who_t all{{Node_id{13}, Node_id{14}}};

    /*
    OrderedQuery<Opcode>(ALL,arguments...) -->
    future<map<Node_id,std::future<Return> > >; //make a type alias that wraps
    this

    P2PQuery<Opcode>(who,arguments...) --> std::future<Return>; //simple case

    P2PSend<Opcode>(who,arguments...) --> void; //simple case

    OrderedSend<Opcode>(ALL,arguments...) --> void;
    OrderedSend<Opcode>(arguments...) --> void; //assume "ALL"

    auto &returned_map = hndlers1->Send<0>(other,1);

    for (auto &pair : returned_map){
        if (pair.second.valid())
            break; //found it!
            } */

    {
        auto ret1 = hndlers1->Send<0>(all, 1);
        assert(ret1.get(Node_id{14}) == 1);
    }
    {
        auto ret2 = hndlers1->Send<0>(all, 1, 2, 3);
        assert(ret2.get(Node_id{14}) == 6);
    }
    {
        auto ret3 =
            hndlers1->Send<0, const std::vector<Opcode> &>(all, {1, 2, 3});
        assert((ret3.get(Node_id{14}) == 1));
    }

    std::cout << "done" << std::endl;
}
