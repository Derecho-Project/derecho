#include <iostream>
#include "ObjectStore.hpp"

int main(int argc, char **argv) {
    if (argc < 3 || (argc > 3 && strcmp(argv[argc-3],"--"))) {
        std::cout << "Usage:" << argv[0] << 
          " [ derecho/objectstore-config-list --] " <<
          " <some-random-msg> <count> " << std::endl;
        return -1;
    }
    // get rand message
    char msg[1024];
    strcpy(msg,argv[argc-2]);
    int ofst = strlen(msg);
    strcpy(msg+ofst,"--------");
    const int count = std::stoi(argv[argc-1]);

    // oss - objectstore service
    auto& oss = objectstore::IObjectStoreService::get(argc, argv, 
        [&](const objectstore::OID& oid, const objectstore::Object& object){
            std::cout << "watcher: " << oid << "->" << object << std::endl;
        });
    // print some message
    std::cout << "Replica:\t" << oss.isReplica() << std::endl;

    // send message
    objectstore::Object object(
        derecho::getConfUInt64(CONF_DERECHO_LOCAL_ID),
        msg,
        strlen(msg)+1);

    for (int i=0;i<count;i++) {
        sprintf(msg+ofst,"[%d]",i);
        oss.put(object);
    }

    // TODO: get message

    // TODO: leave gracefully.
    oss.leave();
}
