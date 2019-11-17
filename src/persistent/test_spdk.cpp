
#include <iostream>
#include <string.h>
#include <derecho/persistent/detail/SPDKPersistLog.hpp>
using namespace std;
using namespace persistent;
using namespace spdk;


static void
printhelp() {
    cout << "usage:" << endl;
    cout << "\tgetbyidx <index>" << endl;
    cout << "\tgetbyver <version>" << endl;
    cout << "\tgetbytime <timestamp>" << endl;
    cout << "\tset <value> <version>" << endl;
    cout << "\ttrimbyidx <index>" << endl;
    cout << "\ttrimbyver <version>" << endl;
    cout << "\ttrimbytime <time>" << endl;
    cout << "\ttruncate <version>" << endl;
    cout << "\tlist" << endl;
    cout << "\thlc" << endl;
    cout << "\tnologsave <int-value>" << endl;
    cout << "\tnologload" << endl;
    cout << "\teval <file|spdk> <datasize> <num> [batch]" << endl;
    cout << "\tlogtail-set <value> <version>" << endl;
    cout << "\tlogtail-list" << endl;
    cout << "\tlogtail-serialize [since-ver]" << endl;
    cout << "\tlogtail-trim <version>" << endl;
    cout << "\tlogtail-apply" << endl;
    cout << "\tdelta-list" << endl;
    cout << "\tdelta-add <op> <version>" << endl;
    cout << "\tdelta-sub <op> <version>" << endl;
    cout << "\tdelta-getbyidx <index>" << endl;
    cout << "\tdelta-getbyver <version>" << endl;
    cout << "NOTICE: test can crash if <datasize> is too large(>8MB).\n"
         << "This is probably due to the stack size is limited. Try \n"
         << "  \"ulimit -s unlimited\"\n"
         << "to remove this limitation." << endl;
}

int main(int argc, char** argv) {
    if(argc < 2) {
        printhelp();
        return 0;
    }

    std::cout << "command:" << argv[1] << std::endl;
    SPDKPersistLog myLog = SPDKPersistLog("mylog");
    SPDKPersistLog myNewLog = SPDKPersistLog("myNewLog");    

    //try {
        if(strcmp(argv[1], "getentrybyindex") == 0){
            int64_t idx = atol(argv[2]);
            LogEntry log_entry = myLog.getLogEntry(idx);

            cout << "output:" << endl;
            cout << "\tlog entry ver: " << log_entry.fields.ver << endl;
            cout << "\tlog entry dlen: " << log_entry.fields.dlen << endl;
            cout << "\tlog entry ofst: " << log_entry.fields.ofst << endl;        
            return 0;    
        } else if (strcmp(argv[1], "getEarliestIndex") == 0){
            int64_t index = myLog.getEarliestIndex();
            cout << "output:" << endl;
            cout << "\tearliest index: " << index << endl;
            return 0;
        } else if (strcmp(argv[1], "getLatestsIndex") == 0) {
            int64_t index = myLog.getLatestIndex();
            cout << "output:" << endl;
            cout << "\tlatests index: " << index << endl;
            return 0;
        } else if (strcmp(argv[1], "set") == 0) {
            char* v = argv[2];
            int64_t ver = (int64_t)atoi(argv[3]);
            HLC mhlc;
            uint64_t dlen = strlen(v) + 1;
            cout << "params:" << endl;
            cout << "\tver: " << ver << endl;
            cout << "\tdlen: " << dlen << endl;
            
            myLog.append(v, dlen, ver, mhlc);
            int64_t idx = myLog.getLatestIndex();
            LogEntry log_entry = myLog.getLogEntry(idx);
            cout << "read entry:" << endl;
            cout << "\tver: " << log_entry.fields.ver << endl;
            cout << "\tdlen: " << log_entry.fields.dlen << endl;
        } else if (strcmp(argv[1], "truncate") == 0) {
            int64_t ver = atol(argv[2]);
            myLog.truncate(ver);
        } else if (strcmp(argv[1], "zeroout") == 0) {
            myLog.zeroout();
        } else if (strcmp(argv[1], "getEntryByIndex") == 0) {
            int64_t index = (int64_t)atol(argv[2]);
            char* v = (char*)myLog.getEntryByIndex(index);
            cout << "read data:" << endl;
            cout << "\tdata: " << v << endl;
        } else if (strcmp(argv[1], "getEntryByVer") == 0) {
            int64_t ver = (int64_t)atol(argv[2]);
            char* v = (char*)myLog.getEntry(ver);
            cout << "read data:" << endl;
            cout << "\tdata: " << v << endl;
        } else if (strcmp(argv[1], "getVersionIndex") == 0) {
            int64_t ver = (int64_t)atol(argv[2]);
            uint64_t index = myLog.getVersionIndex(ver);
            cout << "version idx" << endl;
            cout << "\tidxL " << index << endl;
        } else if (strcmp(argv[1], "trimByIndex") == 0) {
            uint64_t index = (uint64_t)atol(argv[2]);
            myLog.trimByIndex(index);
        } else if (strcmp(argv[1], "advanceVersion") == 0) {
            int64_t ver = (int64_t)atol(argv[2]);
            myLog.advanceVersion(ver);
        } else if (strcmp(argv[1], "trimByVer") == 0) {
            int64_t ver = (int64_t)atol(argv[2]);
            
            myLog.trim(ver);
        } else if (strcmp(argv[1], "setnew") == 0) {
            char* v = argv[2];
            int64_t ver = (int64_t)atoi(argv[3]);
            HLC mhlc;
            uint64_t dlen = strlen(v) + 1;
            cout << "params:" << endl;
            cout << "\tver: " << ver << endl;
            cout << "\tdlen: " << dlen << endl;
            
           // myNewLog.append(v, dlen, ver, mhlc);
           // int64_t idx = myNewLog.getLatestIndex();
           // LogEntry log_entry = myNewLog.getLogEntry(idx);
           // cout << "read entry:" << endl;
           // cout << "\tver: " << log_entry.fields.ver << endl;
           // cout << "\tdlen: " << log_entry.fields.dlen << endl;
	} else if (strcmp(argv[1], "getLatestsIndexnew") == 0) {
	   // myNewLog.getLatestIndex();
	} else if (strcmp(argv[1], "getEntryByIndexnew") == 0) {
	   // int64_t index = (int64_t)atol(argv[2]);
	   // char* v = (char*)myNewLog.getEntryByIndex(index);	    
	   // cout << "read data:" << endl;
	   // cout << "\tdata: " << v << endl;
	} else if (strcmp(argv[1], "getLBA") == 0) {
            uint64_t lba_index = (uint64_t)atol(argv[2]);
            char* v = (char*)myLog.getLBA(lba_index);
            cout << "read data:" << endl;
            cout << "\tdata: " << v << endl;
        }
	return 0;
}
  
