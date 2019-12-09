#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/HLC.hpp>
#include <derecho/persistent/Persistent.hpp>
#include <derecho/persistent/detail/util.hpp>
#include <iostream>
#include <signal.h>
#include <spdlog/spdlog.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>

using namespace persistent;
using namespace mutils;
using std::cout;
using std::endl;
using std::cerr;

void sig_handler(int num) {
    printf("I received signal:%d.\n", num);
}

// A test class
class X {
public:
    int x;
    const std::string to_string() {
        return std::to_string(x);
    }
};

class ReplicatedT {
};

PersistentRegistry pr(nullptr, typeid(ReplicatedT), 123, 321);

#define MAX_VB_SIZE (1ull << 30)
// A variable that can change the length of its value
class VariableBytes : public ByteRepresentable {
public:
    std::size_t data_len;
    char buf[MAX_VB_SIZE];

    VariableBytes() {
        data_len = MAX_VB_SIZE;
    }

    virtual std::size_t to_bytes(char* v) const {
        memcpy(v, buf, this->data_len);
        return data_len;
    };

    virtual void post_object(const std::function<void(char const* const, std::size_t)>& func) const {
        func(this->buf, this->data_len);
    };

    virtual std::size_t bytes_size() const {
        return this->data_len;
    };

    virtual void ensure_registered(DeserializationManager& dsm){
            // do nothing, we don't need DSM.
    };

    virtual std::string to_string() {
        return std::string{buf};
    };

    static std::unique_ptr<VariableBytes> from_bytes(DeserializationManager* dsm, char const* const v) {
        VariableBytes vb;
        std::unique_ptr<VariableBytes> pvb = std::make_unique<VariableBytes>();
        strcpy(pvb->buf, v);
        pvb->data_len = strlen(v) + 1;
        return pvb;
    };
};

///////////////////////////////////////////////////////////////////////////////
// test delta
class IntegerWithDelta : public ByteRepresentable, IDeltaSupport<IntegerWithDelta> {
public:
    int value;
    int delta;
    IntegerWithDelta(int v) : value(v), delta(0) {}
    IntegerWithDelta() : value(0), delta(0) {}
    int add(int op) {
        this->value += op;
        this->delta += op;
        return this->value;
    }
    int sub(int op) {
        this->value -= op;
        this->delta -= op;
        return this->value;
    }
    virtual void finalizeCurrentDelta(const DeltaFinalizer& dp) {
        // finalize current delta
        dp((char const* const) & (this->delta), sizeof(this->delta));
        // clear delta
        this->delta = 0;
    }
    virtual void applyDelta(char const* const pdat) {
        // apply delta
        this->value += *((const int* const)pdat);
    }
    static std::unique_ptr<IntegerWithDelta> create(mutils::DeserializationManager* dm) {
        // create
        return std::make_unique<IntegerWithDelta>();
    }

    virtual const std::string to_string() {
        return std::to_string(this->value);
    };

    DEFAULT_SERIALIZATION_SUPPORT(IntegerWithDelta, value);
};
///////////////////////////////////////////////////////////////////////////////

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
    cout << "\tvolatile" << endl;
    cout << "\thlc" << endl;
    cout << "\tnologsave <int-value>" << endl;
    cout << "\tnologload" << endl;
    cout << "\teval <file|mem> <datasize> <num> [batch]" << endl;
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

Persistent<X> px1([]() { return std::make_unique<X>(); }, nullptr, &pr);
//Persistent<X> px1;
Persistent<VariableBytes> npx([]() { return std::make_unique<VariableBytes>(); }, nullptr, &pr),
        npx_logtail([]() { return std::make_unique<VariableBytes>(); });
//Persistent<X,ST_MEM> px2;
Volatile<X> px2([]() { return std::make_unique<X>(); });
Persistent<IntegerWithDelta> dx([]() { return std::make_unique<IntegerWithDelta>(); }, nullptr, &pr);

template <typename OT, StorageType st = ST_FILE>
void listvar(Persistent<OT, st>& var) {
    int64_t nv = var.getNumOfVersions();
    int64_t idx = var.getEarliestIndex();
    cout << "Number of Versions:\t" << nv << endl;
    while(nv--) {
        /*
    // by lambda
    var.getByIndex(nv,
      [&](OT& x) {
        cout<<"["<<nv<<"]\t"<<x.to_string()<<"\t//by lambda"<<endl;
      });
*/
        // by copy
        cout << "[" << idx << "]\t" << var.getByIndex(idx)->to_string() << "\t//by copy" << endl;
        idx++;
    }
    // list minimum latest persisted version:
    cout << "list minimum latest persisted version:" << getMinimumLatestPersistedVersion(typeid(ReplicatedT), 123, 321) << endl;
}

static void nologsave(int value) {
    saveObject(value);
    saveObject<int, ST_MEM>(value);
}

static void nologload() {
    cout << "in file:" << *loadObject<int>() << endl;
    cout << "in memory:" << *loadObject<int, ST_MEM>() << endl;
}

static void test_hlc();
template <StorageType st = ST_FILE>
static void eval_write(std::size_t osize, int nops, bool batch) {
    VariableBytes writeMe;
    Persistent<VariableBytes, st> pvar([]() { return std::make_unique<VariableBytes>(); });
    writeMe.data_len = osize;
    struct timespec ts, te;
    int cnt = nops;
    int64_t ver = pvar.getLatestVersion();
    ver = (ver == INVALID_VERSION) ? 0 : ver + 1;
    clock_gettime(CLOCK_REALTIME, &ts);
    while(cnt-- > 0) {
        pvar.set(writeMe, ver++);
        if(!batch) pvar.persist();
    }
    if(batch) {
        pvar.persist();
    }

#if defined(_PERFORMANCE_DEBUG)
    pvar.print_performance_stat();
#endif  //_PERFORMANCE_DEBUG

    clock_gettime(CLOCK_REALTIME, &te);
    long sec = (te.tv_sec - ts.tv_sec);
    long nsec = sec * 1000000000 + te.tv_nsec - ts.tv_nsec;
    dbg_default_warn("nanosecond={}\n", nsec);
    double thp_MBPS = (double)osize * nops / (double)nsec * 1000;
    double lat_us = (double)nsec / nops / 1000;
    cout << "WRITE TEST(st=" << st << ", size=" << osize << " byte, ops=" << nops << ")" << endl;
    cout << "throughput:\t" << thp_MBPS << " MB/s" << endl;
    cout << "latency:\t" << lat_us << " microseconds" << endl;
}

int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::trace);

    signal(SIGSEGV, sig_handler);

    if(argc < 2) {
        printhelp();
        return 0;
    }

    std::cout << "command:" << argv[1] << std::endl;

    try {
        if(strcmp(argv[1], "list") == 0) {
            cout << "Persistent<VariableBytes> npx:" << endl;
            listvar<VariableBytes>(npx);
            //cout<<"Persistent<X,ST_MEM> px2:"<<endl;
            //listvar<X,ST_MEM>(px2);
        } else if(strcmp(argv[1], "logtail-list") == 0) {
            cout << "Persistent<VariableBytes> npx:" << endl;
            listvar<VariableBytes>(npx_logtail);
        } else if(strcmp(argv[1], "getbyidx") == 0) {
            int64_t nv = atol(argv[2]);
            // by lambda
            /*
      npx.getByIndex(nv,
        [&](VariableBytes& x) { 
          cout<<"["<<nv<<"]\t"<<x.to_string()<<"\t//by lambda"<<endl;
        });
*/
            // by copy
            cout << "[" << nv << "]\t" << npx.getByIndex(nv)->to_string() << "\t//by copy" << endl;
        } else if(strcmp(argv[1], "getbyver") == 0) {
            int64_t ver = atoi(argv[2]);
            /*
      // by lambda
      npx.get(ver,
        [&](VariableBytes& x) { 
          cout<<"["<<(uint64_t)(ver>>64)<<"."<<(uint64_t)ver<<"]\t"<<x.to_string()<<"\t//by lambda"<<endl;
        });
*/
            // by copy
            cout << "[" << ver << "]\t" << npx.get(ver)->to_string() << "\t//by copy" << endl;
        } else if(strcmp(argv[1], "getbytime") == 0) {
            HLC hlc;
            hlc.m_rtc_us = atol(argv[2]);
            hlc.m_logic = 0;
            /*
      npx.get(hlc,
        [&](VariableBytes& x) {
          cout<<"[("<<hlc.m_rtc_us<<",0)]\t"<<x.to_string()<<"\t//bylambda"<<endl;
        });
*/
            cout << "["
                 << "[(" << hlc.m_rtc_us << ",0)]\t" << npx.get(hlc)->to_string() << "\t//by copy" << endl;
        } else if(strcmp(argv[1], "trimbyidx") == 0) {
            int64_t nv = atol(argv[2]);
            npx.trim(nv);
            npx.persist();
            cout << "trim till index " << nv << " successfully" << endl;
        } else if(strcmp(argv[1], "trimbyver") == 0) {
            int64_t ver = atol(argv[2]);
            npx.trim(ver);
            npx.persist();
            cout << "trim till ver " << ver << " successfully" << endl;
        } else if(strcmp(argv[1], "truncate") == 0) {
            int64_t ver = atol(argv[2]);
            npx.truncate(ver);
            cout << "truncated after version" << ver << "successfully" << endl;
        } else if(strcmp(argv[1], "trimbytime") == 0) {
            HLC hlc;
            hlc.m_rtc_us = atol(argv[2]);
            hlc.m_logic = 0;
            npx.trim(hlc);
            npx.persist();
            cout << "trim till time " << hlc.m_rtc_us << " successfully" << endl;
        } else if(strcmp(argv[1], "set") == 0) {
            char* v = argv[2];
            int64_t ver = (int64_t)atoi(argv[3]);
            sprintf((*npx).buf, "%s", v);
            (*npx).data_len = strlen(v) + 1;
            npx.version(ver);
            npx.persist();
        } else if(strcmp(argv[1], "logtail-set") == 0) {
            char* v = argv[2];
            int64_t ver = (int64_t)atoi(argv[3]);
            sprintf((*npx_logtail).buf, "%s", v);
            (*npx_logtail).data_len = strlen(v) + 1;
            npx_logtail.version(ver);
            npx_logtail.persist();
        }
#define LOGTAIL_FILE "logtail.ser"
        else if(strcmp(argv[1], "logtail-serialize") == 0) {
            int64_t ver = INVALID_VERSION;
            if(argc >= 3) {
                ver = (int64_t)atoi(argv[2]);
            }
            PersistentRegistry::setEarliestVersionToSerialize(ver);
            ssize_t ds1 = npx_logtail.bytes_size();
            ssize_t prefix = mutils::bytes_size(npx_logtail.getObjectName()) + mutils::bytes_size(*npx_logtail);
            char* buf = (char*)malloc(ds1);
            if(buf == NULL) {
                cerr << "faile to allocate " << ds1 << " bytes for serialized data. prefix=" << prefix << " bytes" << endl;
                return -1;
            }
            ssize_t ds2 = npx_logtail.to_bytes(buf);
            cout << "serialization requested " << (ds1 - prefix) << " bytes, used " << (ds2 - prefix) << " bytes" << endl;
            int fd = open(LOGTAIL_FILE, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
            if(fd == -1) {
                cerr << "failed to open file " << LOGTAIL_FILE << endl;
                return -1;
            }
            ssize_t ds3 = write(fd, (void*)((uint64_t)buf + prefix), ds2 - prefix);
            if(ds3 == -1) {
                cerr << "failed to write the buffer to file " << LOGTAIL_FILE << endl;
                free(buf);
                return -1;
            }
            free(buf);
            close(fd);
        } else if(strcmp(argv[1], "logtail-trim") == 0) {
            int64_t ver = atol(argv[2]);
            npx_logtail.trim(ver);
            npx_logtail.persist();
            cout << "logtail-trim till ver " << ver << " successfully" << endl;
        } else if(strcmp(argv[1], "logtail-apply") == 0) {
            //load the serialized logtail.
            int fd = open(LOGTAIL_FILE, O_RDONLY);
            if(fd == -1) {
                cerr << "failed to open file " << LOGTAIL_FILE << endl;
                return -1;
            }

            off_t fsize = lseek(fd, 0, SEEK_END);
            lseek(fd, 0, SEEK_CUR);

            void* buf = mmap(NULL, (size_t)fsize, PROT_READ, MAP_SHARED, fd, 0);
            if(buf == MAP_FAILED) {
                cerr << "failed to map buffer." << endl;
                return -1;
            }

            cout << "before applyLogTail." << endl;
            npx_logtail.applyLogTail(nullptr, (char*)buf);
            cout << "after applyLogTail." << endl;

            munmap(buf, (size_t)fsize);
            close(fd);
        } else if(strcmp(argv[1], "volatile") == 0) {
            cout << "loading Persistent<X,ST_MEM> px2" << endl;
            listvar<X, ST_MEM>(px2);
            int64_t ver = (int64_t)0L;
            X x;
            x.x = 1;
            px2.set(x, ver++);
            px2.persist();
            cout << "after set 1" << endl;
            listvar<X, ST_MEM>(px2);
            x.x = 10;
            px2.set(x, ver++);
            px2.persist();
            cout << "after set 10" << endl;
            listvar<X, ST_MEM>(px2);
            x.x = 100;
            px2.set(x, ver++);
            px2.persist();
            cout << "after set 100" << endl;
            listvar<X, ST_MEM>(px2);
        } else if(strcmp(argv[1], "hlc") == 0) {
            test_hlc();
        } else if(strcmp(argv[1], "nologsave") == 0) {
            nologsave(atoi(argv[2]));
        } else if(strcmp(argv[1], "nologload") == 0) {
            nologload();
        } else if(strcmp(argv[1], "eval") == 0) {
            // eval file|mem osize nops
            int osize = atoi(argv[3]);
            int nops = atoi(argv[4]);
            bool batch = false;

            if(argc >= 6) {
                batch = (strcmp(argv[5], "batch") == 0);
            }

            if(strcmp(argv[2], "file") == 0) {
                eval_write<ST_FILE>(osize, nops, batch);
            } else if(strcmp(argv[2], "mem") == 0) {
                eval_write<ST_MEM>(osize, nops, batch);
            } else {
                cout << "unknown storage type:" << argv[2] << endl;
            }
        } else if(strcmp(argv[1], "delta-add") == 0) {
            int op = std::stoi(argv[2]);
            int64_t ver = (int64_t)atoi(argv[3]);
            cout << "add(" << op << ") = " << (*dx).add(op) << endl;
            dx.version(ver);
            dx.persist();
        } else if(strcmp(argv[1], "delta-sub") == 0) {
            int op = std::stoi(argv[2]);
            int64_t ver = (int64_t)atoi(argv[3]);
            cout << "sub(" << op << ") = " << (*dx).sub(op) << endl;
            dx.version(ver);
            dx.persist();
        } else if(strcmp(argv[1], "delta-list") == 0) {
            cout << "Persistent<IntegerWithDelta>:" << endl;
            listvar<IntegerWithDelta>(dx);
        } else if(strcmp(argv[1], "delta-getbyidx") == 0) {
            int64_t index = std::stoi(argv[1]);
            cout << "dx[idx:" << index << "] = " << dx.getByIndex(index)->value << endl;
        } else if(strcmp(argv[1], "delta-getbyver") == 0) {
            int64_t version = std::stoi(argv[1]);
            cout << "dx[idx:" << version << "] = " << dx[version]->value << endl;
        } else {
            cout << "unknown command: " << argv[1] << endl;
            printhelp();
        }
    } catch(unsigned long long exp) {
        cerr << "Exception captured:0x" << std::hex << exp << endl;
        return -1;
    }

    return 0;
}

static inline void print_hlc(const char* name, const HLC& hlc) {
    cout << "HLC\t" << name << "(" << hlc.m_rtc_us << "," << hlc.m_logic << ")" << endl;
}

void test_hlc() {
    cout << "creating 2 HLC: h1 and h2." << endl;
    HLC h1, h2;
    h1.tick(h2);
    print_hlc("h1", h1);
    print_hlc("h2", h2);

    cout << "\nh1.tick()\t" << endl;
    h1.tick();
    print_hlc("h1", h1);
    print_hlc("h2", h2);

    cout << "\nh2.tick(h1)\t" << endl;
    h2.tick(h1);
    print_hlc("h1", h1);
    print_hlc("h2", h2);

    cout << "\ncomparison" << endl;
    cout << "h1>h2\t" << (h1 > h2) << endl;
    cout << "h1<h2\t" << (h1 < h2) << endl;
    cout << "h1>=h2\t" << (h1 >= h2) << endl;
    cout << "h1<=h2\t" << (h1 <= h2) << endl;
    cout << "h1==h2\t" << (h1 == h2) << endl;

    cout << "\nevaluation:h1=h2" << endl;
    h1 = h2;
    print_hlc("h1", h1);
    print_hlc("h2", h2);
    cout << "h1>h2\t" << (h1 > h2) << endl;
    cout << "h1<h2\t" << (h1 < h2) << endl;
    cout << "h1>=h2\t" << (h1 >= h2) << endl;
    cout << "h1<=h2\t" << (h1 <= h2) << endl;
    cout << "h1==h2\t" << (h1 == h2) << endl;
}
