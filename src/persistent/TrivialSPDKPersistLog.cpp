#include <derecho/persistent/detail/TrivialSPDKPersistLog.hpp>
#include <iostream>
using namespace std;
namespace persistent {
namespace spdk {

static bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid, 
                     struct spdk_nvme_ctrlr_opts *opts)
{
//    cout << "Attaching to: " << trid->traddr << endl;
    return true;
}

static void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                      struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
//    cout << "Attached to: " << trid->traddr << endl;
}

TrivialSPDKPersistLog::TrivialSPDKPersistLog(const string& name) {
 //   cout << name << endl;
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);

    opts.name = "hello_world";
    opts.shm_id = 0;
    if(spdk_env_init(&opts) < 0) {
        cout << "Failed to initialize spdk namespace: " << spdk_env_init(&opts) << endl;
    }

    int rc = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL);
}

TrivialSPDKPersistLog::~TrivialSPDKPersistLog() {
}

}
}
