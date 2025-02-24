#include "ycsb_store_procedure.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "manager.h"
#include "cc_manager.h"
#include "row.h"
#include "table.h"
#include "catalog.h"
#include "index_base.h"
#include "index_hash.h"

#if WORKLOAD == YCSB

YCSBStoreProcedure::YCSBStoreProcedure(TxnManager * txn_man, QueryBase * query)
    : StoreProcedure(txn_man, query)
{
}

YCSBStoreProcedure::~YCSBStoreProcedure()
{
}

RC
YCSBStoreProcedure::execute()
{
    RC rc = RCOK;
    QueryYCSB * query = (QueryYCSB *) _query;
    assert(_query);
    RequestYCSB * requests = query->get_requests();
    // Phase 0: figure out whether we need remote queries; if so, send messages.
    // Phase 1: grab permission of local accesses.
    // Phase 2: after all data is acquired, finish the rest of the transaction.

    if (_phase == 0) {
        for (uint32_t i = 0; i < query->get_request_count(); i++) {
            RequestYCSB &req = requests[i];
            // In phase 0, use row_request to store all accessed rows in _access_set and _remote_set locally
            // No record is reserved locally
            get_cc_manager()->row_request(0, 0, req.key, req.rtype, false, 1, 1);
        }
        _phase = 1;
        // If there are remote requests, rc will be set to LOCAL_MISS, then this function returns
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    // The function does not enter phase 1 until all remote requests return
    // For real workloads, there might be computation between phase 0 and phase 1
    // which executes external logics or computes write values, etc.
    if (_phase == 1) {
        for (uint32_t i = 0; i < query->get_request_count(); i++) {
            RequestYCSB &req = requests[i];
            char fval[10];
            // In phase 1, all accessed rows are stored in the local buffer, use buffer_request to read and write them
            char * data = get_cc_manager()->buffer_request(0, 0, req.key, req.rtype);
            for (int fid = 0; fid < 10; fid ++)
                memcpy(fval, data + 8 + fid*10, 10);
            if (req.rtype == WR) {
                for (int fid = 0; fid < 10; fid ++)
                    memcpy(data + 8 + fid*10, "|xxxxxxx|", 10);
            }
        }
        _phase = 2;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    return RCOK;
}

void
YCSBStoreProcedure::txn_abort()
{
    StoreProcedure::txn_abort();
    _curr_query_id = 0;
    _phase = 0;
}

#endif
