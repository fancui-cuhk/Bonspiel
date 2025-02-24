#include "store_procedure.h"
#include "cc_manager.h"
#include "manager.h"
#include "txn.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "workload.h"
#include "index_base.h"
#include "tictoc_manager.h"
#include "silo_manager.h"
#include "docc_manager.h"
#include "sundial_manager.h"
#include "packetize.h"

StoreProcedure::~StoreProcedure()
{
    delete _query;
}

void
StoreProcedure::init() {
    _self_abort = false;
    _phase = 0;
    _curr_row = NULL;
    _curr_query_id = 0;
    remote_requests.clear();
}

void
StoreProcedure::set_query(QueryBase * query)
{
    assert(_query);
    delete _query;
    _query = query;
}

CCManager *
StoreProcedure::get_cc_manager()
{
    return _txn->get_cc_manager();
}

void
StoreProcedure::txn_abort()
{
    _phase = 0;
    _curr_row = NULL;
    _curr_query_id = 0;
    remote_requests.clear();
}

RC
StoreProcedure::process_remote_req(uint32_t size, char * data)
{
    RC rc = RCOK;
    UnstructuredBuffer buffer(data);
    while (buffer.get_offset() < size) {
        // | type | table_id | index_id | key | reserve |
        access_t type;
        uint32_t table_id = 0;
        uint32_t index_id = 0;
        uint64_t key = 0;
        bool reserve = false;
        uint32_t num_aborts = 0;
        buffer.get( &type );
        buffer.get( &table_id );
        buffer.get( &index_id );
        buffer.get( &key );
        buffer.get( &reserve );
        buffer.get( &num_aborts );
        _txn->set_num_aborts(num_aborts);

        if (type == PWR)
            get_cc_manager()->buffer_request(table_id, index_id, key, WR);
        else {
            // Remote requests might return 0 row in TPCE
            // TODO: For simplicity, remote requests return at most 1 row currently
            get_cc_manager()->row_request(table_id, index_id, key, type, reserve, 0, 1);
        }
        assert(buffer.get_offset() <= size);
    }

    rc = get_cc_manager()->process_requests();
    assert(rc != LOCAL_MISS);
    return rc;
}
