#include "cc_manager.h"
#include "store_procedure.h"
#include "lock_manager.h"
#include "tictoc_manager.h"
#include "silo_manager.h"
#include "docc_manager.h"
#include "sundial_manager.h"
#include "row_docc.h"
#include "row_sundial.h"
#include "row_lock.h"
#include "index_btree.h"
#include "index_hash.h"
#include "manager.h"
#include "txn.h"
#include "row.h"
#include "table.h"
#include "workload.h"
#include "message.h"
#include "transport.h"

CCManager::CCManager(TxnManager * txn, uint64_t ts)
    : _txn(txn)
{
    _timestamp = ts;
    _time_in_cc_man = 0;
    init();
}

void
CCManager::init()
{
    _restart = false;
    _waiting_requests = false;
    _curr_request_idx = 0;
    _curr_row_idx = 0;
    _state = CC_EXECUTING;
    _deletes.clear();
    _inserts.clear();
    _remote_node_info.clear();
}

StoreProcedure *
CCManager::get_store_procedure()
{
    return _txn->get_store_procedure();
}

// This function issues a read or write request to a row specified by (table_id, index_id, key, type)
// Specifically, a RowRequest is constructed and inserted to the end of pending_local_requests or pending_remote_requests
RC
CCManager::row_request(uint32_t table_id, uint32_t index_id, uint64_t key, access_t type, bool reserve, uint32_t min_rows, uint32_t max_rows)
{
    // assert(type == RD || type == WR);

    RowRequest *req = new RowRequest;
    req->type = type;
    req->table_id = table_id;
    req->index_id = index_id;
    req->key = key;
    req->reserve = reserve;
    req->min_rows = min_rows;
    req->max_rows = max_rows;

    if (req->reserve) {
        // Reservation is only for records required by a remote coordinator
        assert(_txn->is_sub_txn());
        assert(GET_WORKLOAD->key_to_part(key, table_id) == g_part_id);
    }

    // For now, only partition leaders can be coordinators, read operations are issued by the coordinator
    // Hence, local partition requests lead to local requests, remote partition requests lead to remote requests
    if (GET_WORKLOAD->key_to_part(key, table_id) == g_part_id)
        pending_local_requests.push_back(req);
    else
        pending_remote_requests.push_back(req);

    return RCOK;
}

// This function issues an insert or delete request to an index entry specified by (row, table_id, type)
RC
CCManager::row_request(row_t * row, uint32_t table_id, access_t type)
{
    assert(type == INS || type == DEL);
    table_t * table = GET_WORKLOAD->get_table(table_id);
    if (type == INS) {
        InsertOp insert = {table, row};
        _inserts.push_back(insert);
    } else {
        _deletes.push_back(row);
    }
    set<INDEX *> indexes;
    table->get_indexes( &indexes );
    for (auto idx : indexes) {
        uint64_t idx_key = GET_WORKLOAD->get_index_key( row, idx->get_index_id() );
        IndexRequest * req = new IndexRequest;
        req->type = type;
        req->table_id = table_id;
        req->index_id = idx->get_index_id();
        req->key = idx_key;
        req->row = row;
        pending_local_requests.push_back(req);
    }
    return RCOK;
}

// This function constructs a result vector and passes it down to the below buffer_request function
char *
CCManager::buffer_request(uint32_t table_id, uint32_t index_id, uint64_t key, access_t type)
{
    vector<char *> results;
    RC rc = buffer_request(results, table_id, index_id, key, type, 1, 1);
    if (rc == LOCAL_MISS)
        return NULL;
    assert(results.size() == 1);
    return results[0];
}

// This function checks the local buffer for a row specified by (table_id, index_id, key, type)
RC
CCManager::buffer_request(vector<char *>& results, uint32_t table_id, uint32_t index_id, uint64_t key, access_t type, uint32_t min_rows, uint32_t max_rows) 
{
    assert(type != DEL || max_rows == 1);

    uint32_t num = 0;
    vector<Access *> accesses;
    INDEX * index = GET_WORKLOAD->get_index(index_id);
    table_t * table = GET_WORKLOAD->get_table(table_id); 
    for (auto & ac : ((CC_MAN*)this)->_access_set) {
        assert(ac.local_data != NULL);
        if (ac.table_id == table_id && row_t(ac.local_data, table).get_index_key(index) == key) {
            num += 1;
            accesses.push_back(&ac);
            if (num >= max_rows)
                break;
        }
    }

    if (num < max_rows) {
        for (auto & ac : ((CC_MAN*)this)->_remote_set) {
            assert(ac.local_data != NULL);
            if (ac.table_id == table_id && row_t(ac.local_data, table).get_index_key(index) == key) {
                num += 1;
                accesses.push_back(&ac);
                if (num >= max_rows)
                    break;
            }
        }
    }

    // assert(num >= min_rows);
    if (num < min_rows)
        return LOCAL_MISS;

    for (auto ac : accesses) {
        if (type == WR || type == RD) {
            assert(ac->local_data);
            results.push_back(ac->local_data);
            assert(ac->type != NA && ac->granted_access != NA);
            if (ac->granted_access == RD && type == WR)
                if (ac->type == RD)
                    ac->type = WR;
        } else if (type == DEL) {
            assert(ac->row);
            row_request(ac->row, table_id, DEL);
        } else {
            assert(false);
        }
    }
    return RCOK;
}

void
CCManager::clear_pending_requests(vector<RequestBase*>& requests)
{
    for (RequestBase * req : requests) {
        if (req->type == RD || req->type == WR)
            delete (RowRequest*) req;
        else if (req->type == PWR)
            delete (PermRequest*) req;
        else if (req->type == INS || req->type == DEL)
            delete (IndexRequest*) req;
    } 
    requests.clear();
}

RC
CCManager::process_requests()
{
    // 1. register remote requests
    // 2. send remote request msg
    // 3. batch access local read/write
    bool local_miss = false;
    if (!_waiting_requests && !pending_remote_requests.empty()) {
        // _waiting_requests == false && pending_remote_requests not empty
        // Take care of pending remote requests first
        // If current transcation requires remote requests, set local_miss to true
        local_miss = true;
        auto & remote_requests = get_store_procedure()->remote_requests;
        for (RequestBase * req_raw : pending_remote_requests) {
            // No remote index operations
            assert(req_raw->type == RD || req_raw->type == WR);
            RowRequest * req = (RowRequest *) req_raw;

            bool reserve = false;
        #if CC_ALG == SUNDIAL
            uint32_t remote_node_id;
            if (req->type == WR)
                remote_node_id = transport->get_partition_leader(GET_WORKLOAD->key_to_part(req->key, req->table_id));
            else
                remote_node_id = get_node_for_request(req->table_id, req->key, reserve);
        #elif CC_ALG == CALVIN
            // lock in local data center
            uint32_t remote_node_id = transport->get_closest_node_of_partition(GET_WORKLOAD->key_to_part(req->key, req->table_id));
        #else
            uint32_t remote_node_id = get_node_for_request(req->table_id, req->key, reserve);
        #endif
            _txn->_mp_txn = true;

            if (reserve) {
                // Reservation is only performed on leaders (OCC)
                assert(CC_ALG == DOCC);
                assert(remote_node_id == transport->get_partition_leader(GET_WORKLOAD->key_to_part(req->key, req->table_id)));
            }

            // Add the pending remote request to StoreProcedure->remote_requests
            if (remote_requests.find(remote_node_id) == remote_requests.end())
                remote_requests[remote_node_id] = UnstructuredBuffer();

            auto & remote_request = remote_requests[remote_node_id];
            remote_request.put( &req->type );
            remote_request.put( &req->table_id );
            remote_request.put( &req->index_id );
            remote_request.put( &req->key );
            remote_request.put( &reserve );
            uint32_t num_aborts = _txn->get_num_aborts();
            remote_request.put( &num_aborts );
        }

        // Send remote requests
        assert(_txn->_num_resp_expected == 0);
        _txn->_num_resp_expected = remote_requests.size();
        if (!remote_requests.empty()) {
            for (auto & remote_req : remote_requests) {
                uint32_t node = remote_req.first;
                // Prepend the transaction timestamp to remote requests
                add_remote_req_header( &remote_req.second );
                Message * msg = new Message(Message::REQ, node, _txn->get_txn_id(), remote_req.second.size(), remote_req.second.data());
                // These messages are asynchronous
                _txn->send_msg(msg);
                _txn->remote_parts_involved.insert(transport->get_node_partition(node));
            #if CC_ALG == CALVIN
                _txn->local_nodes_involved.insert(node);
            #endif
            }
            _txn->_mp_txn = true;
        }

        // Clear both pending_remote_requests and StoreProcedure->remote_requests
        clear_pending_requests(pending_remote_requests);
        remote_requests.clear();
    }

// #if CC_ALG == DOCC
//     // For OCC, handle local read requests after remote read requests
//     if (local_miss)
//         return LOCAL_MISS;
// #endif

    RC rc = RCOK;
    _waiting_requests = true;
    RequestBase * req_raw = NULL;
    // Take care of pending local requests
    for (; _curr_request_idx < pending_local_requests.size(); _curr_request_idx++) {
        req_raw = pending_local_requests[_curr_request_idx];
        access_t type = req_raw->type;
        if (type == INS) {
            IndexRequest * req = (IndexRequest *) req_raw;
            INDEX * index = GET_WORKLOAD->get_index(req->index_id);
            rc = index_insert(index, req->key);
            if (rc != RCOK) break;
        } else if (type == DEL) {
            IndexRequest * req = (IndexRequest *) req_raw;
            INDEX * index = GET_WORKLOAD->get_index(req->index_id);
            rc = index_delete(index, req->key);
            if (rc != RCOK) break;
        } else if (type == RD || type == WR) {
            RowRequest * req = (RowRequest *) req_raw;
            set<row_t *> * rows = NULL;
            // All rows need to be accessed via index_read for the first time
            rc = index_read(GET_WORKLOAD->get_index(req->index_id), req->key, rows, req->max_rows);
            if (rc != RCOK) break;
            if (rows == NULL) {
                assert(req->min_rows == 0);
                continue;
            }
            assert(rows->size() >= req->min_rows);
            uint32_t idx = 0;
            auto row_iter = rows->begin();
            for (; _curr_row_idx < req->max_rows && row_iter != rows->end(); row_iter++, idx++) {
                if (idx < _curr_row_idx)
                    continue;
            // #if ACCESS_METHOD == READ_RESERVE_LEADER || ACCESS_METHOD == HEURISTIC || ACCESS_METHOD == BONSPIEL || ACCESS_METHOD == HOTNESS_ONLY
            //     // MP txns always reserve local records
            //     if (_txn->is_mp_txn() && _txn->_type == TxnManager::MAN_COORD)
            //         req->reserve = true;
            // #endif
                rc = get_row(*row_iter, req->type, req->reserve);
                if (rc != RCOK) break;
                _curr_row_idx += 1;
            }
            if (rc != RCOK) break;
            _curr_row_idx = 0;
        } else {
            assert(false);
        }
    }

    if (rc == ABORT) {
        _waiting_requests = false;
        _curr_request_idx = 0;
        _curr_row_idx = 0;
        clear_pending_requests(pending_local_requests);
        return rc;
    } else if (rc == WAIT) {
        _txn->_lock_wait_start_time = get_sys_clock();
    } else if (rc == RCOK) {
        _waiting_requests = false;
        _curr_request_idx = 0;
        _curr_row_idx = 0;
        clear_pending_requests(pending_local_requests);
    }

    // If there are remote requests, return LOCAL_MISS for the transaction to wait
    if (local_miss) {
        rc = LOCAL_MISS;
    }

    return rc;
}

void
CCManager::add_remote_node_info(uint32_t node_id, bool is_write)
{
    if (_remote_node_info.find(node_id) == _remote_node_info.end()) {
        RemoteNodeInfo info;
        info.node_id = node_id;
        _remote_node_info.insert(pair<uint32_t, RemoteNodeInfo>(node_id, info));
    }
    _remote_node_info[node_id].has_write |= is_write;
}

void
CCManager::get_remote_nodes_with_writes(set<uint32_t> * nodes)
{
    assert(nodes->empty());
    for (map<uint32_t, RemoteNodeInfo>::iterator it = _remote_node_info.begin();
        it != _remote_node_info.end(); it ++)
    {
        if (it->second.has_write)
            nodes->insert(it->first);
    }
}

RC
CCManager::commit_insdel()
{
    // Implemented in concrete CC algorithms
    assert(false);
    return RCOK;
}

row_t * 
CCManager::raw_primary_index_read(uint32_t table_id, uint64_t primary_key)
{
    INDEX * index = GET_WORKLOAD->get_primary_index(table_id);
    ROW_MAN * manager = index->index_get_manager(primary_key);
    manager->latch();
    set<row_t *> *rows = index->read(primary_key);
    if (rows == NULL || rows->size() == 0) {
        manager->unlatch();
        return NULL;
    }
    // assert(rows->size() == 1); // primary index
    row_t * ret = *rows->begin();
    manager->unlatch();
    return ret;
}

LogRecord *
CCManager::get_test_log_record()
{
    LogRecord * record = (LogRecord *) new char[128];
    memset(record, 0, 128);
    record->size = 128;
    record->type = LOG_LOCAL_COMMIT;
    record->txn_id = 0;
    return record;
}
