#include <algorithm>
#include "docc_manager.h"
#include "row.h"
#include "row_docc.h"
#include "txn.h"
#include "manager.h"
#include "workload.h"
#include "index_btree.h"
#include "index_hash.h"
#include "table.h"
#include "store_procedure.h"
#include "catalog.h"
#include "packetize.h"
#include "message.h"
#include "packetize.h"
#include "query.h"
#include "raft_thread.h"
#include "transport.h"

#if CC_ALG == DOCC

DoccManager::DoccManager(TxnManager * txn, uint64_t ts)
    : CCManager(txn, ts)
{
    _is_read_only = true;
    _min_commit_ts = 0;
    _num_lock_waits = 0;
    _state = CC_EXECUTING;
    _lock_acquire_idx = 0;
    _signal_abort = false;
}

void
DoccManager::init()
{
    CCManager::init();
}

// This function takes care of local accesses (install data, release lock)
void
DoccManager::cleanup(RC rc)
{
    for (AccessDocc & access : _access_set) {
    #if CC_ALG == DOCC && EARLY_WRITE_INSTALL
        assert(COMMIT_PROTOCOL == RAFT_2PC);
        // Write sets of local txns are installed in install_local_write_set before writing logs
        if (_txn->is_mp_txn()) {
            if (access.type == WR && rc == COMMIT && !_txn->is_applier()) {
                // Install writes of _txn
                assert(access.local_data != NULL);
                access.row->manager->write(_txn, access.local_data, _min_commit_ts);
            }
        }
    #else
        if (access.type == WR && rc == COMMIT && !_txn->is_applier()) {
            // Install writes of _txn
            assert(access.local_data != NULL);
            access.row->manager->write(_txn, access.local_data, _min_commit_ts);
        }
    #endif
        assert(access.type != NA);
        // Release write locks held by _txn
        if (access.type != RD)
            access.row->manager->release(_txn, rc);
        if (access.local_data)
            delete[] access.local_data;
        access.local_data = NULL;
    }

    // Clear reservations (and update hotness) after lock release to reduce lock-holding time
    for (AccessDocc & access : _access_set) {
        if (access.reserved)
            if (access.type == RD || rc == ABORT)
                access.row->manager->unreserve(_txn, access.wts);
    #if HOTNESS_ENABLE
        // Update hotness (update counter) on leaders
        if (access.type == WR && rc == COMMIT && !_txn->is_applier()) {
            uint32_t table_id = access.table_id;
            uint64_t key = access.primary_key;
            assert(GET_WORKLOAD->key_to_part(key, table_id) == g_part_id);
        #if ACCESS_METHOD == HISTOGRAM_3D
            glob_manager->update_hotness(table_id, key, _txn->get_reserve_priority());
        #else
            glob_manager->update_hotness(table_id, key);
        #endif
        }
    #endif
    }

    for (AccessDocc & access : _remote_set) {
        if (access.local_data)
            delete[] access.local_data;
        access.local_data = NULL;
    }

    // Release writes locks on index held by _txn
    for (IndexAccessDocc & access : _index_access_set) {
        assert(access.type != NA);
        if (access.type != RD)
            access.manager->release(_txn, rc);
        if (access.rows)
            delete access.rows;
    }

    if (rc == ABORT)
        for (auto & ins : _inserts)
            delete ins.row;

    _state = CC_FINISHED;
    _access_set.clear();
    _remote_set.clear();
    _index_access_set.clear();
    _index_remote_set.clear();
    _inserts.clear();
    _deletes.clear();
}

void
DoccManager::release_all_locks()
{
    assert(_num_lock_waits == 0);
    for (auto &access : _index_access_set) {
        access.manager->release(_txn, RCOK);
        access.granted_access = RD;
    }

    for (auto &access : _access_set) {
        access.row->manager->release(_txn, RCOK);
        access.granted_access = RD;
    }
}

RC
DoccManager::force_write_set()
{
    // pretend I'm an applier
    _timestamp = 0;
    assert(_txn->is_sub_txn());
    _txn->set_type(TxnManager::MAN_APPLY);
    
    for (; _curr_request_idx < _index_access_set.size(); _curr_request_idx++) {
        IndexAccessDocc& access = _index_access_set[_curr_request_idx];
        assert(access.type == RD);
        access.manager->update_rts(access.wts, _min_commit_ts);
        // todo : handle the case when update_rts fails (not handling it won't affect consistency)
    }
    for (; _lock_acquire_idx < _access_set.size(); _lock_acquire_idx++) {
        AccessDocc& access = _access_set[_lock_acquire_idx];
        if (access.type == RD) {
            access.row->manager->update_rts(access.wts, _min_commit_ts);
            // todo : handle the case when update_rts fails
        } else if (access.type == WR) {
            RC rc = access.row->manager->lock(_txn);
            assert(rc == RCOK || rc == WAIT || rc == FINISH);
            if (rc == FINISH)
                continue;
            if (rc == WAIT) {
                _txn->set_type(TxnManager::MAN_PART);
                return WAIT;
            } else {
                access.row->manager->update_data(_min_commit_ts, access.local_data);
                access.row->manager->release(_txn, RCOK);
            }
        } else {
            assert(false);
        }
    }
    cleanup(ABORT);
    _txn->set_type(TxnManager::MAN_PART);
    return COMMIT;
}

// This function upgrades the permission on raw_access->row of _txn to type
RC
DoccManager::get_row_permission(Access * raw_access, access_t type)
{
    RC rc = RCOK;
    assert(type == RD || type == WR);
    AccessDocc * access = (AccessDocc*) raw_access;

    if (type == WR)
        _is_read_only = false;

    if (_state == CC_EXECUTING) {
        // In DOCC, transactions only acquire read access during execution
        update_granted_access(access, RD);
        return RCOK;
    }

    if (type == WR && access->granted_access != WR) {
        // For validation, take the write lock and upgrade permission to write
        rc = access->row->manager->lock(_txn);
        if (rc == RCOK)
            update_granted_access(access, type);
    }

    return rc;
}

// This function is called by executing transactions to access a row with type
// This function calls get_row_access and get_row_permission
RC
DoccManager::get_row(row_t * row, access_t type, bool reserve)
{
    RC rc = RCOK;
    // This function should only be called by transactions in the execution phase
    assert(_txn->get_txn_state() == TxnManager::RUNNING);
    assert(_state == CC_EXECUTING);

    // Search for the row in local _access_set
    AccessDocc * access = get_row_access(type, row->get_table()->get_table_id(), row->get_primary_key(), _access_set);
    // access->row points to the original row in DB
    access->row = row;
    // only get read permission during execution
    rc = get_row_permission(access, RD);

    if (rc != RCOK)
        return rc;

    // access->local_data points to the copied row in local workspace
    // Following code snippet is executed when the row is accessed for the first time
    if (access->local_data == NULL) {
        access->data_size = row->get_tuple_size();
        access->local_data = new char[access->data_size];
        // Copy row->data into access->local_data, together with rts and wts
        if (reserve) {
            access->reserved = true;
            assert(type == RD || type == WR);
            rc = row->manager->reserve_and_read(_txn, access->local_data, access->wts, access->rts, type);
            if (rc == WAIT) {
                delete [] access->local_data;
                access->local_data = NULL;
            }
        } else {
            access->reserved = false;
            rc = row->manager->read(_txn, access->local_data, access->wts, access->rts);
        }
    }

#if ABORT_AFTER_WRITE_RESERVATION_FAIL
    assert(rc == RCOK || rc == WAIT || rc == ABORT);
#else
    assert(rc == RCOK || rc == WAIT);
#endif
    return rc;
}

// This function upgrades the permission on raw_access->row of _txn to type
RC
DoccManager::get_index_permission(IndexAccess* raw_access, access_t type)
{
    RC rc = RCOK;
    assert(type == RD || type == WR);
    IndexAccessDocc * access = (IndexAccessDocc*) raw_access;

    // No index locks are taken during execution
    if (_state == CC_EXECUTING)
        return RCOK;

    if (type == WR)
        _is_read_only = false;

    if (type == WR && access->granted_access != WR)
        rc = access->manager->lock(_txn);

    if (rc == RCOK)
        update_granted_access(access, type);

    return rc;
}

// This function accesses rows specified by index and key with type
// and inserts a new entry to set (an access list)
DoccManager::IndexAccessDocc *
DoccManager::get_index_access(access_t type, INDEX * index, uint64_t key, vector<IndexAccessDocc>& set)
{
    assert(type == RD || type == INS || type == DEL);
    // Search for the required row in set
    IndexAccessDocc * access = find_access(key, index->get_index_id(), set);

    if (!access) {
        // The required row is not accessed previously
        // Construct a new access and append it to set
        set.emplace_back();
        access = &set.back();
        access->key = key;
        access->index = index;
        access->type = type;
        access->rows = NULL;
        access->granted_access = NA;
        access->home_node_id = g_node_id;
        access->manager = index->index_get_manager(key);
    }

    _last_index_access = access;
    // Upgrade access type for previously accessed rows
    if (access->type == RD && (type == INS || type == DEL))
        access->type = type;

    return access;
}

// This function accesses a row specified by table id and primary key with type
// and inserts a new entry to set (an access list)
DoccManager::AccessDocc * 
DoccManager::get_row_access(access_t type, uint32_t table_id, uint64_t primary_key, vector<AccessDocc>& set)
{
    assert(type == RD || type == WR);
    // Search for the required row in set
    AccessDocc * access = find_access(primary_key, table_id, set);

    if (!access) {
        // The required row is not accessed previously
        // Construct a new access and append it to set
        set.emplace_back();
        access = &set.back();
        access->home_node_id = g_node_id;
        access->row = NULL;
        access->type = type;
        access->primary_key = primary_key;
        access->table_id = table_id;
        access->data_size = 0;
        access->local_data = NULL;
    }

    // Upgrade access type for previously accessed rows
    if (access->type == RD && type == WR)
        access->type = WR;
    _last_access = access;

    return access; 
}

// This function performs an index read on index specified by key, which (potentially)
// inserts a new entry in _index_access_set, the result is stored in rows
RC
DoccManager::index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit)
{
    uint64_t tt = get_sys_clock();
    IndexAccessDocc * access = get_index_access(RD, index, key, _index_access_set);
    RC rc = get_index_permission(access, RD);
    assert(rc == RCOK);
    if (access->rows == NULL) {
        access->manager->latch();
        rows = index->read(key);
        access->wts = access->manager->get_wts();
        access->rts = access->manager->get_rts();
        if (rows) {
            if (rows->size() > limit) {
                set<row_t *>::iterator it = rows->begin();
                advance(it, limit);
                access->rows = new set<row_t *>( rows->begin(), it );
            } else
                access->rows = new set<row_t *>( *rows );
        }
        access->manager->unlatch();
    }
    rows = _last_index_access->rows;

    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    return RCOK;
}

// This function inserts an insertion entry to _index_access_set
RC
DoccManager::index_insert(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    get_index_access(INS, index, key, _index_access_set);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    return RCOK;
}

// This function inserts a deletion entry to _index_access_set
RC
DoccManager::index_delete(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    get_index_access(DEL, index, key, _index_access_set);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    return RCOK;
}

// This function decides the remote node id to send read request
uint32_t
DoccManager::get_node_for_request(uint32_t table_id, uint64_t key, bool & reserve)
{
    uint32_t remote_part_id = GET_WORKLOAD->key_to_part(key, table_id);
    assert(remote_part_id != g_part_id);

    // For DOCC, there are five options: read-nearest, read-leader, read-reserve-leader, heuristic, dept
#if ACCESS_METHOD == READ_NEAREST
    reserve = false;
    uint32_t remote_node_id = transport->get_closest_node_of_partition(remote_part_id);
    INC_INT_STATS(nearest_access, 1);
#elif ACCESS_METHOD == READ_LEADER
    reserve = false;
    uint32_t remote_node_id = transport->get_partition_leader(remote_part_id);
    INC_INT_STATS(leader_access, 1);
#elif ACCESS_METHOD == READ_RESERVE_LEADER  
    reserve = true;
    uint32_t remote_node_id = transport->get_partition_leader(remote_part_id);
    INC_INT_STATS(leader_reserve, 1);
#elif ACCESS_METHOD == HEURISTIC
    // HEURISTIC only works on YCSB
    assert(WORKLOAD == YCSB);
    uint32_t remote_node_id;
    uint64_t row_id = key / g_num_parts;
    if ((double) row_id / g_synth_table_size <= g_heuristic_perc) {
        INC_INT_STATS(leader_reserve, 1);
        reserve = true;
        remote_node_id = transport->get_partition_leader(remote_part_id);
    } else {
        INC_INT_STATS(nearest_access, 1);
        reserve = false;
        remote_node_id = transport->get_closest_node_of_partition(remote_part_id);
    }
#elif ACCESS_METHOD == HOTNESS_ONLY
    uint32_t remote_node_id;
    if (glob_manager->is_hot(table_id, key)) {
        INC_INT_STATS(leader_reserve, 1);
        reserve = true;
        remote_node_id = transport->get_partition_leader(remote_part_id);
    } else {
        INC_INT_STATS(nearest_access, 1);
        reserve = false;
        remote_node_id = transport->get_closest_node_of_partition(remote_part_id);
    }
#elif ACCESS_METHOD == BONSPIEL
  #if BONSPIEL_NO_AMS
    assert(!BONSPIEL_NO_RESER);
    INC_INT_STATS(leader_reserve, 1);
    reserve = true;
    uint32_t remote_node_id = transport->get_partition_leader(remote_part_id);
  #endif

  #if BONSPIEL_NO_RESER
    assert(!BONSPIEL_NO_AMS);
    uint32_t remote_node_id;
    reserve = false;
    if (glob_manager->high_abort_risk(table_id, key)) {
        INC_INT_STATS(leader_access, 1);
        remote_node_id = transport->get_partition_leader(remote_part_id);
    } else {
        INC_INT_STATS(nearest_access, 1);
        remote_node_id = transport->get_closest_node_of_partition(remote_part_id);
    }
  #endif

  #if !BONSPIEL_NO_AMS && !BONSPIEL_NO_RESER
    uint32_t remote_node_id;
    if (glob_manager->high_abort_risk(table_id, key)) {
        INC_INT_STATS(leader_reserve, 1);
        reserve = true;
        remote_node_id = transport->get_partition_leader(remote_part_id);
    } else {
        INC_INT_STATS(nearest_access, 1);
        reserve = false;
        remote_node_id = transport->get_closest_node_of_partition(remote_part_id);
    }
  #endif
#elif ACCESS_METHOD == HISTOGRAM_3D
    uint32_t remote_node_id;
    double rtt_with_leader = ((double) transport->get_ping_latency(transport->get_partition_leader(remote_part_id))) / BILLION;

    // no reserve + read nearest
    double uf1 = glob_manager->get_update_frequency(table_id, key, 0);
    double af1 = uf1 * rtt_with_leader;
    double lat1 = (1 / (1 - af1)) * rtt_with_leader;

    // reserve + read leader
    double uf2 = glob_manager->get_update_frequency(table_id, key, _txn->get_reserve_priority());
    double af2 = uf2 * rtt_with_leader;
    double lat2 = (1 / (1 - af2)) * 2 * rtt_with_leader;

    if (lat1 >= lat2) {
        INC_INT_STATS(leader_reserve, 1);
        reserve = true;
        remote_node_id = transport->get_partition_leader(remote_part_id);
    } else {
        INC_INT_STATS(nearest_access, 1);
        reserve = false;
        remote_node_id = transport->get_closest_node_of_partition(remote_part_id);
    }
#else
    assert(false);
#endif

    return remote_node_id;
}

// This function accesses row with type and stores a local copy pointed by data
RC
DoccManager::get_row(row_t * row, access_t type, char * &data)
{
    uint64_t tt = get_sys_clock();
    RC rc = get_row(row, type, false);
    if (rc == RCOK)
        data = _last_access->local_data;
    INC_FLOAT_STATS(row, get_sys_clock() - tt);
    return rc;
}

// This function appends _timestamp to the beginning of remote requests
void
DoccManager::add_remote_req_header(UnstructuredBuffer * buffer)
{
    buffer->put_front( &_timestamp );
}

// This function parses headers of remote requests
// Specifically, this function sets _timestamp
uint32_t
DoccManager::process_remote_req_header(UnstructuredBuffer * buffer)
{
    buffer->get( &_timestamp );
#if TS_ALLOC == TS_HYBRID
    uint64_t sub_txn_clock = glob_manager->get_clock_from_ts(_timestamp);
    glob_manager->max_sub_txn_clock = max(sub_txn_clock, glob_manager->max_sub_txn_clock);
#endif
    return sizeof(_timestamp);
}

// This function constructs the respond for a remote transaction in buffer
// Specifically, buffer contains all local row accesses and index accesses
void
DoccManager::get_resp_data(UnstructuredBuffer &buffer)
{
    // construct the return message.
    // Format:
    //    | n | (key, table_id, wts, rts, tuple_size, data) * n
    uint32_t num_tuples = 0;
    uint32_t num_index_accesses = 0;
    buffer.put(&num_tuples); // placeholder
    buffer.put(&num_index_accesses); // placeholder

    for (AccessDocc & access : _access_set) {
        if (access.resp == RESPOND_NOTHING) continue;
        buffer.put( &access.type );
        buffer.put( &access.primary_key );
        buffer.put( &access.table_id );
        buffer.put( &access.wts );
        buffer.put( &access.rts );

        if (access.resp == RESPOND_META) {
            uint32_t data_size = 0;
            buffer.put( &data_size );
        } else {
            assert(access.data_size != 0);
            buffer.put( &access.data_size );
            buffer.put( access.local_data, access.data_size ); 
        }
        // No duplicate responds
        access.resp = RESPOND_NOTHING;
        num_tuples ++;
    }

    for (IndexAccessDocc & access : _index_access_set) {
        if (access.resp == RESPOND_NOTHING) continue;
        assert(access.type == RD); // do not support remote ins/del for now
        uint32_t index_id = access.index->get_index_id();
        buffer.put( &access.type );
        buffer.put( &access.key );
        buffer.put( &index_id );
        buffer.put( &access.wts );
        buffer.put( &access.rts );
        // No duplicate responds
        access.resp = RESPOND_NOTHING;
        num_index_accesses ++;
    }

    // Empty response is possible in TPCE
    // assert(num_tuples > 0);
    buffer.set(0, &num_tuples);
    buffer.set(sizeof(num_tuples), &num_index_accesses);
}

// This function parses remote responds, stores remote accesses in _remote_set and _index_remote_set
void
DoccManager::process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data)
{
    // return data format:
    //    | n | (key, table_id, wts, rts, tuple_size, data) * n
    // store the remote tuple to local remote_set.
    UnstructuredBuffer buffer(resp_data);
    uint32_t num_tuples;
    uint32_t num_index_accesses;
    buffer.get( &num_tuples );
    buffer.get( &num_index_accesses );
    // Empty response is possible in TPCE
    // assert(num_tuples > 0);
    
    for (uint32_t i = 0; i < num_tuples; i++) {
        access_t type;
        uint64_t primary_key;
        uint32_t table_id;
        uint32_t data_size;
        buffer.get( &type );
        buffer.get( &primary_key );
        buffer.get( &table_id );
        // Create an entry in _remote_set to store remote rows
        AccessDocc * access = get_row_access(type, table_id, primary_key, _remote_set);
        buffer.get( &access->wts );
        buffer.get( &access->rts );
        buffer.get( &data_size );
        access->granted_access = RD;
        access->home_node_id = node_id;
        assert(data_size < 65536);
        assert(data_size > 0);
        if (data_size != 0) {
            access->data_size = data_size;
            char * data = NULL;
            buffer.get( data, data_size );
            if (access->local_data == NULL)
                access->local_data = new char [data_size];
            memcpy(access->local_data, data, data_size);
        }
        // Only get read permission during execution
    }

    for (uint32_t i = 0; i < num_index_accesses; i++) {
        access_t type;
        uint64_t key;
        uint32_t index_id;
        buffer.get( &type );
        buffer.get( &key );
        buffer.get( &index_id );
        INDEX * index = GET_WORKLOAD->get_index(index_id);
        // Create an entry in _index_remote_set to store remote rows
        IndexAccessDocc * access = get_index_access(type, index, key, _index_remote_set);
        buffer.get( &access->wts );
        buffer.get( &access->rts );
        access->granted_access = RD;
        access->home_node_id = node_id;
    }
}

// TODO: handle index delete on appliers
// This function constructs Raft logs
LogRecord *
DoccManager::get_log_record(LogType type)
{
    UnstructuredBuffer buffer;
    if (type == LOG_PREPARE || type == LOG_LOCAL_COMMIT) {
        // index read:   | type (IDXRD) | index_id | key | wts
        // index insert: | type (INS)   | table_id | data_size | data
        // renew tuple:  | type (RD)    | table_id | key | wts
        // write tuple:  | type (WR)    | table_id | key | data_size | data
        buffer.put(&_min_commit_ts);
        // Construct header
        if (type == LOG_PREPARE) {
            uint32_t coord_part = _txn->get_coord_part();
            uint32_t num_remote_parts = _txn->get_remote_parts_involved().size();
            buffer.put(&coord_part);
            buffer.put(&num_remote_parts);
            for (uint32_t remote_part : _txn->get_remote_parts_involved()) 
                buffer.put(&remote_part);
        }

        // Index read set
        for (auto &ac : _index_access_set) {
            uint32_t index_id = ac.index->get_index_id();
            if (ac.type == IDXRD && _min_commit_ts >= ac.rts) {
                // Not in use
                buffer.put(&ac.type);
                buffer.put(&index_id);
                buffer.put(&ac.key);
                buffer.put(&ac.wts);
            }
        }

        // Index insert set
        for (auto &ins : _inserts) {
            access_t type = INS;
            uint32_t table_id = ins.table->get_table_id();
            uint32_t data_size = ins.row->get_tuple_size();
            buffer.put(&type);
            buffer.put(&table_id);
            buffer.put(&data_size);
            buffer.put(ins.row->data, data_size);
        }

        // Read and write sets
        for (auto &ac : _access_set) {
            if (ac.type == RD && _min_commit_ts >= ac.rts) {
                buffer.put(&ac.type);
                buffer.put(&ac.table_id);
                buffer.put(&ac.primary_key);
                buffer.put(&ac.wts);
            } else if (ac.type == WR) {
                buffer.put(&ac.type);
                buffer.put(&ac.table_id);
                buffer.put(&ac.primary_key);
                buffer.put(&ac.data_size);
                buffer.put(ac.local_data, ac.data_size);
            }
        }
    }

    LogRecord * record = (LogRecord *) new char[sizeof(LogRecord) + buffer.size()];
    record->size = sizeof(LogRecord) + buffer.size();
    record->type = type;
    record->txn_id = _txn->get_txn_id();
    if (buffer.size() > 0)
        memcpy(record->data, buffer.data(), buffer.size());
    return record;
}

// TODO: handle index delete on appliers
// This function applies Raft records and is executed only by appliers
RC
DoccManager::apply_log_record(LogRecord * log)
{
    // After execution, the coordinator sends out LOG_PREPARE or LOG_LOCAL_COMMIT
    // logs to appliers so that appliers can construct their read & write sets locally
    if (_state == CC_EXECUTING) {
        assert(log->type == LOG_PREPARE || log->type == LOG_LOCAL_COMMIT);
        UnstructuredBuffer buffer(log->data);
        uint32_t size = log->size - sizeof(LogRecord);
        buffer.get(&_min_commit_ts);
        if (log->type == LOG_PREPARE) {
            uint32_t coord_part;
            uint32_t num_remote_parts;
            buffer.get(&coord_part);
            _txn->set_coord_part(coord_part);
            buffer.get(&num_remote_parts);
            set<uint32_t>& remote_parts = _txn->get_remote_parts_involved();
            for (uint32_t i = 0; i < num_remote_parts; i++) {
                uint32_t part_id;
                buffer.get(&part_id);
                remote_parts.insert(part_id);
            }
        }
        while (buffer.get_offset() < size) {
            access_t type;
            buffer.get(&type);
            if (type == IDXRD) {
                // Not in use
                uint32_t index_id;
                uint64_t key;
                uint64_t wts;
                buffer.get(&index_id);
                buffer.get(&key);
                buffer.get(&wts);
                INDEX * index = GET_WORKLOAD->get_index(index_id);
                IndexAccessDocc * access = get_index_access(RD, index, key, _index_access_set);
                access->wts = wts;
            } else if (type == INS) {
                uint32_t table_id;
                uint32_t data_size;
                char * data;
                buffer.get(&table_id);
                buffer.get(&data_size);
                buffer.get(data, data_size);
                table_t * table = GET_WORKLOAD->get_table(table_id);
                row_t * row = new row_t(table);
                memcpy(row->data, data, row->get_tuple_size());
                InsertOp insert = {table, row};
                _inserts.push_back(insert);
                set<INDEX *> indexes;
                table->get_indexes( & indexes );
                for (auto idx : indexes) {
                    uint64_t idx_key = GET_WORKLOAD->get_index_key( row, idx->get_index_id() );
                    RC rc = index_insert(idx, idx_key);
                    assert(rc == RCOK);
                }
            } else if (type == RD || type == WR) {
                uint32_t table_id;
                uint64_t primary_key;
                buffer.get(&table_id);
                buffer.get(&primary_key);
                AccessDocc * access = get_row_access(type, table_id, primary_key, _access_set);
                if (access->row == NULL)
                    access->row = raw_primary_index_read(table_id, primary_key);
                if (type == RD) {
                    buffer.get(&access->wts);
                } else {
                    char * data;
                    buffer.get(&access->data_size);
                    buffer.get(data, access->data_size);
                    if (access->local_data == NULL)
                        access->local_data = new char[access->data_size];
                    memcpy(access->local_data, data, access->data_size);
                }
            } else {
                assert(false);
            }
            assert(buffer.get_offset() <= size);
        }
        assert(buffer.get_offset() == size);
        _state = CC_COMMITTING;
        // If this is a distributed transaction, the committing process cannot be executed now
        if (log->type == LOG_PREPARE)
            return RCOK;
    }

    // The actual committing process on appliers
    if (_state == CC_COMMITTING) {
        assert(log->type == LOG_LOCAL_COMMIT || log->type == LOG_DIST_COMMIT || log->type == LOG_ABORT);
        if (log->type == LOG_ABORT) {
            cleanup(ABORT);
            return ABORT;
        }

        // Sort _index_access_set beforehand to prevent deadlock
        sort(_index_access_set.begin(), _index_access_set.end(), [](const IndexAccessDocc & a, const IndexAccessDocc & b) {
            if (a.index->get_index_id() != b.index->get_index_id())
                return a.index->get_index_id() < b.index->get_index_id();
            else
                return a.key < b.key;
        });
        // Acquire locks on all indexes
        for (; _lock_acquire_idx < _index_access_set.size(); _lock_acquire_idx++) {
            IndexAccessDocc & access = _index_access_set[_lock_acquire_idx];
            if (access.type == RD) {
                access.manager->update_rts(access.wts, _min_commit_ts);
                // TODO: handle the case when update_rts fails (not handling it won't affect consistency)
            } else if (access.type == INS) {
                RC rc = access.manager->lock(_txn);
                // TODO: If the lock operation returns FINISH, some insert and update operations will be lost
                // This is not a very big deal
                if (rc != RCOK)
                    return rc;
                update_granted_access(&access, INS);
                // Update timestamps for index manager
                access.manager->set_ts(_min_commit_ts, _min_commit_ts);
            } else {
                assert(false);
            }
        }
        // Insert rows to indexes
        for (auto ins : _inserts) {
            row_t * row = ins.row;
            row->manager->set_ts(_min_commit_ts, _min_commit_ts);
            set<INDEX *> indexes;
            ins.table->get_indexes( &indexes );
            for (auto idx : indexes) {
                uint64_t key = row->get_index_key(idx);
                idx->insert(key, row);
            }
        }

        for (; _lock_acquire_idx < _index_access_set.size() + _access_set.size(); _lock_acquire_idx++) {
            AccessDocc & access = _access_set[_lock_acquire_idx - _index_access_set.size()];
            // The previous insert log might be lost so here the row might not exist
            // TODO: for the reason, refer to the above TODO tag
            if (access.row == NULL) {
                _access_set.erase(_access_set.begin() + _lock_acquire_idx - _index_access_set.size());
                _lock_acquire_idx--;
                continue;
            }

            if (access.type == RD) {
                access.row->manager->update_rts(access.wts, _min_commit_ts);
                // TODO: handle the case when update_rts fails
            } else if (access.type == WR) {
                RC rc = access.row->manager->lock(_txn);
                assert(rc == RCOK || rc == WAIT || rc == FINISH);
                if (rc == FINISH)
                    continue;
                if (rc == WAIT)
                    return WAIT;
                else {
                    access.row->manager->update_data(_min_commit_ts, access.local_data);
                    // Release the lock immediately after updating the data to prevent deadlock
                    access.row->manager->release(_txn, RCOK);
                }
            } else {
                assert(false);
            }
        }
        cleanup(COMMIT);
        return COMMIT;
    }

    return RCOK;
}

// This function searches in set for the access with specified primary key and table id
DoccManager::AccessDocc *
DoccManager::find_access(uint64_t primary_key, uint32_t table_id, vector<AccessDocc>& set)
{
    for (auto & ac : set) {
        if (ac.primary_key == primary_key && ac.table_id == table_id)
            return &ac;
    }
    return NULL;
}

// This function searches in set for the index access with specified key and index id
DoccManager::IndexAccessDocc *
DoccManager::find_access(uint64_t key, uint32_t index_id, vector<IndexAccessDocc>& set)
{
    for (auto & ac : set) {
        if (ac.key == key && ac.index->get_index_id() == index_id)
            return &ac;
    }
    return NULL;
}

// This function acquires locks on local write set records
RC
DoccManager::lock_write_set()
{
    RC rc = RCOK;
    for (; _lock_acquire_idx < _index_access_set.size(); _lock_acquire_idx++) {
        IndexAccessDocc & access = _index_access_set[_lock_acquire_idx];
        if (access.type == INS || access.type == DEL) {
            rc = get_index_permission(&access, WR);
            if (rc != RCOK)
                return rc;
        }
    }

    for (; _lock_acquire_idx < _index_access_set.size() + _access_set.size(); _lock_acquire_idx++) {
        AccessDocc & access = _access_set[_lock_acquire_idx - _index_access_set.size()];
        if (access.type == WR) {
            rc = get_row_permission(&access, WR);
            if (rc != RCOK)
                return rc;
        }
    }

    return RCOK;
}

// This function computes the minimum commit timestamp based on the TicToc algorithm
// and stores it in _min_commit_ts
void
DoccManager::compute_commit_ts()
{
    for (auto & access : _access_set) {
        if (access.type == WR) // in write set
            _min_commit_ts = max(access.row->manager->get_rts() + 1, _min_commit_ts);
        if (access.local_data != NULL) // in read set
            _min_commit_ts = max(access.wts + 1, _min_commit_ts);
    }

    for (auto & access : _remote_set) {
        if (access.type == WR)
            _min_commit_ts = max(access.rts + 1, _min_commit_ts);
        if (access.local_data != NULL)
            _min_commit_ts = max(access.wts + 1, _min_commit_ts);
    }

    for (auto access : _index_access_set) {
        if (access.type == INS || access.type == DEL)
            _min_commit_ts = max(access.manager->get_rts() + 1, _min_commit_ts);
        if (access.rows != NULL)
            _min_commit_ts = max(access.wts + 1, _min_commit_ts);
    }

    for (auto access : _index_remote_set) {
        assert(access.type != INS && access.type != DEL); // do not support remote ins/del for now
        _min_commit_ts = max(access.wts + 1, _min_commit_ts);
    }

    _min_commit_ts = max(_min_commit_ts, glob_manager->get_ts(GET_THD_ID));
}

// This function validates the read set of a transaction
RC
DoccManager::validate()
{
    // TODO: Using try_renew here is problematic
    for (auto & access : _index_access_set) {
        if (access.type == RD) {
            if (_min_commit_ts >= access.rts && !access.manager->try_renew(access.wts, _min_commit_ts, _txn))
                return ABORT;
        } else if (access.rows != NULL && access.wts != access.manager->get_wts()) {
            return ABORT;
        }
    }

    for (auto & access : _access_set) {
        if (access.type == RD) {
            if (_min_commit_ts >= access.rts && !access.row->manager->try_renew(access.wts, _min_commit_ts, _txn)) {
                // rts of the record (in read-set) cannot be renewed
                // two possible reasons: 1) the record has been updated; 2) the record is locked right now
                if (_txn->get_coord_part() != g_part_id) {
                    INC_INT_STATS(remote_txn_val_rd_abort, 1);
                }
                return ABORT;
            }
        } else if (access.local_data != NULL && access.wts != access.row->manager->get_wts()) {
            // the record (in write-set) has been updated
            if (_txn->get_coord_part() != g_part_id) {
                INC_INT_STATS(remote_txn_val_wr_abort, 1);
            }
            return ABORT;
        }
    }

    return RCOK;
}

// This function commits or aborts local transactions
RC
DoccManager::process_prepare_phase_coord()
{
    RC rc = RCOK;
    // 1. compute commit ts
    // 2. sort the write set
    // 3. lock the write set
    // 4. validate local txn
    assert(_state == CC_EXECUTING || _state == CC_PREPARING);
    if (_state == CC_EXECUTING) {
        _state = CC_PREPARING;
        compute_commit_ts();
    }

    rc = lock_write_set();
    if (rc != RCOK)
        return rc;

    rc = validate();

    // reservation becomes useless after validation
    for (AccessDocc & access : _access_set)
        if (access.reserved)
            access.row->manager->unreserve(_txn, access.wts);

    if (rc == RCOK)
        _state = CC_PREPARED;
    return rc;
}

// This function checks whether the coordinator needs to send prepare messages to part_id
// The result is always yes, this functions constructs the prepare message in buffer
bool
DoccManager::need_prepare_req(uint32_t part_id, UnstructuredBuffer & buffer)
{
#if COMMIT_PROTOCOL == RAFT_2PC
    assert(part_id != g_part_id);
#endif

    uint32_t num_accesses = 0;
    uint32_t num_index_accesses = 0;
    set<uint32_t>& remote_parts = _txn->get_remote_parts_involved();
    uint32_t num_parts = remote_parts.size() + 1;

    // since we may not read from leader, the prepare message should contain the read set as well
    // Format
    // | commit_ts | timestamp | num_access | num_index_access | num_parts | part_id * K | (type, table_id, key, wts, rts, size, data) * N 
    buffer.put( &_min_commit_ts );
    buffer.put( &_timestamp );
    buffer.put( &num_accesses ); // placeholder
    buffer.put( &num_index_accesses ); // placeholder
    buffer.put( &num_parts );
    buffer.put( &g_part_id );
    for (uint32_t part_id : remote_parts) 
        buffer.put( &part_id );

#if COMMIT_PROTOCOL == TAPIR || COMMIT_PROTOCOL == GPAC
    // Put local accesses in the prepare message
    if (part_id == g_part_id) {
        for (auto & ac : _access_set) {
            assert(ac.type == RD || ac.type == WR);
            if (ac.type == RD && _min_commit_ts < ac.rts)
                continue;

            buffer.put(&ac.type);
            buffer.put(&ac.table_id);
            buffer.put(&ac.primary_key);
            buffer.put(&ac.wts);
            buffer.put(&ac.rts);
            if (ac.type == WR) {
                assert(ac.data_size > 0);
                buffer.put(&ac.data_size);
                buffer.put(ac.local_data, ac.data_size);
            }
            num_accesses += 1;
        }

        for (auto & ac : _index_access_set) {
            // TODO: support remote ins/del
            if (ac.type != RD)
                continue;
            if (ac.type == RD && _min_commit_ts < ac.rts)
                continue;

            uint32_t index_id = ac.index->get_index_id();
            buffer.put(&ac.type);
            buffer.put(&index_id);
            buffer.put(&ac.key);
            buffer.put(&ac.wts);
            buffer.put(&ac.rts);
            num_index_accesses += 1;
        }
    } else {
#endif
        // Put all remote accesses at part_id in the prepare message
        for (auto & ac : _remote_set) {
            if (transport->get_node_partition(ac.home_node_id) == part_id) {
                assert(ac.type == RD || ac.type == WR);
                if (ac.type == RD && _min_commit_ts < ac.rts)
                    continue;

                buffer.put(&ac.type);
                buffer.put(&ac.table_id);
                buffer.put(&ac.primary_key);
                buffer.put(&ac.wts);
                buffer.put(&ac.rts);
                if (ac.type == WR) {
                    assert(ac.data_size > 0);
                    buffer.put(&ac.data_size);
                    buffer.put(ac.local_data, ac.data_size);
                }
                num_accesses += 1;
            }
        }

        // Put all remote index accesses at part_id in the prepare message
        for (auto & ac : _index_remote_set) {
            if (transport->get_node_partition(ac.home_node_id) == part_id) {
                assert(ac.type == RD); // TODO: support remote ins/del
                if (ac.type == RD && _min_commit_ts < ac.rts)
                    continue;

                uint32_t index_id = ac.index->get_index_id();
                buffer.put(&ac.type);
                buffer.put(&index_id);
                buffer.put(&ac.key);
                buffer.put(&ac.wts);
                buffer.put(&ac.rts);
                num_index_accesses += 1;
            }
        }
#if COMMIT_PROTOCOL == TAPIR || COMMIT_PROTOCOL == GPAC
    }
#endif

    buffer.set(sizeof(_min_commit_ts) + sizeof(_timestamp), &num_accesses);
    buffer.set(sizeof(_min_commit_ts) + sizeof(_timestamp) + sizeof(num_accesses), &num_index_accesses);
    return true;
}

// This function parses 2PC prepare messages and validate accordingly
// This function is executed on participants
RC
DoccManager::process_prepare_req(uint32_t size, char * data, UnstructuredBuffer &buffer)
{
    RC rc = RCOK;

    if (_state == CC_EXECUTING) {
        _state = CC_PREPARING;

        // Format:
        // | commit_ts | timestamp | num_access | num_index_access | num_parts | part_id * K | (type, table_id, key, wts, rts, size, data) * N 

        UnstructuredBuffer buffer(data);
        uint32_t num_accesses;
        uint32_t num_index_accesses;
        uint32_t num_parts;
        set<uint32_t>& remote_parts = _txn->get_remote_parts_involved();
        buffer.get( &_min_commit_ts );
        buffer.get( &_timestamp );
        buffer.get( &num_accesses );
        buffer.get( &num_index_accesses );
        buffer.get( &num_parts );
        assert(num_parts >= 1);
        for (uint32_t i = 0; i < num_parts; i++) {
            uint32_t part_id;
            buffer.get( &part_id );
            if (i == 0)
                _txn->set_coord_part(part_id);
            if (part_id != g_part_id)
                remote_parts.insert(part_id);
        }
        for (uint32_t i = 0; i < num_accesses; i++) {
            access_t type;
            uint32_t table_id;
            uint64_t primary_key;

            buffer.get( &type );
            buffer.get( &table_id );
            buffer.get( &primary_key );
            AccessDocc * access = get_row_access(type, table_id, primary_key, _access_set);
            buffer.get( &access->wts );
            buffer.get( &access->rts );

            if (access->row == NULL)
                access->row = raw_primary_index_read(table_id, primary_key);

            if (type == WR) {
                // On remote nodes, access->data points to the original row
                // access->local_data points to modified version in the write set
                char * data;
                buffer.get(&access->data_size);
                buffer.get(data, access->data_size);
                if (access->local_data == NULL)
                    access->local_data = new char[access->data_size];
                memcpy(access->local_data, data, access->data_size);
            }
        }
        for (uint32_t i = 0; i < num_index_accesses; i++) {
            access_t type;
            uint32_t index_id;
            uint64_t key;
            buffer.get( &type );
            buffer.get( &index_id );
            buffer.get( &key );
            INDEX * index = GET_WORKLOAD->get_index(index_id);
            IndexAccessDocc * access = get_index_access(type, index, key, _index_access_set);
            buffer.get( &access->wts );
            buffer.get( &access->rts );
        }
    }

    // Local validation on participants
    rc = lock_write_set();
    if (rc == ABORT) {
        INC_INT_STATS(remote_txn_lock_write_set_abort, 1);
    }
    if (rc != RCOK)
        return rc;

    rc = validate();

    // reservation becomes useless after validation
    for (AccessDocc & access : _access_set)
        if (access.reserved)
            access.row->manager->unreserve(_txn, access.wts);

    if (rc == ABORT) {
        INC_INT_STATS(remote_txn_validation_abort, 1);
        cleanup(ABORT);
        return ABORT;
    } else {
        _state = CC_PREPARED;
        if (is_read_only()) {
            if (_txn->get_coord_part() == g_part_id) {
                assert(COMMIT_PROTOCOL == TAPIR || COMMIT_PROTOCOL == GPAC);
                return RCOK;
            }
            cleanup(COMMIT);
            return COMMIT;
        } else
            return rc;
    }
}

void
DoccManager::process_prepare_resp(RC rc, uint32_t node_id, char * data)
{
    assert(data == NULL);
}

#if EARLY_WRITE_INSTALL
// This function installs the local write set
void
DoccManager::install_local_write_set()
{
    for (AccessDocc & access : _access_set) {
        if (access.type == WR && !_txn->is_applier()) {
            // Install local write set
            assert(access.local_data != NULL);
            access.row->manager->write(_txn, access.local_data, _min_commit_ts);
            // Set the fresh flag after early install write set
            // Do not need to latch since this is protected by lock
            access.row->manager->_fresh_flag = true;
            access.row->manager->notify_reserve_waiting_set();
        }
        assert(access.type != NA);
        // Do not release locks
    }
}
#endif

// This function commits or aborts a transaction, and is executed only on the coordinator
void
DoccManager::process_commit_phase_coord(RC rc)
{
    if (rc == COMMIT) {
        // Commit index insertions & deletions
        commit_insdel();
        // Commit write sets
        cleanup(COMMIT);
    } else {
        // Abort write sets
        cleanup(ABORT);
    }
}

// This function commits all index-related operations
RC
DoccManager::commit_insdel() {
    for (auto ins : _inserts) {
        row_t * row = ins.row;
        row->manager->set_ts(_min_commit_ts, _min_commit_ts);
        set<INDEX *> indexes;
        ins.table->get_indexes( &indexes );
        for (auto idx : indexes) {
            uint64_t key = row->get_index_key(idx);
            idx->insert(key, row);
        }
    }
    // handle deletes
    for (auto row : _deletes) {
        set<INDEX *> indexes;
        row->get_table()->get_indexes( &indexes );
        for (auto idx : indexes)
            idx->remove( row );
        for (auto it = _access_set.begin(); it != _access_set.end(); it++) {
            if (it->row == row) {
                _access_set.erase(it);
                break;
            }
        }
        row->manager->delete_row( _min_commit_ts );
        // TODO. not deleting the row here. because another txn might be accessing it.
        // A better solution is to handle deletion in a special way. For example, access the
        // index again to see if the tuple still exists.
        // delete row;
    }
    for (auto access : _index_access_set)
        if (access.type != RD) {
            M_ASSERT(access.manager->_lock_owner == _txn, "lock_owner=%#lx, _txn=%#lx",
                (uint64_t)access.manager->_lock_owner, (uint64_t)_txn);
            access.manager->set_ts(_min_commit_ts, _min_commit_ts);
        }

    return RCOK;
}

bool
DoccManager::need_commit_req(RC rc, uint32_t part_id, UnstructuredBuffer &buffer)
{
    return true;
}

void
DoccManager::process_commit_req(RC rc, uint32_t size, char * data)
{
    cleanup(rc);
}

bool
DoccManager::is_txn_ready()
{
    return _num_lock_waits == 0 || _signal_abort;
}

bool
DoccManager::set_txn_ready(RC rc)
{
    if (rc == RCOK)
        return ATOM_SUB_FETCH(_num_lock_waits, 1) == 0;
    else
        return ATOM_CAS(_signal_abort, false, true);
}

#endif
