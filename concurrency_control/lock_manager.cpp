#include "lock_manager.h"
#include "row.h"
#include "row_lock.h"
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

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == WOUND_WAIT || CC_ALG == CALVIN

LockManager::LockManager(TxnManager * txn, uint64_t ts)
    : CCManager(txn, ts)
{
#if CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT
    assert(g_ts_alloc == TS_CLOCK || g_ts_alloc == TS_HYBRID);
    _timestamp = glob_manager->get_ts(GET_THD_ID);
#endif
#if CC_ALG == WOUND_WAIT
    _wounded = false;
#endif
#if WORKLOAD == TPCC
    _access_set.reserve(128);
#endif
    _is_read_only = true;
    _num_lock_waits = 0;
    _signal_abort = false;
    _lock_acquire_idx = 0;
    _state = CC_EXECUTING;
}

void
LockManager::init()
{
    CCManager::init();
}

// In 2PL, no X-locks are taken during the execution phase, writes are buffered locally
// This function is executed only to access local rows
RC
LockManager::get_row_permission(Access * access, access_t type)
{
#if CC_ALG == CALVIN
    // In Calvin, take both read and write locks
    RC rc = RCOK;
    assert(type == RD || type == WR);
    if (type == WR)
        _is_read_only = false;

    Row_lock::LockType lock_type = (type == RD ? Row_lock::LOCK_SH : Row_lock::LOCK_EX);
    rc = access->row->manager->lock_get(lock_type, _txn);
    assert(rc == RCOK);
    update_granted_access(access, type);

    return rc;
#else
    RC rc = RCOK;
    assert(type == RD || type == WR);

    if (type == WR)
        _is_read_only = false;

    // During execution, take SH-locks for read records, buffer write records
    if (_state == CC_EXECUTING) {
        if (type == RD && access->granted_access < RD) {
            Row_lock::LockType lock_type = Row_lock::LOCK_SH;
            rc = access->row->manager->lock_get(lock_type, _txn);
            if (rc != RCOK)
                return rc;
        }
        update_granted_access(access, RD);
        return rc;
    }

    // During validation phase, take EX-locks for write records
    if (_state == CC_PREPARING) {
        if (type == WR && access->granted_access < WR) {
            Row_lock::LockType lock_type = Row_lock::LOCK_EX;
            rc = access->row->manager->lock_get(lock_type, _txn);
            if (rc != RCOK)
                return rc;
        }
        update_granted_access(access, type);
        return rc;
    }

    assert(false);
    return RCOK;
#endif
}

RC
LockManager::get_row(row_t * row, access_t type, bool reserve)
{
    RC rc = RCOK;
    assert(_txn->get_txn_state() == TxnManager::RUNNING);
    assert(_state == CC_EXECUTING);

    // Search for the row in local _access_set
    Access * access = get_row_access(type, row->get_table()->get_table_id(), row->get_primary_key(), _access_set);
    access->row = row;

    rc = get_row_permission(access, type);

    if (rc != RCOK)
        return rc;

    // The row is not accessed before, make a local copy
    if (access->local_data == NULL) {
        access->data_size = row->get_tuple_size();
        access->local_data = new char[access->data_size];
        memcpy(access->local_data, row->data, access->data_size);
    }

    return RCOK;
}

RC
LockManager::get_row(row_t * row, access_t type, char * &data)
{
    uint64_t tt = get_sys_clock();
    RC rc = get_row(row, type, false);
    if (rc == RCOK)
        data = _last_access->local_data;
    INC_FLOAT_STATS(row, get_sys_clock() - tt);
    return rc;
}

RC
LockManager::get_index_permission(IndexAccess * access, access_t type)
{
    RC rc = RCOK;
    assert(type == RD || type == WR);

    if (type == WR)
        _is_read_only = false;

    // During execution, take SH-locks for read records, buffer write records
    if (_state == CC_EXECUTING) {
        if (type == RD && access->granted_access < RD) {
            Row_lock::LockType lock_type = Row_lock::LOCK_SH;
            rc = access->manager->lock_get(lock_type, _txn);
            if (rc != RCOK)
                return rc;
        }
        update_granted_access(access, RD);
        return rc;
    }

    // During commiting, take EX-locks for write records
    if (type == WR && access->granted_access < WR) {
        Row_lock::LockType lock_type = Row_lock::LOCK_EX;
        rc = access->manager->lock_get(lock_type, _txn);
        if (rc != RCOK)
            return rc;
    }

    update_granted_access(access, type);
    return rc;
}

RC
LockManager::index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit)
{
    uint64_t tt = get_sys_clock();
    IndexAccess * access = get_index_access(RD, index, key, _index_access_set);
    RC rc = get_index_permission(access, RD);
    if (rc != RCOK)
        return rc;

    if (access->rows == NULL) {
        access->manager->latch();
        rows = index->read(key);
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

RC
LockManager::index_insert(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    get_index_access(INS, index, key, _index_access_set);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    return RCOK;
}

RC
LockManager::index_delete(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    get_index_access(DEL, index, key, _index_access_set);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    return RCOK;
}

// This function decides the remote node id to send read request
uint32_t
LockManager::get_node_for_request(uint32_t table_id, uint64_t key, bool & reserve)
{
    uint32_t remote_part_id = GET_WORKLOAD->key_to_part(key, table_id);
    assert(remote_part_id != g_part_id);

    // For 2PL, there is only one option: read-leader
    uint32_t remote_node_id = transport->get_partition_leader(remote_part_id);

    return remote_node_id;
}

void
LockManager::add_remote_req_header(UnstructuredBuffer * buffer)
{
#if CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT
    buffer->put_front( &_timestamp );
#endif
}

uint32_t
LockManager::process_remote_req_header(UnstructuredBuffer * buffer)
{
#if CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT
    buffer->get( &_timestamp );
#if TS_ALLOC == TS_HYBRID
    uint64_t sub_txn_clock = glob_manager->get_clock_from_ts(_timestamp);
    glob_manager->max_sub_txn_clock = max(sub_txn_clock, glob_manager->max_sub_txn_clock);
#endif
    return sizeof(_timestamp);
#else
    return 0;
#endif
}

// For 2PL, no need to send index accesses
void
LockManager::get_resp_data(UnstructuredBuffer &buffer)
{
    // construct the return message.
    // Format:
    //    | n | (key, table_id, tuple_size, data) * n
    uint32_t num_tuples = 0;
    buffer.put( &num_tuples ); //placeholder
    
    for (Access & access : _access_set) {
        if (access.resp == RESPOND_NOTHING)
            continue;
        buffer.put( &access.type );
        buffer.put( &access.primary_key );
        buffer.put( &access.table_id );

        if (access.resp == RESPOND_META) {
            uint32_t data_size = 0;
            buffer.put( &data_size );
        } else {
            assert(access.data_size != 0);
            buffer.put( &access.data_size );
            buffer.put( access.local_data, access.data_size );
        }
        access.resp = RESPOND_NOTHING;
        num_tuples ++;
    }

    // Empty response is possible in TPCE
    // assert(num_tuples > 0);
    buffer.set(0, &num_tuples);
}

void
LockManager::process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data)
{
    // return data format:
    //        | n | (key, table_id, tuple_size, data) * n
    // store the remote tuple to local remote_set.
    UnstructuredBuffer buffer(resp_data);
    uint32_t num_tuples;
    buffer.get( &num_tuples );
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
        Access * access = get_row_access(type, table_id, primary_key, _remote_set);
        buffer.get( &data_size );
        // Tuples in remote responds are granted RD accesses
        access->granted_access = RD;
        access->home_node_id = node_id;
        assert(data_size < 65536);
        assert(data_size >= 0);
        if (data_size != 0) {
            access->data_size = data_size;
            char * data = NULL;
            buffer.get( data, data_size );
            // Tuples in _remote_set do not have an valid row pointer
            // access->local_data stores remotely returned data
            if (access->local_data == NULL)
                access->local_data = new char [data_size];
            memcpy(access->local_data, data, data_size);
        }
    }
}

RC
LockManager::process_prepare_phase_coord()
{
    RC rc = RCOK;
    assert(_state == CC_EXECUTING || _state == CC_PREPARING);
    if (_state == CC_EXECUTING)
        _state = CC_PREPARING;

    rc = lock_write_set();
#if CC_ALG == WOUND_WAIT
    rc = _wounded ? ABORT : rc;
#endif
    if (rc == RCOK)
        _state = CC_PREPARED;
    return rc;
}

bool
LockManager::need_prepare_req(uint32_t remote_part_id, UnstructuredBuffer &buffer)
{
    uint32_t num_accesses = 0;
    set<uint32_t>& remote_parts = _txn->get_remote_parts_involved();
    uint32_t num_parts = remote_parts.size() + 1;

    // Format
    //   | timestamp | num_access | num_parts | part_id * K | (table_id, key, size, data) * N
    buffer.put( &_timestamp );
    buffer.put( &num_accesses );
    buffer.put( &num_parts );
    buffer.put( &g_part_id );
    for (uint32_t part_id : remote_parts)
        buffer.put( &part_id );
    
    for (auto & ac : _remote_set) {
        if (transport->get_node_partition(ac.home_node_id) == remote_part_id) {
            assert(ac.type == RD || ac.type == WR);
            if (ac.type == RD)
                continue;

            buffer.put( &ac.table_id );
            buffer.put( &ac.primary_key );
            assert(ac.data_size > 0);
            buffer.put( &ac.data_size );
            buffer.put( ac.local_data, ac.data_size );
            num_accesses ++;
        }
    }

    buffer.set(sizeof(_timestamp), &num_accesses);
    return true;
}

RC
LockManager::process_prepare_req(uint32_t size, char * data, UnstructuredBuffer &buffer)
{
    RC rc = RCOK;

    if (_state == CC_EXECUTING) {
        _state = CC_PREPARING;

        // Format
        //   | timestamp | num_access | num_parts | part_id * K | (table_id, key, size, data) * N
        UnstructuredBuffer buffer(data);
        uint32_t num_accesses;
        uint32_t num_parts;
        set<uint32_t>& remote_parts = _txn->get_remote_parts_involved();
        buffer.get( &_timestamp );
        buffer.get( &num_accesses );
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
            uint32_t table_id;
            uint64_t primary_key;
            buffer.get(&table_id);
            buffer.get(&primary_key);
            Access * access = get_row_access(WR, table_id, primary_key, _access_set);
            if (access->row == NULL)
                access->row = raw_primary_index_read(table_id, primary_key);
            char * data;
            buffer.get( &access->data_size );
            buffer.get( data, access->data_size );
            if (access->local_data == NULL)
                access->local_data = new char[access->data_size];
            memcpy(access->local_data, data, access->data_size);
        }
    }

    // 2PC prepare phase
    rc = lock_write_set();
#if CC_ALG == WOUND_WAIT
    rc = _wounded ? ABORT : rc;
#endif
    if (rc == ABORT) {
        cleanup(ABORT);
        return ABORT;
    } else {
        if (rc == COMMIT && is_read_only()) {
            cleanup(COMMIT);
            return COMMIT;
        } else
            return rc;
    }
}

void
LockManager::process_prepare_resp(RC rc, uint32_t node_id, char * data)
{
    assert(data == NULL);
}

void
LockManager::process_commit_phase_coord(RC rc)
{
    if (rc == COMMIT) {
        commit_insdel();
        cleanup(COMMIT);
    } else {
        cleanup(ABORT);
    }
}

RC
LockManager::commit_insdel()
{
    for (auto ins : _inserts) {
        row_t * row = ins.row;
        set<INDEX *> indexes;
        ins.table->get_indexes( &indexes );
        for (auto idx : indexes) {
            uint64_t key = row->get_index_key(idx);
            idx->insert(key, row);
        }
    }

    for (auto row : _deletes) {
        set<INDEX *> indexes;
        row->get_table()->get_indexes( &indexes );
        for (auto idx : indexes)
            idx->remove(row);
        for (vector<Access>::iterator it = _access_set.begin(); it != _access_set.end(); it++) {
            if (it->row == row) {
                _access_set.erase(it);
                break;
            }
        }
    }
    return RCOK;
}

bool
LockManager::need_commit_req(RC rc, uint32_t part_id, UnstructuredBuffer & buffer)
{
    return true;
}

void
LockManager::process_commit_req(RC rc, uint32_t size, char * data)
{
    cleanup(rc);
}

void
LockManager::cleanup(RC rc)
{
    for (Access & access : _access_set) {
        if (access.type == WR && rc == COMMIT && !_txn->is_applier()) {
            assert(access.local_data != NULL);
            access.row->copy(access.local_data);
        }
        assert(access.type != NA);
        access.row->manager->lock_release(_txn, rc);
        if (access.local_data)
            delete[] access.local_data;
        access.local_data = NULL;
    }

    for (Access & access : _remote_set) {
        if (access.local_data)
            delete[] access.local_data;
        access.local_data = NULL;
    }

    for (IndexAccess & access : _index_access_set) {
        assert(access.type != NA);
        access.manager->lock_release(_txn, rc);
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

bool
LockManager::set_txn_ready(RC rc)
{
    if (rc == RCOK)
        return ATOM_SUB_FETCH(_num_lock_waits, 1) == 0;
    else
        return ATOM_CAS(_signal_abort, false, true);
}

bool
LockManager::is_txn_ready()
{
    return _num_lock_waits == 0 || _signal_abort;
}

// TODO: handle index delete on appliers
LogRecord *
LockManager::get_log_record(LogType type)
{
    UnstructuredBuffer buffer;
    if (type == LOG_PREPARE || type == LOG_LOCAL_COMMIT) {
        // Records in the read set can be safely ignored
        // index insert: | type (INS)   | table_id | data_size | data
        // write tuple:  | type (WR)    | table_id | key | data_size | data

        // Construct header
        if (type == LOG_PREPARE) {
            uint32_t coord_part = _txn->get_coord_part();
            uint32_t num_remote_parts = _txn->get_remote_parts_involved().size();
            buffer.put(&coord_part);
            buffer.put(&num_remote_parts);
            for (uint32_t remote_part : _txn->get_remote_parts_involved()) 
                buffer.put(&remote_part);
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

        // Write set
        for (auto &ac : _access_set) {
            if (ac.type == WR) {
                buffer.put(&ac.type);
                buffer.put(&ac.table_id);
                buffer.put(&ac.primary_key);
                buffer.put(&ac.data_size);
                buffer.put(ac.local_data, ac.data_size);
            }
        }
    }
    
    LogRecord *record = (LogRecord *) new char[sizeof(LogRecord) + buffer.size()];
    record->size = sizeof(LogRecord) + buffer.size();
    record->type = type;
    record->txn_id = _txn->get_txn_id();
    if (buffer.size() > 0)
        memcpy(record->data, buffer.data(), buffer.size());
    return record;
}

// TODO: handle index delete on appliers
RC
LockManager::apply_log_record(LogRecord * log)
{
    if (_state == CC_EXECUTING) {
        assert(log->type == LOG_PREPARE || log->type == LOG_LOCAL_COMMIT);
        UnstructuredBuffer buffer(log->data);
        uint32_t size = log->size - sizeof(LogRecord);
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
            if (type == INS) {
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
            } else if (type == WR) {
                uint32_t table_id;
                uint64_t primary_key;
                buffer.get(&table_id);
                buffer.get(&primary_key);
                Access * access = get_row_access(WR, table_id, primary_key, _access_set);
                if (access->row == NULL)
                    access->row = raw_primary_index_read(table_id, primary_key);
                char * data;
                buffer.get(&access->data_size);
                buffer.get(data, access->data_size);
                if (access->local_data == NULL)
                    access->local_data = new char [access->data_size];
                memcpy(access->local_data, data, access->data_size);
            } else {
                assert(false);
            }
            assert(buffer.get_offset() <= size);
        }
        assert(buffer.get_offset() == size);
        _state = CC_COMMITTING;
        if (log->type == LOG_PREPARE)
            return RCOK;
    }

    if (_state == CC_COMMITTING) {
        assert(log->type == LOG_LOCAL_COMMIT || log->type == LOG_DIST_COMMIT || log->type == LOG_ABORT);
        if (log->type == LOG_ABORT) {
            cleanup(ABORT);
            return ABORT;
        }

        // Sort _index_access_set beforehand to prevent deadlock
        sort(_index_access_set.begin(), _index_access_set.end(), [](const IndexAccess & a, const IndexAccess & b) {
            if (a.index->get_index_id() != b.index->get_index_id())
                return a.index->get_index_id() < b.index->get_index_id();
            else
                return a.key < b.key;
        });
        // Acquire locks on all indexes
        for (; _lock_acquire_idx < _index_access_set.size(); _lock_acquire_idx++) {
            IndexAccess & access = _index_access_set[_lock_acquire_idx];
            assert(access.type == INS);
            Row_lock::LockType lock_type = Row_lock::LOCK_EX;
            RC rc = access.manager->lock_get(lock_type, _txn);
            assert(rc == WAIT || rc == RCOK);
            if (rc == WAIT)
                return rc;
            update_granted_access(&access, INS);
        }
        // Insert rows to indexes
        for (auto ins : _inserts) {
            row_t * row = ins.row;
            set<INDEX *> indexes;
            ins.table->get_indexes( &indexes );
            for (auto idx : indexes) {
                uint64_t key = row->get_index_key(idx);
                idx->insert(key, row);
            }
        }

        // Sort _access_set beforehand to prevent deadlock
        sort(_access_set.begin(), _access_set.end(), [](const Access & a, const Access & b) {
            if (a.table_id != b.table_id)
                return a.table_id < b.table_id;
            else
                return a.primary_key < b.primary_key;
        });
        for (; _lock_acquire_idx < _index_access_set.size() + _access_set.size(); _lock_acquire_idx++) {
            Access& access = _access_set[_lock_acquire_idx - _index_access_set.size()];
            assert(access.type == WR);
            // The previous insert log might be lost so here the row might not exist
            if (access.row == NULL) {
                _access_set.erase(_access_set.begin() + _lock_acquire_idx - _index_access_set.size());
                _lock_acquire_idx--;
                continue;
            }

            Row_lock::LockType lock_type = Row_lock::LOCK_EX;
            RC rc = access.row->manager->lock_get(lock_type, _txn);
            assert(rc == WAIT || rc == RCOK);
            if (rc == WAIT) {
                return WAIT;
            } else {
                access.row->copy(access.local_data);
                access.row->manager->lock_release(_txn, RCOK);
            }
        }
        cleanup(COMMIT);
        return COMMIT;
    }
    
    return RCOK;
}

LockManager::Access *
LockManager::find_access(uint64_t primary_key, uint32_t table_id, vector<Access>& set)
{
    for (auto & ac : set) {
        if (ac.primary_key == primary_key && ac.table_id == table_id)
            return &ac;
    }
    return NULL;
}

LockManager::IndexAccess *
LockManager::find_access(uint64_t key, uint32_t index_id, vector<IndexAccess>& set)
{
    for (auto & ac : set) {
        if (ac.key == key && ac.index->get_index_id() == index_id)
            return &ac;
    }
    return NULL;
}

RC
LockManager::lock_write_set()
{
    RC rc = RCOK;
    for (; _lock_acquire_idx < _index_access_set.size(); _lock_acquire_idx++) {
        IndexAccess &access = _index_access_set[_lock_acquire_idx];
        if (access.type == INS || access.type == DEL) {
            rc = get_index_permission(&access, WR);
            if (rc != RCOK)
                return rc;
        }
    }
    for (; _lock_acquire_idx < _index_access_set.size() + _access_set.size(); _lock_acquire_idx++) {
        Access &access = _access_set[_lock_acquire_idx - _index_access_set.size()];
        if (access.type == WR) {
            rc = get_row_permission(&access, WR);
            if (rc != RCOK)
                return rc;
        }
    }
    return RCOK;
}

LockManager::IndexAccess *
LockManager::get_index_access(access_t type, INDEX * index, uint64_t key, vector<IndexAccess>& set)
{
    assert(type == RD || type == INS || type == DEL);
    IndexAccess * access = find_access(key, index->get_index_id(), set);

    if (!access) {
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
    if (access->type == RD && (type == INS || type == DEL))
        access->type = type;

    return access;
}

LockManager::Access *
LockManager::get_row_access(access_t type, uint32_t table_id, uint64_t primary_key, vector<Access>& set)
{
    assert(type == RD || type == WR);
    Access * access = find_access(primary_key, table_id, set);

    if (!access) {
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

    if (access->type == RD && type == WR)
        access->type = WR;
    _last_access = access;

    return access;
}

#endif
