#include <algorithm>
#include "silo_manager.h"
#include "row.h"
#include "row_silo.h"
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
#if CC_ALG == SILO

SiloManager::SiloManager(TxnManager * txn, uint64_t ts)
    : CCManager(txn, glob_manager->get_ts(GET_THD_ID))
{
    _is_read_only = true;
    _min_commit_ts = 0;
    _num_lock_waits = 0;
    _signal_abort = false;
}

void
SiloManager::init()
{
    CCManager::init();
}

void
SiloManager::cleanup(RC rc)
{
    for (AccessSilo & access : _access_set) {
        if (access.type == WR && rc == COMMIT) {
            assert(access.local_data != NULL);
            access.row->manager->write(_txn, access.local_data, _min_commit_ts);
        }
        assert(access.type != NA);
        if (access.type != RD)
            access.row->manager->release(_txn, rc);
        if (access.local_data)
            delete[] access.local_data;
        access.local_data = NULL;
    }

    for (AccessSilo & access : _remote_set) {
        if (access.local_data)
            delete[] access.local_data;
    }

    for (IndexAccessSilo & access : _index_access_set) {
        if (access.type != RD)
            access.manager->release(_txn, rc);
        if (access.rows)
            delete access.rows;
    }

    if (rc == ABORT)
        for (auto & ins : _inserts)
            delete ins.row;

    _access_set.clear();
    _remote_set.clear();
    _index_access_set.clear();
    _inserts.clear();
    _deletes.clear();
}

RC
SiloManager::register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key,
                                    uint32_t table_id, uint32_t &msg_size, char * &msg_data)
{
    AccessSilo ac;
    assert(remote_node_id != g_node_id);
    ac.home_node_id = remote_node_id;
    ac.row = NULL;
    ac.type = type;
    ac.key = key;
    ac.table_id = table_id;
    ac.local_data = NULL;
    _remote_set.push_back(ac);

    AccessSilo * access = &(*_remote_set.rbegin());
    _last_access = access;
    return LOCAL_MISS;
}

RC
SiloManager::get_row(row_t * row, access_t type, uint64_t key)
{
    return get_row(row, type, key, -1);
}

RC
SiloManager::get_row_permission(Access * raw_access, access_t type)
{
    RC rc = RCOK;
    assert(type == RD || type == WR);
    AccessSilo * access = (AccessSilo*) raw_access;

    if (type == WR)
        _is_read_only = false;

    if (type == WR && access->granted_access != WR)
        rc = access->row->manager->lock(_txn);

    update_granted_access(access, type);

    if (rc == RCOK && access->local_data != NULL && access->row->manager->get_wts() != access->wts)
        return ABORT;

    return rc;
}

RC
SiloManager::get_row(row_t * row, access_t type, uint64_t key, uint64_t wts)
{
    RC rc = RCOK;
    assert (_txn->get_txn_state() == TxnManager::RUNNING);

    AccessSilo * access = NULL;
    for (AccessSilo & ac : _access_set) {
        if (ac.row == row) {
            access = &ac;
            break;
        }
    }

    if (!access) {
        AccessSilo ac;
        ac.home_node_id = g_node_id;
        ac.row = row;
        ac.type = type;
        ac.key = key;
        ac.table_id = row->get_table()->get_table_id();
        ac.data_size = row->get_tuple_size();
        ac.local_data = NULL;

        _access_set.push_back(ac);
        access = &(*_access_set.rbegin());
    } 

    if (access->type == RD && type == WR)
        access->type = WR;

    _last_access = access;
    rc = get_row_permission(access, type);

    if (rc != RCOK)
        return rc;
    
    if (access->local_data == NULL) {
        access->local_data = new char[access->data_size];
        row->manager->read(_txn, access->local_data, access->wts);
    }

    assert(rc == RCOK);
    return rc;
}

RC
SiloManager::get_index_permission(IndexAccess* raw_access, access_t type)
{
    RC rc = RCOK;
    assert(type == RD || type == WR);
    IndexAccessSilo * access = (IndexAccessSilo*) raw_access;

    if (type == WR)
        _is_read_only = false;

    if (type == WR && access->granted_access != WR)
        rc = access->manager->lock(_txn);

    update_granted_access(access, type);

    if (rc == ABORT)
        return ABORT;

    if (rc == RCOK && access->rows != NULL && access->manager->get_wts() != access->wts)
        return ABORT;
    
    return rc;
}

SiloManager::IndexAccessSilo*
SiloManager::get_index_access(access_t type, INDEX * index, uint64_t key)
{
    assert(type == RD || type == INS || type == DEL);
    IndexAccessSilo * access = NULL;
    for (IndexAccessSilo & ac : _index_access_set) {
        if (ac.index == index && ac.key == key)  {
            access = &ac;
            break;
        }
    }

    if (!access) {
        IndexAccessSilo ac;
        ac.key = key;
        ac.index = index;
        ac.type = type;
        ac.rows = NULL;
        ac.granted_access = NA;
        ac.manager = index->index_get_manager(key);

        _index_access_set.push_back(ac);
        access = &(*_index_access_set.rbegin());
    }

    _last_index_access = access;

    if (access->type == RD && (type == INS || type == DEL))
        access->type = type;

    return access;
}

RC
SiloManager::index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit)
{
    uint64_t tt = get_sys_clock();
    IndexAccessSilo *access = get_index_access(RD, index, key);
    RC rc = get_index_permission(access, RD);
    assert(rc == RCOK);

    if (access->rows == NULL) {
        access->manager->latch();
        rows = index->read(key);
        access->wts = access->manager->get_wts();
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
SiloManager::index_insert(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    IndexAccessSilo *access = get_index_access(INS, index, key);
    RC rc = get_index_permission(access, WR);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    return rc;
}

RC
SiloManager::index_delete(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    IndexAccessSilo *access = get_index_access(INS, index, key);
    RC rc = get_index_permission(access, WR);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    return rc;
}

RC
SiloManager::get_row(row_t * row, access_t type, char * &data, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = get_row(row, type, key);
    if (rc == RCOK)
        data = _last_access->local_data;
    INC_FLOAT_STATS(row, get_sys_clock() - tt);
    return rc;
}

void
SiloManager::add_remote_req_header(UnstructuredBuffer * buffer)
{
    buffer->put_front( &_timestamp );
}

uint32_t
SiloManager::process_remote_req_header(UnstructuredBuffer * buffer)
{
    buffer->get( &_timestamp );
#if TS_ALLOC == TS_HYBRID
    uint64_t sub_txn_clock = glob_manager->get_clock_from_ts(_timestamp);
    glob_manager->max_sub_txn_clock = max(sub_txn_clock, glob_manager->max_sub_txn_clock);
#endif
    return sizeof(_timestamp);
}

void
SiloManager::compute_commit_ts()
{
    for (auto & access : _access_set)
        _min_commit_ts = max(access.wts + 1, _min_commit_ts);

    for (auto & access : _remote_set)
        _min_commit_ts = max(access.wts + 1, _min_commit_ts);

    for (auto access : _index_access_set) {
        _min_commit_ts = max(access.wts + 1, _min_commit_ts);
    }
}

void
SiloManager::get_resp_data(uint32_t &size, char * &data)
{
    // construct the return message.
    // Format:
    //    | n | (key, table_id, wts, rts, tuple_size, data) * n
    UnstructuredBuffer buffer;
    uint32_t num_tuples = 0;
    for (AccessSilo & access : _access_set) {
        if (access.resp == RESPOND_NOTHING) continue;
        buffer.put( &access.key );
        buffer.put( &access.table_id );
        buffer.put( &access.wts );

        if (access.resp == RESPOND_META) {
            uint32_t data_size = 0;
            buffer.put( &data_size );
        } else {
            buffer.put( &access.data_size );
            buffer.put( access.local_data, access.data_size );           
        }
        access.resp = RESPOND_NOTHING;
        num_tuples ++;
    }
    assert(num_tuples > 0);
    buffer.put_front( &num_tuples );
    size = buffer.size();
    data = new char[size];
    memcpy(data, buffer.data(), size);
}

uint32_t
SiloManager::get_log_record(char *& record)
{
    uint32_t size = 0;
    for (auto access : _access_set) {
        if (access.type == WR) {
            //    access_t     type;
            // uint64_t     key;
            // uint32_t     table_id;
            size += sizeof(access_t) + sizeof(uint32_t) + sizeof(uint64_t) + access.data_size;
        }
    }
    if (size > 0) {
        uint32_t offset = 0;
        record = new char[size];
        for (auto access : _access_set) {
            if (access.type == WR) {
                //    access_t     type;
                // uint64_t     key;
                // uint32_t     table_id;
                memcpy(record + offset, &access.type, sizeof(access_t));
                offset += sizeof(access_t);
                memcpy(record + offset, &access.key, sizeof(uint64_t));
                offset += sizeof(access_t);
                memcpy(record + offset, &access.table_id, sizeof(uint32_t));
                offset += sizeof(access_t);
                memcpy(record + offset, &access.local_data, access.data_size);
            }
        }
    }
    return size;
}


SiloManager::AccessSilo *
SiloManager::find_access(uint64_t key, uint32_t table_id, vector<AccessSilo> * set)
{
    for (vector<AccessSilo>::iterator it = set->begin(); it != set->end(); it ++) {
        if (it->key == key && it->table_id == table_id)
            return &(*it);
    }
    return NULL;
}

void
SiloManager::process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data)
{
    // return data format:
    //    | n | (key, table_id, wts, rts, tuple_size, data) * n
    // store the remote tuple to local access_set.
    UnstructuredBuffer buffer(resp_data);
    uint32_t num_tuples;
    buffer.get( &num_tuples );
    assert(num_tuples > 0);
    for (uint32_t i = 0; i < num_tuples; i++) {
        uint64_t key;
        uint32_t table_id;
        uint32_t data_size;
        buffer.get( &key );
        buffer.get( &table_id );
        AccessSilo * access = find_access(key, table_id, &_remote_set);
        assert(access && node_id == access->home_node_id);
        buffer.get( &access->wts );
        buffer.get( &data_size );
        assert(data_size < 65536);
        if (data_size != 0) {
            char * data = NULL;
            access->data_size = data_size;
            buffer.get( data, access->data_size );
            access->local_data = new char [access->data_size];
            memcpy(access->local_data, data, access->data_size);
        }
        access->granted_access = access->type;
    }
}

RC
SiloManager::validate_read_set(uint64_t commit_ts)
{
    for (auto & access : _index_access_set) {
        if (access.type == INS || access.type == DEL)
            continue;
        if (access.wts != access.manager->get_wts() || access.manager->_lock_owner != NULL)
            return ABORT;
    }

    for (auto & access : _access_set) {
        if (access.type == RD && (access.wts != access.row->manager->get_wts() || access.row->manager->_lock_owner != NULL))
            return ABORT;
    }
    return RCOK;
}

RC
SiloManager::process_prepare_phase_coord()
{
    RC rc = RCOK;
    // 1. compute commit ts
    // 2. validate local txn

    // [Compute the commit timestamp]
    compute_commit_ts();
    rc = validate_read_set(_min_commit_ts);
    
    return rc;
}

void
SiloManager::get_remote_nodes(set<uint32_t> * remote_nodes)
{
}

bool
SiloManager::need_prepare_req(uint32_t remote_node_id, uint32_t &size, char * &data)
{
    UnstructuredBuffer buffer;
    buffer.put( &_min_commit_ts );
    size = buffer.size();
    data = new char [size];
    memcpy(data, buffer.data(), size);
    return true;
}

RC
SiloManager::process_prepare_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data )
{
    RC rc = RCOK;
    // Request Format:
    //     | commit_ts | ...
    UnstructuredBuffer req_buffer(data);
    uint64_t commit_ts;
    req_buffer.get( &commit_ts );
    M_ASSERT(_min_commit_ts <= commit_ts, "txn=%ld. _min_commit_ts=%ld, commit_ts=%ld", _txn->get_txn_id(), _min_commit_ts, commit_ts);
    _min_commit_ts = commit_ts;
    assert(size == sizeof(commit_ts));

    rc = validate_read_set(_min_commit_ts);

    if (rc == ABORT) {
        cleanup(ABORT);
        return ABORT;
    } else {
        if (is_read_only()) {
            cleanup(COMMIT);
            return COMMIT;
        } else
            return rc;
    }
}

void
SiloManager::process_prepare_resp(RC rc, uint32_t node_id, char * data)
{
    assert(data == NULL);
}

void
SiloManager::process_commit_phase_coord(RC rc)
{
    if (rc == COMMIT) {
        commit_insdel();
        cleanup(COMMIT);
    } else {
        cleanup(ABORT);
    }
}

RC
SiloManager::commit_insdel() {
    for (auto ins : _inserts) {
        row_t * row = ins.row;
        row->manager->set_wts(_min_commit_ts);
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
        for (vector<AccessSilo>::iterator it = _access_set.begin();
             it != _access_set.end(); it ++)
        {
            if (it->row == row) {
                _access_set.erase(it);
                break;
            }
        }
        row->manager->delete_row( _min_commit_ts );
        // TODO. not deleting the row here. because another txn might be accessing it.
        // A better solution is to handle deletion in a special way. For example, access the
        // index again to see if the tuple still exists.
        //delete row;
    }
    for (auto access : _index_access_set)
        if (access.type != RD) {
            M_ASSERT(access.manager->_lock_owner == _txn, "lock_owner=%#lx, _txn=%#lx",
                (uint64_t)access.manager->_lock_owner, (uint64_t)_txn);
            access.manager->set_wts(_min_commit_ts);
        }

    return RCOK;

}

bool
SiloManager::need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data)
{
    if (rc == ABORT)
        return true;

    uint32_t num_writes = 0;
    for (auto & ac : _remote_set)
        if (ac.home_node_id == node_id && ac.type == WR)
            num_writes ++;


    // COMMIT and the remote node is not readonly.
    // Format
    //   | commit_ts | num_writes | (key, table_id, size, data) * num_writes
    assert(rc == COMMIT);
    UnstructuredBuffer buffer;
    buffer.put( &_min_commit_ts );
    buffer.put( &num_writes );
    for (auto & ac : _remote_set)
        if (ac.home_node_id == node_id && ac.type == WR) {
            buffer.put( &ac.key );
            buffer.put( &ac.table_id );
            buffer.put( &ac.data_size );
            buffer.put( ac.local_data, ac.data_size );
        }
    size = buffer.size();
    data = new char[size];
    memcpy(data, buffer.data(), size);
    return true;
}

void
SiloManager::process_commit_req(RC rc, uint32_t size, char * data)
{
    if (rc == COMMIT) {
        // Format
        //   | commit_ts | num_writes | (key, table_id, size, data) * num_writes
        UnstructuredBuffer buffer(data);
        uint64_t commit_ts;
        buffer.get( &commit_ts );
        if (size > sizeof(uint64_t)) {
            uint32_t num_writes;
            buffer.get( &num_writes );
            for (uint32_t i = 0; i < num_writes; i ++) {
                uint64_t key;
                uint32_t table_id;
                uint32_t size = 0;
                char * tuple_data = NULL;
                buffer.get( &key );
                buffer.get( &table_id );
                buffer.get( &size );
                buffer.get( tuple_data, size );
                AccessSilo * access = find_access( key, table_id, &_access_set );
                memcpy(access->local_data, tuple_data, size);
            }
        }
        cleanup(COMMIT);
    } else
        cleanup(ABORT);
}

bool
SiloManager::is_txn_ready()
{
    return _num_lock_waits == 0 || _signal_abort;
}

bool
SiloManager::set_txn_ready(RC rc)
{
    // this function can be called by multiple threads concurrently
    if (rc == RCOK)
        return ATOM_SUB_FETCH(_num_lock_waits, 1) == 0;
    else {
        return ATOM_CAS(_signal_abort, false, true);
    }
}

#endif
