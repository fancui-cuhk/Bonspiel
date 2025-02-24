#pragma once

#include "cc_manager.h"

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == WOUND_WAIT || CC_ALG == CALVIN

class LockManager : public CCManager
{
public:
    LockManager(TxnManager * txn, uint64_t ts);
    ~LockManager() {}
    friend class CCManager;

    void        init();
    bool        is_read_only() { return _is_read_only; }

    RC          get_row_permission(Access * access, access_t type);
    RC          get_row(row_t * row, access_t type, bool reserve);
    RC          get_row(row_t * row, access_t type, char * &data);

    RC          get_index_permission(IndexAccess * access, access_t type);
    RC          index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit = -1);
    RC          index_insert(INDEX * index, uint64_t key);
    RC          index_delete(INDEX * index, uint64_t key);

    uint32_t    get_node_for_request(uint32_t table_id, uint64_t key, bool & reserve);

    // Normal execution
    void        add_remote_req_header(UnstructuredBuffer * buffer);
    uint32_t    process_remote_req_header(UnstructuredBuffer * buffer);
    void        get_resp_data(UnstructuredBuffer &buffer);
    void        process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data);

    // Prepare phase
    RC          process_prepare_phase_coord();
    bool        need_prepare_req(uint32_t remote_part_id, UnstructuredBuffer &buffer);
    RC          process_prepare_req(uint32_t size, char * data, UnstructuredBuffer &buffer);
    void        process_prepare_resp(RC rc, uint32_t node_id, char * data);

    // Commit phase
    void        process_commit_phase_coord(RC rc);
    RC          commit_insdel();
    bool        need_commit_req(RC rc, uint32_t part_id, UnstructuredBuffer &buffer);
    void        process_commit_req(RC rc, uint32_t size, char * data);
    void        cleanup(RC rc);
    void        release_all_locks() { assert(false); };
    RC          force_write_set() { assert(false); };

    uint64_t    get_priority() { return _timestamp; }
    void        set_priority(uint64_t ts) { _timestamp = ts; }
    bool        set_txn_ready(RC rc);
    bool        is_txn_ready();
    bool        is_signal_abort() { return _signal_abort; }

    // Raft logging
    LogRecord * get_log_record(LogType log_type);
    RC          apply_log_record(LogRecord * log);

#if CC_ALG == WOUND_WAIT
    void        wound() { ATOM_CAS(_wounded, false, true); }
    bool        is_wounded() { return _wounded; }
#endif

private:
#if CC_ALG == WOUND_WAIT
    bool                    _wounded;
#endif

    vector<Access>          _access_set;
    vector<IndexAccess>     _index_access_set;
    vector<Access>          _remote_set;
    vector<IndexAccess>     _index_remote_set;
    Access *                _last_access;
    IndexAccess *           _last_index_access;

    bool                    _is_read_only;
    bool                    _signal_abort;
    uint32_t                _lock_acquire_idx;

    Access *        find_access(uint64_t primary_key, uint32_t table_id, vector<Access>& set);
    IndexAccess *   find_access(uint64_t key, uint32_t index_id, vector<IndexAccess>& set);
    RC              lock_write_set();
    IndexAccess *   get_index_access(access_t type, INDEX * index, uint64_t key, vector<IndexAccess>& set);
    Access *        get_row_access(access_t type, uint32_t table_id, uint64_t primary_key, vector<Access>& set);
};

#endif
