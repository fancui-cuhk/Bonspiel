#pragma once

#include "cc_manager.h"
#include <chrono>
#include <thread>

#if CC_ALG == SILO

class SiloManager : public CCManager
{
public:
    SiloManager(TxnManager * txn, uint64_t ts);
    ~SiloManager() {};
    friend class CCManager;

    void        init();
    bool        is_read_only() { return _is_read_only; }

    RC          get_row_permission(Access* access, access_t type);
    RC          get_row(row_t * row, access_t type, uint64_t key);
    RC          get_row(row_t * row, access_t type, uint64_t key, uint64_t wts);
    RC          get_row(row_t * row, access_t type, char * &data, uint64_t key);

    RC          get_index_permission(IndexAccess* acesss, access_t type);
    RC          index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit = -1);
    RC          index_insert(INDEX * index, uint64_t key);
    RC          index_delete(INDEX * index, uint64_t key);

    RC          register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key, uint32_t table_id, uint32_t &msg_size, char * &msg_data);

    // normal execution
    void        add_remote_req_header(UnstructuredBuffer * buffer);
    uint32_t    process_remote_req_header(UnstructuredBuffer * buffer);
    void        get_resp_data(uint32_t &size, char * &data);
    void        process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data);

    // prepare phase
    RC          process_prepare_phase_coord();
    void        get_remote_nodes(set<uint32_t> * remote_nodes);
    bool        need_prepare_req(uint32_t remote_node_id, uint32_t &size, char * &data);
    RC          process_prepare_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data );
    void        process_prepare_resp(RC rc, uint32_t node_id, char * data);

    // commit phase
    void        process_commit_phase_coord(RC rc);
    RC          commit_insdel();
    bool        need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data);
    void        process_commit_req(RC rc, uint32_t size, char * data);
    void        cleanup(RC rc);
    void        commit() { cleanup(COMMIT); }
    void        abort() { cleanup(ABORT); }

    // handle WAIT_DIE validation
    void        set_ts(uint64_t timestamp) { _timestamp = timestamp; }
    uint64_t    get_priority() { return _timestamp; }
    bool        set_txn_ready(RC rc);
    bool        is_txn_ready();
    bool        is_signal_abort() { return _signal_abort; }

    // for logging
    uint32_t     get_log_record(char *& record);

private:
    bool         _is_read_only;
    struct IndexAccessSilo : IndexAccess {
        uint64_t wts;
        IndexAccessSilo() { wts = 0; }
    };

    struct AccessSilo : Access {
        uint64_t     wts;
        AccessSilo() { wts = 0; };
    };

    vector<IndexAccessSilo>   _index_access_set;
    vector<AccessSilo>        _access_set;
    vector<AccessSilo>        _remote_set;
    AccessSilo *              _last_access;
    IndexAccessSilo *         _last_index_access;
    bool                      _signal_abort;
    uint64_t                  _min_commit_ts;

    AccessSilo *find_access(uint64_t key, uint32_t table_id, vector<AccessSilo> * set);
    void       compute_commit_ts();
    RC         validate_read_set(uint64_t commit_ts);
    IndexAccessSilo * get_index_access(access_t type, INDEX * index, uint64_t key);

};

#endif
