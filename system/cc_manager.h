#pragma once

#include "global.h"
#include "helper.h"
#include "log.h"

class TxnManager;
class row_t;
class INDEX;
class table_t;
class StoreProcedure;
class UnstructuredBuffer;
class RemoteQuery;
class itemid_t;

class CCManager
{
public:
    friend class TxnManager;
    CCManager(TxnManager * txn, uint64_t ts);
    virtual ~CCManager() {};

    enum RespType {
        RESPOND_ALL,
        RESPOND_META,
        RESPOND_NOTHING,
    };

    enum CCState {
        CC_EXECUTING,
        CC_PREPARING,
        CC_PREPARED,
        CC_COMMITTING,
        CC_FINISHED,
    };

    struct Access {
        Access() {
            row = NULL;
            granted_access = NA;
            local_data = NULL;
            data_size = 0;
            resp = RESPOND_ALL;
        }
        access_t     type;
        uint64_t     primary_key;
        uint32_t     table_id;
        uint32_t     home_node_id;
        access_t     granted_access;
        row_t *      row;
        uint32_t     data_size;
        char *       local_data;
        RespType     resp;
    };

    struct IndexAccess {
        IndexAccess() {
            index = NULL;
            manager = NULL;
            rows = NULL;
            granted_access = NA;
            resp = RESPOND_META;
        };
        uint64_t       key;
        access_t       type;
        access_t       granted_access;
        RespType       resp;
        uint32_t       home_node_id;
        INDEX *        index;
        ROW_MAN *      manager;
        set<row_t *> * rows;
    };

    virtual void     init();
    CCState          get_cc_state() { return _state; }
    void             set_cc_state(CCState state) { _state = state; }
    RC               row_request(uint32_t table_id, uint32_t index_id, uint64_t key, access_t type, bool reserve = false, uint32_t min_rows = 1, uint32_t max_rows = 1);
    RC               row_request(row_t * row, uint32_t table_id, access_t type);
    char*            buffer_request(uint32_t table_id, uint32_t index_id, uint64_t key, access_t type);
    RC               buffer_request(vector<char *>& results, uint32_t table_id, uint32_t index_id, uint64_t key, access_t type, uint32_t min_rows, uint32_t max_rows);
    RC               process_requests();

    // For algorithms other than TicToc, we don't care whether the txn is readonly or not.
    virtual bool     is_read_only() { return false; }

    virtual RC       get_row_permission(Access* access, access_t type) { assert(false); }
    virtual RC       get_row(row_t * row, access_t type, bool reserve = false) { assert(false); }
    virtual RC       get_row(row_t * row, access_t type, char * &data) = 0;

    virtual RC       get_index_permission(IndexAccess* acesss, access_t type) { assert(false); }
    virtual RC       index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit = -1) { assert(false); }
    virtual RC       index_insert(INDEX * index, uint64_t key) { assert(false); }
    virtual RC       index_delete(INDEX * index, uint64_t key) { assert(false); }

    virtual uint32_t get_node_for_request(uint32_t table_id, uint64_t key, bool & reserve) { assert(false); };

    // rc is either COMMIT or Abort.
    // the following function will cleanup the txn. e.g., release locks, etc.
    virtual void     cleanup(RC rc) { assert(false); }

    StoreProcedure * get_store_procedure();

    ////////// for txn waiting /////////////
    virtual bool     is_txn_ready() = 0; // { assert(false); }
    virtual void     set_txn_ready() { assert(false); }
    virtual bool     set_txn_ready(RC rc) { assert(false); } // return true if txn changes from "waiting" to "ready"
    virtual void     set_txn_waiting() { ATOM_ADD_FETCH(_num_lock_waits, 1); }
    virtual bool     is_signal_abort() { assert(false); }
    virtual uint64_t get_priority() { return 0; }
    //////////////////////////////////////

    // handle response during normal execution
    virtual RC       process_remote_req(uint32_t size, char * req_data, uint32_t &resp_size, char * &resp_data) { assert(false); }

    virtual void     add_remote_req_header(UnstructuredBuffer * buffer) {}
    virtual uint32_t process_remote_req_header(UnstructuredBuffer * buffer) { return 0; }
    virtual void     get_resp_data(uint32_t num_queries, RemoteQuery * queries, uint32_t &size, char * &data) { assert(false); }
    virtual void     get_resp_data(UnstructuredBuffer &buffer) { assert(false); }
    virtual void     process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data) {};

    // prepare phase.
    // return value: whether a prepare message needs to be sent
    virtual void     get_remote_nodes(set<uint32_t> * _remote_nodes) {};
    void             get_remote_nodes_with_writes(set<uint32_t> * nodes);
    virtual RC       process_prepare_phase_coord() { return RCOK; }
    virtual bool     need_prepare_req(uint32_t remote_part_id, UnstructuredBuffer &buffer) { return true; };
    virtual RC       process_prepare_req(uint32_t size, char * data, UnstructuredBuffer &buffer) { return RCOK; }
    virtual void     process_prepare_resp(RC rc, uint32_t node_id, char * data) {};

    // amend phase
    virtual RC       process_amend_phase_coord() { return RCOK; }
    virtual bool     need_amend_req(uint32_t remote_node_id, uint32_t &size, char * &data) { return false; };
    virtual RC       process_amend_req(char * data) { return RCOK; }

    // commit phase
    virtual void     install_local_write_set() { assert(false); }
    virtual void     process_commit_phase_coord(RC rc) = 0;
    virtual bool     need_commit_req(RC rc, uint32_t node_id, UnstructuredBuffer &buffer) { return true; }
    virtual void     process_commit_req(RC rc, uint32_t size, char * data) = 0;
    virtual void     release_all_locks() = 0;
    virtual RC       force_write_set() = 0;

    // [TICTOC] handle local caching
    virtual uint32_t handle_local_caching(char * &data) { return 0; };
    virtual void     process_caching_resp(uint32_t node_id, uint32_t size, char * data) { return; }

    virtual LogRecord * get_log_record(LogType log_type) { assert(false); }
    virtual RC          apply_log_record(LogRecord *log) { assert(false); }
    static LogRecord *  get_test_log_record();

protected:
    volatile int32_t    _num_lock_waits;
    uint64_t            _timestamp;

    struct RemoteNodeInfo {
        uint32_t node_id;
        bool     has_write;
    };
    map<uint32_t, RemoteNodeInfo> _remote_node_info;
    void add_remote_node_info(uint32_t node_id, bool is_write);

    // TODO. different CC algorithms should have different ways to handle index consistency.
    // For now, just ignore index concurrency control.
    // Since this is not a problem for YCSB and TPCC.
    virtual RC commit_insdel();

    void    update_granted_access(Access * access, access_t type) { if (access->granted_access < type) access->granted_access = type; }
    void    update_granted_access(IndexAccess * access, access_t type) { if (access->granted_access < type) access->granted_access = type; }
    row_t * raw_primary_index_read(uint32_t table_id, uint64_t primary_key);

    struct RequestBase {
        access_t type;
        uint32_t table_id;
        uint32_t index_id;
        uint64_t key;
    };

    struct RowRequest : public RequestBase {
        bool reserve;
        uint32_t min_rows;
        uint32_t max_rows;
    };

    struct PermRequest : public RequestBase {
        Access * access;
    };

    struct IndexRequest : RequestBase {
        row_t * row;
    };

    vector<RequestBase*> pending_local_requests;
    vector<RequestBase*> pending_remote_requests;

    TxnManager *         _txn;
    struct InsertOp {
        table_t * table;
        row_t * row;
    };
    vector<InsertOp> _inserts;
    vector<row_t *>  _deletes;
    // remote query processing
    bool             _restart;
    bool             _waiting_requests;
    uint32_t         _curr_request_idx;
    uint32_t         _curr_row_idx;
    // Stats
    uint64_t         _time_in_cc_man;
    CCState          _state;

private:
    static void      clear_pending_requests(vector<RequestBase*>& requests);
};
