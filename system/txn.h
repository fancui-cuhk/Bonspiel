#pragma once

#include "global.h"
#include "helper.h"
#include "cc_manager.h"
#include "log.h"
#include <deque>

class workload;
class row_t;
class table_t;
class QueryBase;
class SubQuery;
class Message;
class StoreProcedure;

class TxnManager
{
public:
    enum State {
        WAITING_TO_START,
        RUNNING,
        PREPARING,
        COMMITTING,
        ABORTING,
        COMMITTED,
        ABORTED
    };

    enum ManagerType {
        MAN_COORD,
        MAN_PART,
        MAN_APPLY
    };

    TxnManager(QueryBase * query, uint64_t txn_id); // init a coordinator manager
    TxnManager(Message * msg); // init a participant manager
    TxnManager(LogRecord *log); // init an log applier
    TxnManager(ManagerType type, uint64_t txn_id); // base constructor
    TxnManager(TxnManager * txn, uint64_t txn_id); // for retry

    virtual ~TxnManager();

    void             set_txn_id(uint64_t txn_id) { _txn_id = txn_id; }
    uint64_t         get_txn_id() { return _txn_id; }
    bool             is_sub_txn() { return _type == MAN_PART; }
    bool             is_applier() { return _type == MAN_APPLY; }
    bool             is_coord()   { return _type == MAN_COORD; }
    bool             is_mp_txn()  { return _mp_txn; }
    void             set_type(ManagerType type);
    void             set_coord_part(uint32_t part_id) { coord_part = part_id; }
    uint32_t         get_coord_part() { return coord_part; }

    bool             set_txn_ready(RC rc) { return _cc_manager->set_txn_ready(rc); }
    bool             is_txn_ready() { return _cc_manager->is_txn_ready(); }

    State            get_txn_state() { return _txn_state; }
    void             set_txn_state(State state) { _txn_state = state; }
    bool             waiting_remote() { return _num_resp_expected > 0; }
    bool             waiting_local_requests() { return _cc_manager->_waiting_requests; }
    bool             waiting_log_flush() { return _waiting_log_flush; }

    CCManager *      get_cc_manager() { return _cc_manager; }
    StoreProcedure * get_store_procedure() { return _store_procedure; };

    RC               calvin_execute();
    RC               execute();
    RC               coordinator_execute();
    RC               participant_execute();
    RC               applier_execute();
    void             send_2pc_prepare();
    void             send_2pc_vote(RC rc);
    void             send_2pc_fast_vote(RC rc);
    void             send_2pc_decide(RC rc);
    void             send_2pc_ack();
#if COMMIT_PROTOCOL == TAPIR
    void             send_slow_path();
#endif
    void             write_log();
    void             send_msg(Message * msg);
    RC               process_msg(Message * msg);
    RC               process_log(LogRecord * record);
    void             cleanup_current_msg();
    void             cleanup_current_log();
    void             schedule_retry();
    bool             ready_to_start(uint64_t curr_time) { return curr_time > _ready_time; }
    set<uint32_t>&   get_remote_parts_involved() { return remote_parts_involved; }

    // Stats
    void             update_stats();
    void             print_state();

    uint32_t         get_num_aborts() { return _num_aborts; }
    void             set_num_aborts(uint32_t num_aborts) { _num_aborts = num_aborts; }
#if MULTIPLE_PRIORITY_LEVELS
    uint32_t         get_reserve_priority() { return _num_aborts / PRIORITY_BOOST_THRESHOLD; }
#endif

    deque<Message*>  pending_msgs;

    uint64_t         _time_debug1;
    uint64_t         _time_debug2;
    uint64_t         _time_debug3;
    uint64_t         _time_debug4;

private:
    friend class CCManager;

    // Store procedure (YCSB, TPCC, etc.)
    uint64_t         _txn_id;
    ManagerType      _type;
    StoreProcedure * _store_procedure;
    // Concurrency control manager (2PL, TicToc, etc.)
    CCManager      * _cc_manager;

    // For remote request, only keep msg.
    Message        * _msg;
    LogRecord      * _log;

    State            _txn_state;
    uint32_t         _num_resp_expected;
    bool             _waiting_log_flush;
    bool             _remote_txn_abort;
    uint64_t         _ready_time;

    bool             _mp_txn;
    uint32_t         _src_node_id; // for sub_query, the src_node is stored.

    // stats
    uint64_t         _txn_start_time;
    uint64_t         _txn_restart_time; // after aborts
    uint64_t         _lock_phase_start_time;
    uint64_t         _prepare_start_time;
    uint64_t *       _msg_count;
    uint64_t *       _msg_size;
    uint32_t         _num_aborts;
    uint64_t         _commit_start_time;
    uint64_t         _finish_time;
    uint64_t         _lock_wait_time;
    uint64_t         _lock_wait_start_time;
    uint64_t         _net_wait_start_time;
    uint64_t         _net_wait_time;
    uint64_t         _log_start_flush_time;
    uint64_t         _log_total_flush_time;

    // for continued prepare.
    uint32_t         _resp_size;
    char *           _resp_data;

    // remote nodes involved in this txn
    set<uint32_t>    remote_parts_involved;
    set<uint32_t>    aborted_remote_parts;
    set<uint32_t>    readonly_remote_parts;
    set<uint32_t>    prepared_remote_parts;
    uint32_t         coord_part;
    set<uint32_t>    local_nodes_involved;
#if BYPASS_LEADER_DECIDE
    vector<uint32_t> num_append_resps;
#endif

#if COMMIT_PROTOCOL == TAPIR || COMMIT_PROTOCOL == GPAC
    enum VoteType {
        VOTE_COMMIT,
        VOTE_ABORT,
        VOTE_NOT_RECEIVED,
    };

    struct VoteStat {
        uint32_t num_commits;
        uint32_t num_aborts;
        VoteType leader_vote;

        VoteStat() {
            num_commits = 0; 
            num_aborts = 0; 
            leader_vote = VOTE_NOT_RECEIVED; 
        }
    };

    vector<VoteStat> partition_votes;
    uint32_t num_undetermined_partitions;
    set<uint32_t> determined_remote_parts;
#endif
};

struct CompareTxn {
    bool operator() (TxnManager * txn1, TxnManager * txn2) const {
        return txn1->get_txn_id() < txn2->get_txn_id();
    }
};

