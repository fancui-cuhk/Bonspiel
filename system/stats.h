#pragma once
#include "global.h"
#include "helper.h"
#include <utility>

using std::vector;
using std::pair;

enum StatsFloat {
    // Worker Thread
    STAT_run_time,
    STAT_txn_latency,

    STAT_time_read_input_queue,
    STAT_time_process_txn,
    STAT_time_waiting_for_job,
    STAT_time_wait_buffer,
    STAT_time_abort_queue,
    STAT_time_idle,
    STAT_cross_dc_sent,
    STAT_time_write_msg,
    STAT_time_write_output_queue,
    STAT_time_msg_wait_inqueue,

    // Logging
    STAT_time_log_apply,
    STAT_log_size,

    // Output Thread
    STAT_bytes_sent,
    STAT_dummy_bytes_sent,
    STAT_time_send_msg,
    STAT_time_read_queue, // read output_queue
    STAT_time_output_idle,

    // Input Thread
    STAT_bytes_received,
    STAT_time_recv_msg,
    STAT_time_write_queue, // write input_queue
    STAT_time_input_idle,

    // Raft Thread
    STAT_raft_time_periodic,
    STAT_raft_time_apply,
    STAT_raft_time_append,
    STAT_raft_time_msg,

    // txn lifetime breakdown
    STAT_execute_phase,
    STAT_lock_phase,
    STAT_prepare_phase,
    STAT_commit_phase,
    STAT_abort,

    STAT_row,
    STAT_index,
    STAT_logic,
    STAT_wait,
    STAT_network,
    STAT_cache,
    STAT_time_write_log,

    // debug stats
    STAT_time_debug1,
    STAT_time_debug2,
    STAT_time_debug3,
    STAT_time_debug4,
    STAT_time_debug5,
    STAT_time_debug6,
    STAT_time_debug7,

    NUM_FLOAT_STATS
};

enum StatsInt {
    STAT_num_commits,
    STAT_num_aborts,
    STAT_num_waits,

    STAT_num_home_txn, // txn mapped to this node.
    STAT_num_remote_txn,
    STAT_num_remote_commits,
    STAT_num_home_sp,
    STAT_num_home_mp,
    STAT_num_home_sp_commits,
    STAT_num_home_mp_commits,

    // For TicToc
    // Abort breakdown
    STAT_num_aborts_ws,
    STAT_num_aborts_rs,
    STAT_num_aborts_signal,
    STAT_num_aborts_remote,
    STAT_num_aborts_execute,
    STAT_num_pre_aborts,

    STAT_num_aborts_restart,
    STAT_num_aborts_terminate,

    STAT_num_renewals,
    STAT_num_no_need_to_renewal,

    // For local caching
    STAT_num_cache_bypass,
    STAT_num_cache_reads,
    STAT_num_cache_hits,
    STAT_num_cache_misses,
    STAT_num_cache_remove,
    STAT_num_cache_inserts,
    STAT_num_cache_updates,
    STAT_num_cache_evictions,

    STAT_num_local_hits,
    STAT_num_renew,
    STAT_num_renew_success,
    STAT_num_renew_failure,

    // for READ_INTENSIVE
    STAT_num_ro_read,
    STAT_num_ro_check,
    STAT_num_rw_read,
    STAT_num_rw_check,

    STAT_int_debug1,
    STAT_int_debug2,
    STAT_int_debug3,
    STAT_int_debug4,
    STAT_int_debug5,
    STAT_int_debug6,

    STAT_dist_txn_local_abort,
    STAT_dist_txn_2pc_abort,
    STAT_remote_txn_lock_write_set_abort,
    STAT_remote_txn_validation_abort,
    STAT_remote_txn_val_rd_abort,
    STAT_remote_txn_val_wr_abort,

    STAT_dist_txn_abort_local,
    STAT_dist_txn_abort_dist,
    STAT_local_txn_abort_local,
    STAT_local_txn_abort_dist,

    STAT_nearest_access,
    STAT_leader_access,
    STAT_leader_reserve,

    STAT_int_urgentwrite,
    STAT_int_aborts_rs1,
    STAT_int_aborts_rs2,
    STAT_int_aborts_rs3,
    STAT_int_aborts_ws1,
    STAT_int_aborts_ws2,
    STAT_int_inevitable,
    STAT_int_possibMVCC,

    STAT_int_saved_by_hist,

    NUM_INT_STATS
};

enum StatsFloatSP {
    // Worker Thread
    STAT_sp_txn_latency,

    // txn lifetime breakdown
    STAT_sp_execute_phase,
    STAT_sp_lock_phase,
    STAT_sp_prepare_phase,
    STAT_sp_commit_phase,
    STAT_sp_abort,

    STAT_sp_wait,
    STAT_sp_network,

    STAT_sp_retry,
    NUM_SP_FLOAT_STATS
};

enum StatsFloatMP {
    // Worker Thread
    STAT_mp_txn_latency,

    // txn lifetime breakdown
    STAT_mp_execute_phase,
    STAT_mp_lock_phase,
    STAT_mp_prepare_phase,
    STAT_mp_commit_phase,
    STAT_mp_abort,

    STAT_mp_wait,
    STAT_mp_network,

    STAT_mp_retry,
    NUM_MP_FLOAT_STATS
};

enum StatsFloatRemote {
    // Worker Thread
    STAT_remote_wait,
    STAT_remote_prepare,
    STAT_remote_commit,
    NUM_REMOTE_FLOAT_STATS
};

struct LatencyEntry {
    double   latency;
    double   execution_latency;
    double   prepare_latency;
    double   commit_latency;
    double   abort;
    double   network;
    uint32_t abort_count;

    bool operator< (const LatencyEntry & other) const {
        return latency < other.latency;
    }
};

class Stats_thd {
public:
    Stats_thd();
    void copy_from(Stats_thd * stats_thd);

    void init(uint64_t thd_id);
    void clear();

    double * _float_stats;
    double * _sp_float_stats;
    double * _mp_float_stats;
    double * _remote_float_stats;
    uint64_t * _int_stats;

#if COLLECT_LATENCY
    vector<LatencyEntry> all_latency;
    vector<LatencyEntry> sp_latency;
    vector<LatencyEntry> mp_latency;
#endif
    uint64_t * _msg_count;
    uint64_t * _msg_size;
    uint64_t * _msg_committed_count;
    uint64_t * _msg_committed_size;
#if WORKLOAD == TPCC
    uint64_t _commits_per_txn_type[5];
    uint64_t _aborts_per_txn_type[5];
    uint64_t _latency_per_txn_type[5];
#elif WORKLOAD == TPCE
    // SP & MP stats
    uint64_t _commits_per_txn_type[10];
    uint64_t _aborts_per_txn_type[10];
    uint64_t _latency_per_txn_type[10];
    uint64_t _execution_phase_per_txn_type[10];
    uint64_t _prepare_phase_per_txn_type[10];
    uint64_t _commit_phase_per_txn_type[10];
    uint64_t _abort_time_per_txn_type[10];

    // MP stats
    uint64_t _mp_commits_per_txn_type[10];
    uint64_t _mp_aborts_per_txn_type[10];
    uint64_t _mp_latency_per_txn_type[10];
    uint64_t _mp_execution_phase_per_txn_type[10];
    uint64_t _mp_prepare_phase_per_txn_type[10];
    uint64_t _mp_commit_phase_per_txn_type[10];
    uint64_t _mp_abort_time_per_txn_type[10];
#endif
};

class Stats {
public:
    Stats();
    // PER THREAD statistics
    Stats_thd ** _stats;

    // GLOBAL statistics
    double dl_detect_time;
    double dl_wait_time;
    uint64_t cycle_detect;
    uint64_t deadlock;

    // output thread
    uint64_t bytes_sent;
    uint64_t bytes_recv;

    double last_cp_bytes_sent(double &dummy_bytes);
    void init();
    void init(uint64_t thread_id);
    void clear(uint64_t tid);
    void print();
    void print_lat_distr();

    void checkpoint();
    void copy_from(Stats * stats);

    void output(std::ostream * os);

    std::string statsFloatName[NUM_FLOAT_STATS] = {
        // worker thread
        "run_time",
        "average_latency",

        "time_read_input_queue",
        "time_process_txn",
        "time_waiting_for_job",
        "time_wait_buffer",
        "time_abort_queue",
        "time_idle",
        "time_write_output_queue",
        "time_msg_wait_inqueue",

        // Logging
        "time_log_apply",
        "log_size",

        // output thread
        "bytes_sent",
        "dummy_bytes_sent",
        "time_send_msg",
        "time_read_queue",
        "time_output_idle",

        // Input Thread
        "bytes_received",
        "time_recv_msg",
        "time_write_queue",
        "time_input_idle",

        // Raft Thread
        "raft_time_periodic",
        "raft_time_apply",
        "raft_time_append",
        "raft_time_msg",

        // txn lifetime breakdown
        "execute_phase",
        "lock_phase",
        "prepare_phase",
        "commit_phase",
        "abort",

        "CC (row)",
        "CC (index)",
        "logic",
        "wait",
        "network",
        "cache",
        "time_write_log",

        // debug
        "time_debug1",
        "time_debug2",
        "time_debug3",
        "time_debug4",
        "time_debug5",
        "time_debug6",
        "time_debug7",
    };

    std::string statsIntName[NUM_INT_STATS] = {
        "num_commits",
        "num_aborts",
        "num_waits",

        "num_home_txn",
        "num_remote_txn",
        "num_remote_commits",
        "num_home_sp_txn",
        "num_home_mp_txn",
        "num_home_sp_commits",
        "num_home_mp_commits",

        // TicToc abort breakdown
        "num_aborts_ws",
        "num_aborts_rs",
        "num_aborts_signal",
        "num_aborts_remote",
        "num_aborts_execute",
        "num_pre_aborts",

        "num_aborts_restart",
        "num_aborts_terminate",

        "num_renewals",
        "num_no_need_to_renewal",

        // For local caching
        "num_cache_bypass",
        "num_cache_reads",

        "num_cache_hits",
        "num_cache_misses",
        "num_cache_remove",
        "num_cache_inserts",
        "num_cache_updates",
        "num_cache_evictions",

        "num_local_hits",
        "num_renew",
        "num_renew_success",
        "num_renew_failure",

        // for READ_INTENSIVE
        "num_ro_read",
        "num_ro_check",
        "num_rw_read",
        "num_rw_check",

        "int_debug1",
        "int_debug2",
        "int_debug3",
        "int_debug4",
        "int_debug5",
        "int_debug6",

        "dist_txn_local_abort",
        "dist_txn_2pc_abort",
        "remote_txn_lock_write_set_abort",
        "remote_txn_validation_abort",
        "remote_txn_val_rd_abort",
        "remote_txn_val_wr_abort",

        "dist_txn_abort_local",
        "dist_txn_abort_dist",
        "local_txn_abort_local",
        "local_txn_abort_dist",

        "nearest_access",
        "leader_access",
        "leader_reserve",

        "int_urgentwrite",

        "int_aborts_rs1",
        "int_aborts_rs2",
        "int_aborts_rs3",
        "int_aborts_ws1",
        "int_aborts_ws2",
        "int_inevitable",
        "int_possibMVCC",

        "int_saved_by_hist"
    };

    std::string statsFloatSPName[NUM_SP_FLOAT_STATS] = {
        // worker thread
        "sp_average_latency",

        // txn lifetime breakdown
        "sp_execute_phase",
        "sp_lock_phase",
        "sp_prepare_phase",
        "sp_commit_phase",
        "sp_abort",

        "sp_wait",
        "sp_network",
        "sp_num_retry"
    };

    std::string statsFloatMPName[NUM_MP_FLOAT_STATS] = {
        // worker thread
        "mp_average_latency",

        // txn lifetime breakdown
        "mp_execute_phase",
        "mp_lock_phase",
        "mp_prepare_phase",
        "mp_commit_phase",
        "mp_abort",

        "mp_wait",
        "mp_network",
        "mp_num_retry"
    };

    std::string statsFloatRemoteName[NUM_REMOTE_FLOAT_STATS] = {
        // worker thread
        "remote_wait",
        "remote_prepare",
        "remote_commit"
    };

private:
    vector<LatencyEntry> _agg_latency;
    vector<LatencyEntry> _agg_sp_latency;
    vector<LatencyEntry> _agg_mp_latency;

    vector<Stats *>      _checkpoints;
    uint32_t             _num_cp;
};
