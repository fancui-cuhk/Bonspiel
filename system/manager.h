#pragma once

#include "helper.h"
#include "global.h"
#include "packetize.h"

class row_t;
class TxnManager;
class workload;
class Transport;

// Global Manager shared by all the threads.
// For distributed version, Manager is shared by all threads on a single node.
class Manager {
public:
    void              init();

    // Global timestamp allocation
    uint64_t          get_ts(uint64_t thread_id);

    // For MVCC. To calculate the min active ts in the system
    void              add_ts(uint64_t ts);
    uint64_t          get_min_ts(uint64_t tid = 0);

    // For DL_DETECT.
    void              set_txn_man(TxnManager * txn);
    TxnManager *      get_txn_man(int thd_id) { return _all_txns[thd_id]; };

    // per-thread random number generator
    void              init_rand(uint64_t thd_id) { srand48_r(thd_id, &_buffer); }
    uint64_t          rand_uint64();
    uint64_t          rand_uint64(uint64_t max);
    uint64_t          rand_uint64(uint64_t min, uint64_t max);
    double            rand_double();

    // thread id
    void              set_thd_id(uint64_t thread_id) { _thread_id = thread_id; }
    uint64_t          get_thd_id() { return _thread_id; }

    // TICTOC, max_cts
    void              set_max_cts(uint64_t cts) { _max_cts = cts; }
    uint64_t          get_max_cts() { return _max_cts; }

    // workload
    void              set_workload(workload * wl)    { _workload = wl; }
    inline workload * get_workload()    { return _workload; }

    // For client server thd id conversion
    uint32_t          txnid_to_server_node(uint64_t txn_id) { return txn_id % g_num_nodes; }
    uint32_t          txnid_to_server_thread(uint64_t txn_id) { return txn_id / g_num_nodes % g_num_server_threads; }
    uint64_t          get_clock_from_ts(uint64_t ts) { return ts / g_num_nodes / g_num_worker_threads / TS_COUNTER_MAX; }

    // global synchronization
    bool              is_sim_done() { return _remote_done && are_all_worker_threads_done(); }
    void              set_remote_done() { _remote_done = true; }
    uint32_t          worker_thread_done() { return ATOM_ADD_FETCH(_num_finished_worker_threads, 1); }
    bool              are_all_worker_threads_done() { return _num_finished_worker_threads == g_num_worker_threads; }

    static uint64_t   max_sub_txn_clock;

    // For Bonspiel
    void              resize_hotness_table(uint32_t num_table, vector<uint64_t> num_row);
    bool              is_hot(uint32_t table_id, uint64_t key);
#if ACCESS_METHOD == HISTOGRAM_3D
    double            get_update_frequency(uint32_t table_id, uint64_t key, uint64_t prio);
    void              update_hotness(uint32_t table_id, uint64_t key, uint64_t prio);
#else
    bool              high_abort_risk(uint32_t table_id, uint64_t key);
    void              update_hotness(uint32_t table_id, uint64_t key);
#endif
    uint32_t          get_local_hotness_table(UnstructuredBuffer & buffer);
    void              apply_remote_hotness_table(uint32_t part_id, UnstructuredBuffer buffer);

    // For Sundial
    bool              read_local();
    void              vote_for_local();
    void              vote_for_remote();

private:
    pthread_mutex_t   ts_mutex;
    uint64_t *        timestamp;
    pthread_mutex_t   mutexes[BUCKET_CNT];
    uint64_t          hash(row_t * row);
    uint64_t volatile * volatile * volatile all_ts;
    TxnManager **     _all_txns;

    bool volatile     _remote_done;
    uint32_t          _num_finished_worker_threads;
    uint32_t          _num_finished_remote_nodes;

    // Each hotness entry is a 64-bit update counter
#if ACCESS_METHOD == HISTOGRAM_3D
    vector<uint32_t>     _hotness_group_num_per_table;
    atomic<uint64_t> *** _local_hotness_table;
    atomic<uint64_t> *** _remote_hotness_table;
#else
    vector<uint32_t>    _hotness_group_num_per_table;
    atomic<uint64_t> ** _local_hotness_table;
    atomic<uint64_t> ** _remote_hotness_table;
#endif

    int32_t             _hotness_msg_ts;
    vector<int32_t>     _remote_hotness_msg_ts;

    // For Sundial
    uint64_t            _vote_local;
    uint64_t            _vote_remote;

    // per-thread random number
    static __thread drand48_data _buffer;

    // thread id
    static __thread uint64_t _thread_id;

    // For TICTOC timestamp
    static __thread uint64_t _max_cts; // max commit timestamp seen by the thread so far

    // thread local transport
    // static __thread Transport * _transport;

    // workload
    workload * _workload;

    uint64_t * _prev_physic_time;
    uint64_t * _prev_logical_time;
};
