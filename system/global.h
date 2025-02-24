#pragma once

#include "stdint.h"
#include <iomanip>
#include <unistd.h>
#include <cstddef>
#include <cstdlib>
#include <cassert>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string.h>
#include <typeinfo>
#include <list>
#include <mm_malloc.h>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <sstream>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <boost/lockfree/queue.hpp>
#include "pthread.h"
#include "utils/ringbuffer.h"
#include "numa.h"
#include "config.h"
#include "stats.h"

using namespace std;

class Stats;
class DL_detect;
class Manager;
class Query_queue;
class Plock;
class VLLMan;
class TxnTable;
class Transport;
class FreeQueue;
class CacheManager;
class IndexHash;
class ServerThread;
class LogManager;
class LogRecord;
class LogResp;
class Message;
class TxnManager;

typedef uint32_t UInt32;
typedef int32_t SInt32;
typedef uint64_t UInt64;
typedef int64_t SInt64;

typedef uint64_t ts_t; // time stamp type

/******************************************/
// Global Data Structure
/******************************************/
extern Stats * stats;
extern Manager * glob_manager;

extern bool volatile warmup_finish;
extern bool volatile enable_thread_mem_pool;
extern pthread_barrier_t global_barrier;
extern pthread_mutex_t global_lock;

/******************************************/
// Global Parameter
/******************************************/
extern uint32_t g_sequencer_threads;
extern bool g_prt_lat_distr;
extern uint32_t g_num_worker_threads;
extern uint32_t g_num_server_threads;
//extern uint32_t g_num_remote_threads;

extern uint32_t g_total_num_threads;
extern ts_t g_abort_penalty;
extern uint32_t g_ts_alloc;
extern bool g_key_order;
extern bool g_ts_batch_alloc;
extern uint32_t g_ts_batch_num;
extern uint32_t g_max_num_active_txns;
extern double g_warmup_time;
extern double g_run_time;
extern uint64_t g_max_clock_skew;

// TICTOC
extern uint32_t g_max_num_waits;
extern uint64_t g_local_cache_size;
extern double g_read_intensity_thresh;

// DOCC
extern uint32_t g_num_hotness_threads;
extern uint32_t g_hotness_interval;
extern uint32_t g_hotness_threshold;
extern double g_bonspiel_threshold;
extern uint32_t g_hotness_group_size;
extern double g_heuristic_perc;

// SUNDIAL
extern double g_local_threshold;

// CALVIN
extern double g_epoch_size;

////////////////////////////
// YCSB
////////////////////////////
extern uint32_t g_cc_alg;
extern double g_perc_remote;
extern double g_perc_dist_txn;
extern double g_perc_remote_dist_txn;
extern double g_read_perc;
extern double g_zipf_theta;
extern uint64_t g_synth_table_size;
extern uint32_t g_req_per_query;
extern uint32_t g_init_parallelism;
extern double g_readonly_perc;

////////////////////////////
// TPCC
////////////////////////////
extern uint32_t g_num_wh;
extern double g_perc_payment;
extern uint32_t g_max_items;
extern uint32_t g_cust_per_dist;
extern uint32_t g_payment_remote_perc;
extern uint32_t g_new_order_remote_perc;
extern double g_perc_payment;
extern double g_perc_new_order;
extern double g_perc_order_status;
extern double g_perc_delivery;

////////////////////////////
// TPCE
////////////////////////////
extern double g_perc_broker_volume;
extern double g_perc_customer_position;
extern double g_perc_market_feed;
extern double g_perc_market_watch;
extern double g_perc_security_detail;
extern double g_perc_trade_lookup;
extern double g_perc_trade_order;
extern double g_perc_trade_result;
extern double g_perc_trade_status;
extern double g_perc_trade_update;

extern uint32_t g_tpce_perc_remote;

extern uint32_t g_customer_num;
extern uint32_t g_account_num;
extern uint32_t g_broker_num;
extern uint32_t g_security_num;
extern uint32_t g_company_num;
extern uint32_t g_init_trade_num;

////////////////////////////
// TATP
////////////////////////////
extern uint64_t g_tatp_population;

extern char * output_file;
extern char ifconfig_file[];

enum RC {RCOK, COMMIT, ABORT, WAIT, LOCAL_MISS, SPECULATE, ERROR, FINISH};

// INDEX
enum latch_t {LATCH_EX, LATCH_SH, LATCH_NONE};
// accessing type determines the latch type on nodes
enum idx_acc_t {INDEX_INSERT, INDEX_READ, INDEX_NONE};

// LOOKUP, INS and DEL are operations on indexes.
enum access_t {NA, RD, WR, PWR, IDXRD, XP, SCAN, INS, DEL};

// TIMESTAMP
enum TsType {R_REQ, W_REQ, P_REQ, XP_REQ};
enum Isolation {SR, SI, RR, NO_ACID};

enum Role { ROLE_LEADER, ROLE_REPLICA };

#define MSG(str, args...) { \
    printf("[%s : %d] " str, __FILE__, __LINE__, args); } \

// principal index structure. The workload may decide to use a different
// index structure for specific purposes. (e.g. non-primary key access should use hash)
#if INDEX_STRUCT == IDX_BTREE
#define INDEX        index_btree
#else  // IDX_HASH
#define INDEX        IndexHash
#endif

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == WOUND_WAIT || CC_ALG == CALVIN
    class Row_lock;
    class LockManager;
    #define ROW_MAN Row_lock
    #define CC_MAN LockManager
#elif CC_ALG == TICTOC
    class Row_tictoc;
    class TicTocManager;
    #define ROW_MAN Row_tictoc
    #define CC_MAN TicTocManager
#elif CC_ALG == SILO
    class Row_silo;
    class SiloManager;
    #define ROW_MAN Row_silo
    #define CC_MAN SiloManager
#elif CC_ALG == DOCC
    class Row_docc;
    class DoccManager;
    #define ROW_MAN Row_docc
    #define CC_MAN DoccManager
#elif CC_ALG == SUNDIAL
    class Row_sundial;
    class SundialManager;
    #define ROW_MAN Row_sundial
    #define CC_MAN SundialManager
#endif
/************************************************/
// constants
/************************************************/
#ifndef UINT64_MAX
#define UINT64_MAX         18446744073709551615UL
#endif // UINT64_MAX

//////////////////////////////////////////////////
// Distributed DBMS
//////////////////////////////////////////////////
extern uint32_t g_num_nodes;
extern uint32_t g_num_parts;
extern uint32_t g_node_id;
extern uint32_t g_part_id;

extern uint32_t g_num_input_threads;
extern uint32_t g_num_output_threads;
extern uint32_t g_num_raft_instances;

extern Transport * transport;

typedef boost::lockfree::queue<Message *> InOutQueue;
typedef boost::lockfree::queue<uint64_t> WaitQueue;
typedef boost::lockfree::queue<LogRecord *> LogQueue;
typedef boost::lockfree::queue<LogRecord *> LogApplyQueue;

extern InOutQueue ** input_queues;
extern InOutQueue ** output_queues;
extern WaitQueue ** ready_queues;
extern LogQueue ** raft_log_queues;
extern InOutQueue ** raft_msg_queues;
extern InOutQueue ** hotness_msg_queues;
extern LogApplyQueue ** log_apply_queues;
extern ServerThread ** server_threads;

extern bool calvin_new_batch_ready;
extern uint64_t calvin_txn_remaining;
extern std::vector<TxnManager *> calvin_txn_list;
extern std::set<uint32_t> calvin_part_recv;

extern uint32_t num_terminated_nodes;
extern std::vector<std::string> g_urls;
extern std::vector<std::pair<int,std::string>> g_replicas;
extern Role g_role;
extern std::string g_hostname;

#if DEBUG_TXN_RESULT && WORKLOAD == BANK
extern int64_t * balances[NUM_SERVER_THREADS];
#endif

#if DEBUG_MSG_WAIT_INQUEUE
typedef boost::lockfree::queue<uint64_t> MsgArrivalQueue;
extern MsgArrivalQueue * msg_arrival_times;
#endif
