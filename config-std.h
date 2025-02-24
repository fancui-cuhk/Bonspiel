#pragma once

// number of server threads on each node
#define NUM_SERVER_THREADS 12

// max number of native txns on a server
#define MAX_NUM_NATIVE_TXNS 500

// CPU_FREQ is used to get accurate timing info
// We assume all nodes have the same CPU frequency.
#define CPU_FREQ 2.10    // in GHz/s

// warmup time in seconds, not in use
#define WARMUP 0
// WORKLOAD can be TPCE, YCSB, TPCC
// Right now TPCC has bugs, BANK is not implemented yet.
#define WORKLOAD YCSB

// txn generation: FIX_GEN_RATIO or FIX_ACTIVE_RATIO
#define TXN_GEN FIX_GEN_RATIO

// global MP ratio - only in use under FIX_ACTIVE_RATIO
#define GLOBAL_MP_RATIO 0.1

// Statistics
// ==========
// COLLECT_LATENCY: when set to true, will collect transaction latency information
#define COLLECT_LATENCY true
// PRT_LAT_DISTR: when set to true, will print transaction latency distribution
#define PRT_LAT_DISTR false
#define STATS_ENABLE true
#define TIME_ENABLE true
#define STATS_CP_INTERVAL 1000 // in ms

// Concurrency Control
// ===================
// Supported concurrency control algorithms: DOCC, WAIT_DIE, NO_WAIT, WOUND_WAIT, SUNDIAL, CALVIN
// CALVIN is not ready yet
#define CC_ALG DOCC
#define ISOLATION_LEVEL SERIALIZABLE

// KEY_ORDER: when set to true, each transaction accesses tuples in the primary key order.
// Only effective when WORKLOAD == YCSB.
#define KEY_ORDER false

// per-row lock/ts management or central lock/ts management
#define BUCKET_CNT 31
#define ABORT_PENALTY 1000000
#define MAX_BACKOFF_TIMES 6
// [ INDEX ]
#define ENABLE_LATCH false
#define CENTRAL_INDEX false
#define CENTRAL_MANAGER false
#define INDEX_STRUCT IDX_HASH
#define BTREE_ORDER 16

// [Two Phase Locking]
#define NO_LOCK false // NO_LOCK=true : used to model H-Store
#define PRIORITY_LOCK true // PRIORITY_LOCK=true : dist txns' locks have higher priority
// [TIMESTAMP]
#define TS_ALLOC TS_HYBRID
#define TS_BATCH_ALLOC false
#define TS_BATCH_NUM 1
#define TS_COUNTER_MAX 1000u
#define TS_SCALE 1000000lu // to avoid timestamp overflow
// [MVCC]
#define MIN_TS_INTVL 5000000 //5 ms. In nanoseconds
// [DOCC]
#define ACCESS_METHOD BONSPIEL
#define HOTNESS_REQUIRED (ACCESS_METHOD == BONSPIEL || ACCESS_METHOD == HOTNESS_ONLY || ACCESS_METHOD == HISTOGRAM_3D)
#define HOTNESS_ENABLE (CC_ALG == DOCC && HOTNESS_REQUIRED)
#define HOTNESS_GROUP_SIZE 1000
#define HOTNESS_THRESHOLD 200
#define BONSPIEL_THRESHOLD 0.01
#define MULTIPLE_PRIORITY_LEVELS false
#define ABORT_AFTER_WRITE_RESERVATION_FAIL 0
#define PRIORITY_BOOST_THRESHOLD 2
#define HOTNESS_INTERVAL 5 // in seconds
#define NUM_HOTNESS_THREADS 1
#define HEURISTIC_PERC 0.2
#define MAX_WRITE_SET 20
#define PER_ROW_VALID true
#define EARLY_WRITE_INSTALL false
#define BONSPIEL_NO_RESER false
#define BONSPIEL_NO_AMS false
// [TICTOC]
#define WRITE_COPY_FORM "data" // ptr or data
#define TICTOC_MV false
#define WR_VALIDATION_SEPARATE true
#define WRITE_PERMISSION_LOCK false
#define ATOMIC_TIMESTAMP "false"
// [SUNDIAL]
#define LOCAL_THRESHOLD 0.8
// [CALVIN]
#define EPOCH_SIZE 0.5 // in seconds

// when WAW_LOCK is true, lock a tuple before write.
// essentially, WW conflicts are handled as 2PL.
#define OCC_WAW_LOCK                  false
// if SKIP_READONLY_PREPARE is true, then a readonly subtxn will forget
// about its states after returning. If no renewal is required, this remote
// node will not participate in the 2PC protocol.
#define SKIP_READONLY_PREPARE         false
#define MAX_NUM_WAITS                 50
#define READ_INTENSITY_THRESH         0.8

#define LOCK_ALL_BEFORE_COMMIT        false
#define LOCK_ALL_DEBUG                false
#define TRACK_LAST                    false
#define LOCK_TRIAL                    3
#define MULTI_VERSION                 false
// [TICTOC, SILO, DOCC]
// EARLY_WRITE_INSTALL only supports NO_WAIT now
#define OCC_LOCK_TYPE                 NO_WAIT
#define PRE_ABORT                     true
#define ATOMIC_WORD                   false
#define UPDATE_TABLE_TS               true
// [HSTORE]
// when set to true, hstore will not access the global timestamp.
// This is fine for single partition transactions.
#define HSTORE_LOCAL_TS               false
// [VLL]
#define TXN_QUEUE_SIZE_LIMIT          THREAD_CNT

////////////////////////////////////////////////////////////////////////
// Logging
////////////////////////////////////////////////////////////////////////
#define LOG_ENABLE                    true
#define LOG_COMMAND                   false
#define LOG_REDO                      false
#define LOG_BATCH_TIME                10 // in ms

// RAFT
#define NUM_RAFT_INSTANCES            1
#define RAFT_ELECTION_TIMEOUT         1000
#define RAFT_PERIODIC                 100000000
#define RAFT_WORK_PER_LOOP            200
#define PING_INTERVAL                 (10 * MILLION)
#define LATENCY_LOW_PASS              0.2

////////////////////////////////////////////////////////////////////////
// Benchmark
////////////////////////////////////////////////////////////////////////
// max number of rows touched per transaction
#define WARMUP_TIME                   0 // in seconds
#define RUN_TIME                      5 // in seconds
#define MAX_TUPLE_SIZE                1024 // in bytes
#define INIT_PARALLELISM              40

///////////////////////////////
// YCSB
///////////////////////////////
// Number of tuples per node
#define SYNTH_TABLE_SIZE              (10000000)
#define ZIPF_THETA                    0.5
#define READ_PERC                     0.5
#define PERC_READONLY_DATA            0
// PERC_REMOTE not in use now
#define PERC_REMOTE                   0.1
#define PERC_DIST_TXN                 0.1
#define PERC_REMOTE_DIST_TXN          1
#define SINGLE_PART_ONLY              false // access single partition only
#define REQ_PER_QUERY                 10
#define THINK_TIME                    0  // in us
#define SOCIAL_NETWORK                false

///////////////////////////////
// TPCC
///////////////////////////////
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL false
#define NUM_WH 32
// TODO. REPLICATE_ITEM_TABLE = false only works for TICTOC.
#define REPLICATE_ITEM_TABLE          true
#define PERC_PAYMENT                  0.5
#define PERC_NEWORDER                 0.5
#define PERC_ORDERSTATUS              0
#define PERC_DELIVERY                 0
#define PERC_STOCKLEVEL               0
#define PAYMENT_REMOTE_PERC           15 // 15% customers are remote
#define NEW_ORDER_REMOTE_PERC         1  // 1% order lines are remote
#define FIRSTNAME_MINLEN              8
#define FIRSTNAME_LEN                 16
#define LASTNAME_LEN                  16
#define DIST_PER_WARE                 10

///////////////////////////////
// TPCE
///////////////////////////////
#define PERC_BROKER_VOLUME            0.049
#define PERC_CUSTOMER_POSITION        0.13
#define PERC_MARKET_FEED              0.01
#define PERC_MARKET_WATCH             0.18
#define PERC_SECURITY_DETAIL          0.14
#define PERC_TRADE_LOOKUP             0.08
#define PERC_TRADE_ORDER              0.101
#define PERC_TRADE_RESULT             0.10
#define PERC_TRADE_STATUS             0.19
#define PERC_TRADE_UPDATE             0.02

#define TPCE_PERC_REMOTE              10

///////////////////////////////
// TATP
///////////////////////////////
// Number of subscribers per node.
#define TATP_POPULATION               100000

///////////////////////////////
// BANK
///////////////////////////////
#define BANK_NUM_KEYS                 3
#define INIT_BALANCE                  100
#define MAX_TRANSFER                  1
#define BANK_REMOTE_PERC              0.1d

////////////////////////////////////////////////////////////////////////
// TODO centralized CC management.
////////////////////////////////////////////////////////////////////////
#define MAX_LOCK_CNT                  (20 * THREAD_CNT)
#define TSTAB_SIZE                    50 * THREAD_CNT
#define TSTAB_FREE                    TSTAB_SIZE
#define TSREQ_FREE                    4 * TSTAB_FREE
#define MVHIS_FREE                    4 * TSTAB_FREE
#define SPIN                          false

////////////////////////////////////////////////////////////////////////
// Test cases
////////////////////////////////////////////////////////////////////////
#define TEST_ALL                      true
enum TestCases {
    READ_WRITE,
    CONFLICT
};
extern TestCases                      g_test_case;

////////////////////////////////////////////////////////////////////////
// DEBUG info
////////////////////////////////////////////////////////////////////////
#define WL_VERB                       true
#define IDX_VERB                      false
#define VERB_ALLOC                    true

#define DEBUG_LOCK                    false
#define DEBUG_TIMESTAMP               false
#define DEBUG_SYNTH                   false
#define DEBUG_ASSERT                  false
#define DEBUG_CC                      false
#define DEBUG_MSG_WAIT_INQUEUE        false

////////////////////////////////////////////////////////////////////////
// Constant
////////////////////////////////////////////////////////////////////////
// Txn generation
#define FIX_GEN_RATIO                 1
#define FIX_ACTIVE_RATIO              2
// Access method
#define READ_NEAREST                  1
#define READ_LEADER                   2
#define READ_RESERVE_LEADER           3
#define HEURISTIC                     4
#define HOTNESS_ONLY                  5
#define BONSPIEL                      6
#define HISTOGRAM_3D                  7
// Index structure
#define IDX_HASH                      1
#define IDX_BTREE                     2
// WORKLOAD
#define YCSB                          1
#define TPCC                          2
#define TPCE                          3
#define BANK                          4
// Concurrency Control Algorithm
#define NO_WAIT                       1
#define WAIT_DIE                      2
#define WOUND_WAIT                    3
#define TICTOC                        4
#define SILO                          5
#define DOCC                          6
#define SUNDIAL                       7
#define CALVIN                        8
// Isolation Levels
#define SERIALIZABLE                  1
#define SNAPSHOT                      2
#define REPEATABLE_READ               3
// TIMESTAMP allocation method.
#define TS_MUTEX                      1
#define TS_CAS                        2
#define TS_HW                         3
#define TS_CLOCK                      4
#define TS_HYBRID                     5
// Commit protocol
#define RAFT_2PC                      1
#define TAPIR                         2
#define GPAC                          3

/***********************************************/
// Distributed DBMS
/***********************************************/
#define START_PORT 45777
#define INOUT_QUEUE_SIZE 1024
#define NUM_INPUT_THREADS 5
#define NUM_OUTPUT_THREADS 5
#define MAX_NUM_ACTIVE_TXNS (MAX_NUM_NATIVE_TXNS * NUM_SERVER_THREADS + 128)
#define ENABLE_MSG_BUFFER true
#if WORKLOAD == TPCC || CC_ALG == CALVIN
#define MAX_MESSAGE_SIZE (8192 * 8192 * 4)
#define RECV_BUFFER_SIZE (8192 * 8192 * 16)
#define SEND_BUFFER_SIZE (8192 * 8192 * 16)
#else
#define MAX_MESSAGE_SIZE (1024 * 200)
#define RECV_BUFFER_SIZE (1024 * 1024)
#define SEND_BUFFER_SIZE (1024 * 1024)
#endif
#define NUM_SEND_BUFFER_SEGS 1024
#define BUFFER_SEND_TIME_THREAS (200 * 1000)
#define MODEL_DUMMY_MSG false

#define MAX_CLOCK_SKEW 0 // in us

#define RERUN_ABORT true
#define MAX_ABORT_NUMS 100
#define DEBUG_TXN_RESULT false

#define SET_AFFINITY true
#define PRECISE_LOCK false
#define TEST_RAFT false

// #define NUM_TEST_TX 10

#define COMMIT_PROTOCOL RAFT_2PC

// optimization modules
#define BYPASS_COMMIT_CONSENSUS true
#define BYPASS_COORD_CONSENSUS true
#define BYPASS_LEADER_DECIDE true
