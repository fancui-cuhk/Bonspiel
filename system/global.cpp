#include "stats.h"
#include "manager.h"
#include "query.h"
#include "transport.h"
#include "txn_table.h"
#include "log.h"
#include "message.h"
#include <map>

Stats * stats;
Manager * glob_manager;

bool volatile warmup_finish = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t global_barrier;
pthread_mutex_t global_lock;

////////////////////////////
// Global Parameter
////////////////////////////
uint64_t g_abort_penalty         = ABORT_PENALTY;
uint32_t g_ts_alloc              = TS_ALLOC;
bool g_key_order                 = KEY_ORDER;
bool g_ts_batch_alloc            = TS_BATCH_ALLOC;
uint32_t g_ts_batch_num          = TS_BATCH_NUM;
uint32_t g_max_num_active_txns   = MAX_NUM_ACTIVE_TXNS;
double g_warmup_time             = WARMUP_TIME;
double g_run_time                = RUN_TIME;
bool g_prt_lat_distr             = PRT_LAT_DISTR;
uint64_t g_max_clock_skew        = MAX_CLOCK_SKEW;

////////////////////////////
// YCSB
////////////////////////////
uint32_t g_cc_alg                = CC_ALG;
double g_perc_remote             = PERC_REMOTE;
double g_perc_dist_txn           = PERC_DIST_TXN;
double g_perc_remote_dist_txn    = PERC_REMOTE_DIST_TXN;
double g_read_perc               = READ_PERC;
double g_zipf_theta              = ZIPF_THETA;
uint64_t g_synth_table_size      = SYNTH_TABLE_SIZE;
uint32_t g_req_per_query         = REQ_PER_QUERY;
uint32_t g_init_parallelism      = INIT_PARALLELISM;
double g_readonly_perc           = PERC_READONLY_DATA;

////////////////////////////
// TPCC
////////////////////////////
uint32_t g_num_wh                = NUM_WH;
uint32_t g_payment_remote_perc   = PAYMENT_REMOTE_PERC;
uint32_t g_new_order_remote_perc = NEW_ORDER_REMOTE_PERC;
double g_perc_payment            = PERC_PAYMENT;
double g_perc_new_order          = PERC_NEWORDER;
double g_perc_order_status       = PERC_ORDERSTATUS;
double g_perc_delivery           = PERC_DELIVERY;

#if TPCC_SMALL
uint32_t g_max_items = 10000;
uint32_t g_cust_per_dist = 2000;
#else
uint32_t g_max_items = 100000;
uint32_t g_cust_per_dist = 3000;
#endif

////////////////////////////
// TPCE
////////////////////////////
double g_perc_broker_volume      = PERC_BROKER_VOLUME;
double g_perc_customer_position  = PERC_CUSTOMER_POSITION;
double g_perc_market_feed        = PERC_MARKET_FEED;
double g_perc_market_watch       = PERC_MARKET_WATCH;
double g_perc_security_detail    = PERC_SECURITY_DETAIL;
double g_perc_trade_lookup       = PERC_TRADE_LOOKUP;
double g_perc_trade_order        = PERC_TRADE_ORDER;
double g_perc_trade_result       = PERC_TRADE_RESULT;
double g_perc_trade_status       = PERC_TRADE_STATUS;
double g_perc_trade_update       = PERC_TRADE_UPDATE;

uint32_t g_tpce_perc_remote      = TPCE_PERC_REMOTE;

uint32_t g_customer_num          = 100000;
uint32_t g_account_num           = 200000;
uint32_t g_broker_num            = 10000;
uint32_t g_security_num          = 100000;
uint32_t g_company_num           = 1000;
uint32_t g_init_trade_num        = 2000000;

////////////////////////////
// TATP
////////////////////////////
uint64_t g_tatp_population       = TATP_POPULATION;

char * output_file = NULL;
char ifconfig_file[80] = "ifconfig.txt";

// TICTOC
uint32_t g_max_num_waits         = MAX_NUM_WAITS;

// DOCC
uint32_t g_num_hotness_threads   = NUM_HOTNESS_THREADS;
uint32_t g_hotness_interval      = HOTNESS_INTERVAL;
uint32_t g_hotness_threshold     = HOTNESS_THRESHOLD;
double g_bonspiel_threshold      = BONSPIEL_THRESHOLD;
uint32_t g_hotness_group_size    = HOTNESS_GROUP_SIZE;
double g_heuristic_perc          = HEURISTIC_PERC;

// SUNDIAL
double g_local_threshold         = LOCAL_THRESHOLD;

// CALVIN
double g_epoch_size              = EPOCH_SIZE;

//////////////////////////////////////////////////
// Distributed DBMS
//////////////////////////////////////////////////

uint32_t g_sequencer_threads     = 0;
uint32_t g_num_worker_threads    = 0;
uint32_t g_num_server_threads    = NUM_SERVER_THREADS;
uint32_t g_total_num_threads     = 0;

uint32_t g_num_nodes             = 0;
uint32_t g_num_client_nodes      = 0;
uint32_t g_num_parts             = 0;

uint32_t g_node_id;
uint32_t g_part_id;

bool g_is_client_node            = false;

uint32_t g_num_input_threads     = NUM_INPUT_THREADS;
uint32_t g_num_output_threads    = NUM_OUTPUT_THREADS;
uint32_t g_num_raft_instances    = NUM_RAFT_INSTANCES;

Transport * transport;
InOutQueue ** input_queues;
InOutQueue ** output_queues;
WaitQueue ** ready_queues;
LogQueue ** raft_log_queues;
InOutQueue ** raft_msg_queues;
InOutQueue ** hotness_msg_queues;
LogApplyQueue ** log_apply_queues;
ServerThread ** server_threads;
map<uint64_t, TxnManager *> * txn_tables;

bool calvin_new_batch_ready;
uint64_t calvin_txn_remaining;
std::vector<TxnManager *> calvin_txn_list;
std::set<uint32_t> calvin_part_recv;

uint32_t g_dummy_size = 0;
uint32_t num_terminated_nodes = 0;
std::vector<std::string> g_urls;
std::vector<std::pair<int,std::string>> g_replicas;
Role g_role;
std::string g_hostname;

#if DEBUG_TXN_RESULT && WORKLOAD == BANK
int64_t * balances[NUM_SERVER_THREADS];
#endif

#if DEBUG_MSG_WAIT_INQUEUE
MsgArrivalQueue * msg_arrival_times;
#endif
