#include <sched.h>
#include <iomanip>
#include "global.h"
#include "manager.h"
#include "server_thread.h"
#include "txn.h"
#include "store_procedure.h"
#include "workload.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "message.h"
#include "txn_table.h"
#include "transport.h"
#include "cc_manager.h"
#include "raft_thread.h"

ServerThread::ServerThread(uint64_t thd_id)
    : Thread(thd_id, WORKER_THREAD)
{
    _msg = NULL;
    _new_txn = NULL;
    _txn_man = NULL;
    _max_txn_id = 1;
    already_printed_debug = false;

    // used in txn generation (FIX_ACTIVE_RATIO)
    _num_sp_to_gen = MAX_NUM_NATIVE_TXNS * (1 - GLOBAL_MP_RATIO);
    _num_mp_to_gen = MAX_NUM_NATIVE_TXNS - _num_sp_to_gen;
}

RC ServerThread::run() {
    glob_manager->init_rand( get_thd_id() );
    glob_manager->set_thd_id( get_thd_id() );
#if SET_AFFINITY
    pin_thread_by_id(pthread_self(), _thd_id);
#endif
    pthread_barrier_wait( &global_barrier );

    RC           rc = RCOK;
    uint64_t     init_time = get_sys_clock();
    bool         sim_done = false;
    bool         terminate_sent = false;

    uint64_t     last_stats_cp_time = init_time;
    Message    * msg = NULL;
    TxnManager * txn_man = NULL;
    LogRecord  * log_to_apply = NULL;

    // uint64_t last_ping_time = get_sys_clock();

#ifdef NUM_TEST_TX
    uint64_t num_txns = 0;
#endif

    //////////////////////////
    // Main loop
    //////////////////////////
    while (true) {
        txn_man = NULL;
        msg = NULL;
        log_to_apply = NULL;
        
        // checkpoint the stats every 100 ms.
        if (GET_THD_ID == 0 && get_sys_clock() - last_stats_cp_time > STATS_CP_INTERVAL * MILLION) {
            stats->checkpoint();
            last_stats_cp_time += STATS_CP_INTERVAL * MILLION;
        }

#if TEST_RAFT
        usleep(1);
        if (g_role == ROLE_LEADER) {
            LogRecord * record = CCManager::get_test_log_record();
            raft_append(record);
        }
        if (get_sys_clock() - init_time > (g_warmup_time + g_run_time) * BILLION) {
            sim_done = true;
            glob_manager->worker_thread_done();
            break;
        }
        PAUSE100;
        continue;
#endif

        uint64_t t0 = get_sys_clock();

        // Get txn from the ready queue
        uint64_t txn_id;
        if (ready_queues[get_thd_id()]->pop(txn_id))
            txn_man = get_txn(txn_id);

        INC_FLOAT_STATS(time_wait_buffer, get_sys_clock() - t0);

        uint64_t t2 = get_sys_clock();
        if (txn_man) {
            assert(txn_man->get_txn_state() == TxnManager::RUNNING
                    || txn_man->get_txn_state() == TxnManager::PREPARING
                    || txn_man->get_txn_state() == TxnManager::ABORTED
                    || ((COMMIT_PROTOCOL == TAPIR || COMMIT_PROTOCOL == GPAC) && txn_man->get_txn_state() == TxnManager::COMMITTING && txn_man->get_coord_part() != g_part_id));
            // Execute transactions from the ready queue
            rc = txn_man->execute();
            handle_req_finish(rc, txn_man);
            INC_FLOAT_STATS(time_process_txn, get_sys_clock() - t2);
            continue;
        }

        // no txn in ready queue, process msg
        input_queues[get_thd_id()]->pop(msg);
        INC_FLOAT_STATS(time_read_input_queue, get_sys_clock() - t2);
        if (msg) {
        #if DEBUG_MSG_WAIT_INQUEUE
            uint64_t t1;
            msg_arrival_times->pop(t1);
            INC_FLOAT_STATS(time_msg_wait_inqueue, get_sys_clock() - t1);
        #endif
            uint64_t t2 = get_sys_clock();
            assert(glob_manager->txnid_to_server_thread(msg->get_txn_id()) == GET_THD_ID);
            if (msg->get_type() != Message::CLIENT_REQ)
                txn_man = get_txn(msg->get_txn_id());

            if (txn_man == NULL) {
                if (msg->get_type() == Message::REQ || msg->get_type() == Message::CLIENT_REQ || msg->get_type() == Message::PREPARE_REQ) {
                    txn_man = new TxnManager(msg);
                    INC_INT_STATS(num_remote_txn, 1);
                    add_txn(txn_man);
                }
            #if BYPASS_COORD_CONSENSUS
                else if (msg->get_type() == Message::PREPARED_ABORT || msg->get_type() == Message::PREPARED_COMMIT || msg->get_type() == Message::COMMITTED) {
                    txn_man = new TxnManager(msg);
                    txn_man->pending_msgs.push_back(msg);
                    INC_INT_STATS(num_remote_txn, 1);
                    add_txn(txn_man);
                    txn_man = NULL;
                    continue;
                }
            #endif
                else {
                    delete msg;
                    continue;
                }
            }

            // Execute transactions as a participant
            RC rc = txn_man->process_msg(msg);
            handle_req_finish(rc, txn_man);
            INC_FLOAT_STATS(time_process_txn, get_sys_clock() - t2);
            continue;
        }

        log_apply_queues[get_thd_id()]->pop(log_to_apply);
        INC_FLOAT_STATS(time_read_input_queue, get_sys_clock() - t2);
        if (log_to_apply) {
            uint64_t t2 = get_sys_clock();
            RC rc = RCOK;
            txn_man = get_txn(log_to_apply->txn_id);
            if (txn_man == NULL && (log_to_apply->type == LOG_PREPARE || log_to_apply->type == LOG_LOCAL_COMMIT)) {
            #if COMMIT_PROTOCOL == TAPIR
                if (glob_manager->txnid_to_server_node(log_to_apply->txn_id) != transport->get_partition_leader(g_part_id) || log_to_apply->type == LOG_PREPARE)
                    continue;
            #endif
                txn_man = new TxnManager(log_to_apply);
                add_txn(txn_man);
            }
            if (txn_man) {
                if (!txn_man->waiting_log_flush() && !txn_man->is_applier()) {
                #if COMMIT_PROTOCOL == TAPIR
                    if (glob_manager->txnid_to_server_node(log_to_apply->txn_id) != transport->get_partition_leader(g_part_id) || log_to_apply->type == LOG_PREPARE)
                        continue;
                #endif
                    if (log_to_apply->type == LOG_PREPARE || log_to_apply->type == LOG_LOCAL_COMMIT)
                        txn_man->set_type(TxnManager::MAN_APPLY);
                    else {
                        // the txn is aborted on leader or it is a readonly txn
                        remove_txn(txn_man);
                        delete txn_man;
                        txn_man = NULL;
                    }
                }
            }
            if (txn_man) {
                rc = txn_man->process_log(log_to_apply);
                handle_req_finish(rc, txn_man);
            }
            INC_FLOAT_STATS(time_log_apply, get_sys_clock() - t2);
            continue;
        }

        // no msg, see if we have a new txn to start
        uint64_t t3 = get_sys_clock();
        if (_new_txn) {
            assert(g_role == ROLE_LEADER);
            // Execute new transactions, these transactions are either newly-generated, or aborted before
            rc = _new_txn->execute();
            handle_req_finish(rc, _new_txn);
            INC_FLOAT_STATS(time_process_txn, get_sys_clock() - t3);
            _new_txn = NULL;
            continue;
        }

        // no new txn to start, see if we have aborted txn to retry
        bool restarted = false;
        uint64_t cur_time = get_sys_clock();
        for (auto it = _native_txns.begin(); it != _native_txns.end();) {
            auto cur_it = it++;
            TxnManager *native_txn = *cur_it;
            if (native_txn->get_txn_state() == TxnManager::WAITING_TO_START && native_txn->ready_to_start(cur_time)) {
                add_txn(native_txn);
                _new_txn = native_txn;
                restarted = true;
                break;
            }
        }

        if (restarted)
            continue;

        /* when time is up, stop generating new transactions */
    #ifdef NUM_TEST_TX
        if (num_txns > NUM_TEST_TX) {
    #else
        if (get_sys_clock() - init_time > (g_warmup_time + g_run_time) * BILLION) {
    #endif
            for (auto it = _native_txns.begin(); it != _native_txns.end();)
                it = _native_txns.erase(it);

            if (!sim_done && _native_txns.empty()) {
                sim_done = true;
                glob_manager->worker_thread_done();
            }

            if (glob_manager->get_thd_id() == 0 && glob_manager->are_all_worker_threads_done() && !terminate_sent) {
                terminate_sent = true;
                for (uint32_t i = 0; i < g_num_nodes; i++) {
                    if (i == g_node_id) continue;
                    Message * msg = new Message(Message::TERMINATE, i, 0, 0, NULL);
                    transport->send_msg(msg);
                }
            }

            break;
        }

        // Only the leader can generate new txns
        if (g_role == ROLE_LEADER && _native_txns.size() < MAX_NUM_NATIVE_TXNS && _new_txn == NULL) {
        #ifdef NUM_TEST_TX
            num_txns += 1;
        #endif
            INC_INT_STATS(num_home_txn, 1);
            QueryBase * query;
        #if TXN_GEN == FIX_GEN_RATIO
            query = GET_WORKLOAD->gen_query(-1);
        #elif TXN_GEN == FIX_ACTIVE_RATIO
            if (_num_sp_to_gen > 0) {
                // generate a new SP txn
                query = GET_WORKLOAD->gen_query(0);
                _num_sp_to_gen --;
            } else if (_num_mp_to_gen > 0) {
                // generate a new MP txn
                query = GET_WORKLOAD->gen_query(1);
                _num_mp_to_gen --;
            } else {
                query = GET_WORKLOAD->gen_query(-1);
            }
        #endif
            uint64_t txn_id = get_new_txn_id(); 
            _new_txn = new TxnManager(query, txn_id);
            _native_txns.insert(_new_txn);
            add_txn(_new_txn);
            continue;
        }

        // pause if nothing to do
        PAUSE10;
        INC_FLOAT_STATS(time_idle, get_sys_clock() - t3);
        continue;
    }

    INC_FLOAT_STATS(run_time, get_sys_clock() - init_time);
    assert(_native_txns.empty());
    assert(_new_txn == NULL);
    if (get_thd_id() == 0) {
        stats->checkpoint();
        //assert(_txn_table.size() == 0);
    }

    for (auto txn : _txn_table) {
        if (txn.second->get_txn_state() == TxnManager::COMMITTED || txn.second->get_txn_state() == TxnManager::ABORTED)
            txn.second->update_stats();
    }

    return FINISH;
}

uint64_t
ServerThread::get_new_txn_id() 
{
    // todo: unify txn id and cc timestamp
    uint64_t txn_id = _max_txn_id ++;
    txn_id = txn_id * g_num_server_threads + _thd_id;
    txn_id = txn_id * g_num_nodes + g_node_id;
    return txn_id;
}

TxnManager *
ServerThread::get_txn(uint64_t txn_id)
{
    auto txn_it = _txn_table.find(txn_id);
    if (txn_it == _txn_table.end())
        return NULL;
    return txn_it->second;
}

void
ServerThread::add_txn(TxnManager *txn)
{
    _txn_table[txn->get_txn_id()] = txn;
}

void
ServerThread::remove_txn(TxnManager *txn)
{
    _txn_table.erase(txn->get_txn_id());
}

TxnManager*
ServerThread::get_debug_txn(uint64_t txn_id) {
    return _debug_txn_table[txn_id];
}

// RCOK: txn active, do nothing.
// COMMIT: txn commits
// ABORT: txn aborts
// WAIT: txn waiting for lock
// LOCAL_MISS: txn needs data from a remote node.
void
ServerThread::handle_req_finish(RC rc, TxnManager * &txn_man)
{
    if (rc == WAIT) {
        INC_INT_STATS(num_waits, 1);
    } else if (rc == ABORT) {
        _native_txns.erase(txn_man);
    #if RERUN_ABORT
        if (txn_man->get_num_aborts() <= MAX_ABORT_NUMS && txn_man->is_coord() && !txn_man->get_store_procedure()->is_self_abort()) {
            TxnManager * restart_txn = new TxnManager(txn_man, get_new_txn_id());
            _native_txns.insert(restart_txn);
            INC_INT_STATS(num_aborts_restart, 1);
        }
    #endif
        if (txn_man->is_coord() && txn_man->get_store_procedure()->is_self_abort())
            INC_INT_STATS(num_aborts_terminate, 1);
    } else if (rc == COMMIT) {
    #if DEBUG_TXN_RESULT && WORKLOAD == BANK
        if (txn_man->is_coord()) {
            QueryBank *query = (QueryBank*) txn_man->get_store_procedure()->get_query();
            balances[glob_manager->get_thd_id()][query->from_key] -= query->amount;
            balances[glob_manager->get_thd_id()][query->to_key] += query->amount;
            //printf("Txn (%lu) %ld -> %ld: %ld, commit\n", _txn_id, query->from_key, query->to_key, query->amount);
        }
    #endif
    #if TXN_GEN == FIX_ACTIVE_RATIO
        if (txn_man->is_mp_txn())
            _num_mp_to_gen ++;
        else
            _num_sp_to_gen ++;
    #endif
        _native_txns.erase(txn_man);
    }

    if ((txn_man->get_txn_state() == TxnManager::COMMITTED || txn_man->get_txn_state() == TxnManager::ABORTED)
        && !txn_man->waiting_remote() && !txn_man->waiting_log_flush()) {
        txn_man->update_stats();
        remove_txn(txn_man);
        delete txn_man;
    }
}
