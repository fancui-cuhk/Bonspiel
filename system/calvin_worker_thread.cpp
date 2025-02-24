#include <sched.h>
#include <iomanip>
#include "global.h"
#include "manager.h"
#include "calvin_worker_thread.h"
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

CalvinWorkerThread::CalvinWorkerThread(uint64_t thd_id) : Thread(thd_id, CALVIN_THREAD) { }

RC
CalvinWorkerThread::run()
{
    glob_manager->init_rand(get_thd_id());
    glob_manager->set_thd_id(get_thd_id());
#if SET_AFFINITY
    pin_thread_by_id(pthread_self(), _thd_id);
#endif
    pthread_barrier_wait(&global_barrier);

    RC           rc = RCOK;
    uint32_t     cur_txn_pos = 0;
    uint64_t     init_time = get_sys_clock();
    Message *    msg = NULL;
    TxnManager * txn_man = NULL;

    while (true) {
        if (get_sys_clock() - init_time > (g_warmup_time + g_run_time) * BILLION)
            break;

        if (g_role == ROLE_LEADER && calvin_new_batch_ready) {
            assert(calvin_txn_remaining != 0);

            while (calvin_txn_remaining > 0) {
                auto it = calvin_txn_list.begin();
                advance(it, cur_txn_pos);
                txn_man = *it;

                rc = txn_man->execute();
                if (rc == COMMIT) {
                    txn_man->update_stats();
                    delete txn_man;
                    calvin_txn_remaining--;
                    cur_txn_pos++;
                } else if (rc == WAIT) {
                    break;
                } else
                    assert(false);

                if (calvin_txn_remaining == 0) {
                    calvin_new_batch_ready = false;
                }
            }
        }

        input_queues[get_thd_id()]->pop(msg);
        if (msg) {
            rc = txn_man->process_msg(msg);

            if (g_role == ROLE_LEADER) {
                if (rc == COMMIT) {
                    txn_man->update_stats();
                    delete txn_man;
                    calvin_txn_remaining--;
                    cur_txn_pos++;
                } else if (rc == WAIT) {
                    continue;
                } else
                    assert(false);

                if (calvin_txn_remaining == 0) {
                    calvin_new_batch_ready = false;
                }
            } else
                assert(rc == COMMIT || rc == RCOK);
        }

        PAUSE10;
    }

    return FINISH;
}
