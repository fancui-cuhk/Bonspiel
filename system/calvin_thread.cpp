#include <sched.h>
#include <iomanip>
#include "global.h"
#include "manager.h"
#include "calvin_thread.h"
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

CalvinThread::CalvinThread(uint64_t thd_id)
    :Thread(thd_id, CALVIN_THREAD)
{
    _epoch_id = 0;
    _cur_txn_id = 1;
    _txn_man = NULL;
}

RC
CalvinThread::run()
{
    glob_manager->init_rand(get_thd_id());
    glob_manager->set_thd_id(get_thd_id());
#if SET_AFFINITY
    pin_thread_by_id(pthread_self(), _thd_id);
#endif
    pthread_barrier_wait(&global_barrier);

    uint64_t    init_time = get_sys_clock();
    uint64_t    last_epoch = init_time;
    LogRecord * log = NULL;
    Message *   msg = NULL;

    while (true) {
        if (get_sys_clock() - init_time > (g_warmup_time + g_run_time) * BILLION)
            break;

        if (g_role == ROLE_LEADER) {
            uint64_t t0 = get_sys_clock();
            uint64_t elapsed = (t0 - last_epoch) / BILLION;

            if (elapsed > g_epoch_size && calvin_txn_remaining == 0) {
                assert(!calvin_new_batch_ready);

                // On leader, generate txns for the new epoch and replicate them using Raft
                _epoch_id++;
                UnstructuredBuffer buffer;
                buffer.put(&_epoch_id);
                buffer.put(&g_max_num_active_txns);

                for (uint32_t i = 0; i < g_max_num_active_txns; i++) {
                    QueryBase * query;
                    query = GET_WORKLOAD->gen_query(0);
                    uint64_t txn_id = get_new_txn_id();
                    _txn_man = new TxnManager(query, txn_id);
                    calvin_txn_list.push_back(_txn_man);
                    calvin_txn_remaining++;

                    // currently CALVIN only works with YCSB
                    assert(WORKLOAD == YCSB);
                    QueryYCSB * ycsb_query = (QueryYCSB *) query;
                    RequestYCSB * requests = ycsb_query->get_requests();
                    uint32_t request_cnt = ycsb_query->get_request_count();
                    buffer.put(&txn_id);
                    buffer.put(&request_cnt);
                    for (uint32_t i = 0; i < request_cnt; i++) {
                        buffer.put(&requests->rtype);
                        buffer.put(&requests->key);
                        buffer.put(&requests->value);
                    }
                }

                // log entry with txns
                LogRecord * record = (LogRecord *) new char[sizeof(LogRecord) + buffer.size()];
                record->size = sizeof(LogRecord) + buffer.size();
                record->type = LOG_CALVIN;
                record->txn_id = 1;
                if (buffer.size() > 0)
                    memcpy(record->data, buffer.data(), buffer.size());

                raft_append(record);
                last_epoch = get_sys_clock();
            } else {
                // log entry without txns
                LogRecord * record = (LogRecord *) new char[sizeof(LogRecord)];
                record->size = sizeof(LogRecord);
                record->type = LOG_CALVIN;
                record->txn_id = 0;

                raft_append(record);
            }

            input_queues[get_thd_id()]->pop(msg);
            if (msg) {
                assert(msg->get_type() == Message::CALVIN_TXN_INFO);

                UnstructuredBuffer buffer(msg->get_data());
                uint32_t epoch_id;
                uint32_t num_txns;
                buffer.get(&epoch_id);
                buffer.get(&num_txns);
                if (epoch_id == _epoch_id) {
                    for (uint32_t i = 0; i < num_txns; i++) {
                        uint64_t txn_id;
                        uint32_t request_cnt;
                        access_t rtype;
                        uint64_t key;
                        uint32_t value;

                        buffer.get(&txn_id);
                        buffer.get(&request_cnt);
                        RequestYCSB * requests = new RequestYCSB[request_cnt];

                        for (uint32_t j = 0; j < request_cnt; j++) {
                            buffer.get(&rtype);
                            buffer.get(&key);
                            buffer.get(&value);
                            requests[j].rtype = rtype;
                            requests[j].key = key;
                            requests[j].value = value;
                        }

                        QueryYCSB * query = new QueryYCSB(requests, request_cnt);
                        _txn_man = new TxnManager(query, txn_id);
                        calvin_txn_list.push_back(_txn_man);
                        calvin_txn_remaining++;
                    }
                }

                uint32_t part_id = transport->get_node_partition(msg->get_src_node_id());
                calvin_part_recv.insert(part_id);
            }

            if (calvin_part_recv.size() == g_num_parts - 1) {
                calvin_new_batch_ready = true;
                std::sort(calvin_txn_list.begin(), calvin_txn_list.end(), CompareTxn());
            }
        } else if (g_role == ROLE_REPLICA) {
            // On follower, accept log from partition leader and send txn info to leader in the local DC
            log_apply_queues[0]->pop(log);

            if (log && log->txn_id == 1) {
                UnstructuredBuffer buffer(log->data);
                uint32_t dest = transport->get_partition_leader(transport->get_node_dc(g_node_id));
                assert(transport->get_node_dc(dest) == transport->get_node_dc(g_node_id));

                Message msg(Message::CALVIN_TXN_INFO, dest, 0, buffer.size(), buffer.data());
                transport->send_msg(&msg);
            }
        }

        PAUSE100 PAUSE100 PAUSE100 PAUSE100 PAUSE100;
        PAUSE100 PAUSE100 PAUSE100 PAUSE100 PAUSE100;
    }

    return FINISH;
}

uint64_t
CalvinThread::get_new_txn_id()
{
    uint64_t txn_id = _cur_txn_id++;
    txn_id = txn_id * g_num_nodes + g_node_id;
    txn_id = ((uint64_t) _epoch_id << 56) | txn_id;

    return txn_id;
}

