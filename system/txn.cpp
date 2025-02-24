#include "txn.h"
#include "row.h"
#include "workload.h"
#include "ycsb.h"
#include "server_thread.h"
#include "table.h"
#include "catalog.h"
#include "index_btree.h"
#include "index_hash.h"
#include "helper.h"
#include "manager.h"
#include "message.h"
#include "query.h"
#include "txn_table.h"
#include "transport.h"
#include "cc_manager.h"
#include "store_procedure.h"
#include "ycsb_store_procedure.h"
#include "tpcc_store_procedure.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "tpce_const.h"
#include "tpce_store_procedure.h"
#include "tpce_query.h"
#include "bank_query.h"
#include "tictoc_manager.h"
#include "silo_manager.h"
#include "docc_manager.h"
#include "sundial_manager.h"
#include "lock_manager.h"
#include "raft_thread.h"
#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT
#include "row_lock.h"
#endif
#include "log.h"

// TODO. cleanup the accesses related malloc code.

// This function is invoked to create a txn manager at the coordinator
TxnManager::TxnManager(QueryBase * query, uint64_t txn_id) : TxnManager(MAN_COORD, txn_id)
{
    coord_part = g_part_id;
    _store_procedure = GET_WORKLOAD->create_store_procedure(this, query);
    _store_procedure->init();
    _cc_manager = new CC_MAN(this, glob_manager->get_ts(GET_THD_ID));
    _txn_state = WAITING_TO_START;
}

// Upon receiving a REQ message or a PREPARE_REQ message, this function is invoked to
// create txn managers at participants
TxnManager::TxnManager(Message * msg) : TxnManager(MAN_PART, msg->get_txn_id())
{
    // assert(msg->get_type() == Message::REQ || msg->get_type() == Message::PREPARE_REQ);
    if (msg->get_type() == Message::REQ || msg->get_type() == Message::PREPARE_REQ)
        coord_part = transport->get_node_partition(msg->get_src_node_id());
    _store_procedure = GET_WORKLOAD->create_store_procedure(this, NULL);
    _store_procedure->init();
    _cc_manager = new CC_MAN(this, 0); // will receive timestamp in request
}

// Upon receiving log records, this function is invoked to create txn managers at appiliers
TxnManager::TxnManager(LogRecord * log) : TxnManager(MAN_APPLY, log->txn_id)
{
    _store_procedure = NULL;
    _cc_manager = new CC_MAN(this, 0);
    assert(log->type == LOG_LOCAL_COMMIT || log->type == LOG_PREPARE);
    if (log->type == LOG_LOCAL_COMMIT)
        coord_part = g_part_id;
    // for log_prepare, we will know the coord when parsing the log
}

// This function is invoked to rerun aborted txns at the coordinator
TxnManager::TxnManager(TxnManager * txn, uint64_t txn_id) : TxnManager(MAN_COORD, txn_id)
{
    assert(txn->_txn_state == ABORTED && txn->_type == MAN_COORD);
    _txn_start_time = txn->_txn_start_time;
    QueryBase * query = GET_WORKLOAD->clone_query(txn->get_store_procedure()->get_query());
    _store_procedure = GET_WORKLOAD->create_store_procedure(this, query);
    _store_procedure->init();
    _num_aborts = txn->_num_aborts;
    coord_part = txn->coord_part;
    _cc_manager = new CC_MAN(this, txn->_cc_manager->_timestamp);
    _txn_state = WAITING_TO_START;
    txn->schedule_retry();
    _ready_time = txn->_ready_time;
}

TxnManager::TxnManager(ManagerType type, uint64_t txn_id) 
{
    _store_procedure = NULL;
    _txn_id = txn_id;
    _txn_state = RUNNING;
    _num_resp_expected = 0;
    _waiting_log_flush = false;
    _type = type;
    _mp_txn = (type == MAN_PART);
    _msg = NULL;
    _log = NULL;
    _resp_size = 0;
    _resp_data = NULL;
    _num_aborts = 0;
    coord_part = -1;

    _txn_start_time = get_sys_clock();
    _txn_restart_time = _txn_start_time;
    _lock_wait_time = 0;
    _net_wait_time = 0;
    _msg_count = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);
    _msg_size = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);
    memset(_msg_count, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
    memset(_msg_size, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);

    _ready_time = 0;
    _remote_txn_abort = false;
    _prepare_start_time = 0;
    _log_start_flush_time = 0;
    _log_total_flush_time = 0;
    _time_debug1 = 0;
    _time_debug2 = 0;
    _time_debug3 = 0;
    _time_debug4 = 0;
#if BYPASS_LEADER_DECIDE
    num_append_resps.assign(g_num_parts, 0);
#endif

#if COMMIT_PROTOCOL == TAPIR || COMMIT_PROTOCOL == GPAC
    num_undetermined_partitions = 0;
    partition_votes.assign(g_num_parts, VoteStat());
#endif
}

TxnManager::~TxnManager()
{
    _mm_free(_msg_count);
    _mm_free(_msg_size);
    while (!pending_msgs.empty()) {
        Message * msg = pending_msgs.front();
        delete msg;
        pending_msgs.pop_front();
    }
    if (_store_procedure)
        delete _store_procedure;
    delete _cc_manager;
}

void
TxnManager::set_type(TxnManager::ManagerType type)
{
    _type = type;
#if CC_ALG == DOCC
    ((CC_MAN*)_cc_manager)->set_priority(0);
#endif
}

// Execution entrance
RC
TxnManager::execute()
{
#if CC_ALG == CALVIN
    return calvin_execute();
#else
    if (waiting_local_requests())
        _lock_wait_time += get_sys_clock() - _lock_wait_start_time;

    if (is_sub_txn())
        return participant_execute();
    else if (is_applier())
        return applier_execute();
    else
        return coordinator_execute();
#endif
}

void
TxnManager::send_2pc_prepare()
{
    // Remote partitions are inserted into remote_parts_involved in CCManager->process_requests()
    // get_remote_nodes() function is not in use now
    // _cc_manager->get_remote_nodes(&remote_parts_involved);

    for (uint32_t remote_part : remote_parts_involved) {
        UnstructuredBuffer buffer;
        bool send = _cc_manager->need_prepare_req(remote_part, buffer);
        if (send) {
            _num_resp_expected ++;
        #if COMMIT_PROTOCOL == RAFT_2PC
            uint32_t part_leader = transport->get_partition_leader(remote_part);
            Message msg(Message::PREPARE_REQ, part_leader, get_txn_id(), buffer.size(), buffer.data());
            send_msg(&msg);
        #elif COMMIT_PROTOCOL == TAPIR || COMMIT_PROTOCOL == GPAC
            num_undetermined_partitions ++;
            for (uint32_t node_id : transport->get_partition_nodes(remote_part)) {
                Message msg(Message::PREPARE_REQ, node_id, get_txn_id(), buffer.size(), buffer.data());
                send_msg(&msg);
            }
        #endif
        }
    }

#if COMMIT_PROTOCOL == TAPIR || COMMIT_PROTOCOL == GPAC
    // For TAPIR and GPAC, send preapre msgs to coordinator replicas as well
    UnstructuredBuffer buffer;
    bool send = _cc_manager->need_prepare_req(g_part_id, buffer);
    if (send) {
        _num_resp_expected ++;
        num_undetermined_partitions ++;
        for (uint32_t node_id : transport->get_partition_nodes(g_part_id)) {
            if (node_id == g_node_id) continue;
            Message msg(Message::PREPARE_REQ, node_id, get_txn_id(), buffer.size(), buffer.data());
            send_msg(&msg);
        }
    }
#endif
}

void
TxnManager::send_2pc_vote(RC rc)
{
    assert(rc == ABORT || rc == RCOK || rc == COMMIT);
    Message::Type type;
    if (rc == RCOK)
        type = Message::PREPARED_COMMIT;
    else if (rc == ABORT)
        type = Message::PREPARED_ABORT;
    else if (rc == COMMIT)
        type = Message::COMMITTED;
#if BYPASS_COORD_CONSENSUS
    // Broadcast 2PC votes to all partition leaders
    for (uint32_t remote_part : remote_parts_involved) {
        if (remote_part == g_part_id) continue;
        uint32_t part_leader = transport->get_partition_leader(remote_part);
        Message vote_msg(type, part_leader, get_txn_id(), 0, NULL);
        send_msg(&vote_msg);
    }
#else
    Message resp_msg(type, _src_node_id, get_txn_id(), 0, NULL);
    send_msg(&resp_msg);
#endif
}

#if COMMIT_PROTOCOL == TAPIR || COMMIT_PROTOCOL == GPAC
void
TxnManager::send_2pc_fast_vote(RC rc)
{
    assert(rc == ABORT || rc == RCOK || rc == COMMIT);
    if (rc == COMMIT) {
        if (g_role == ROLE_LEADER) {
            Message vote_msg(Message::COMMITTED, _src_node_id, get_txn_id(), 0, NULL);
            send_msg(&vote_msg);
        }
    } else {
        VoteType vote = (rc == RCOK) ? VOTE_COMMIT : VOTE_ABORT;
        Message vote_msg(Message::APPEND_ENTRIES_RESP, _src_node_id, get_txn_id(), sizeof(vote), (char *)&vote);
        send_msg(&vote_msg);
    }
}
#endif

void
TxnManager::send_2pc_decide(RC rc)
{
    assert(rc == COMMIT || rc == ABORT);
    if (!is_mp_txn()) return;

    Message::Type type = (rc == COMMIT) ? Message::COMMIT_REQ : Message::ABORT_REQ;
    for (uint32_t remote_part : remote_parts_involved) {
        // Do not send 2PC decide messages to aborted partitions and read-only partitions
    #if COMMIT_PROTOCOL == RAFT_2PC
        if (aborted_remote_parts.find(remote_part) != aborted_remote_parts.end())
            continue;
    #endif
        if (readonly_remote_parts.find(remote_part) != readonly_remote_parts.end())
            continue;

        UnstructuredBuffer buffer;
        bool send = _cc_manager->need_commit_req(rc, remote_part, buffer);
        if (send) {
        #if COMMIT_PROTOCOL == RAFT_2PC
            if (rc == COMMIT)
                _num_resp_expected ++;
            uint32_t part_leader = transport->get_partition_leader(remote_part);
            Message msg(type, part_leader, get_txn_id(), buffer.size(), buffer.data());
            send_msg(&msg);
        #elif COMMIT_PROTOCOL == TAPIR || COMMIT_PROTOCOL == GPAC
            for (uint32_t node_id : transport->get_partition_nodes(remote_part)) {
                Message msg(type, node_id, get_txn_id(), buffer.size(), buffer.data());
                send_msg(&msg);
            }
        #endif
        }
    }

#if COMMIT_PROTOCOL == TAPIR || COMMIT_PROTOCOL == GPAC
    UnstructuredBuffer buffer;
    bool send = _cc_manager->need_commit_req(rc, g_part_id, buffer);
    if (send) {
        for (uint32_t node_id : transport->get_partition_nodes(g_part_id)) {
            if (node_id == g_node_id) continue;
            Message msg(type, node_id, get_txn_id(), buffer.size(), buffer.data());
            send_msg(&msg);
        }
    }
#endif
}

void
TxnManager::send_2pc_ack()
{
    Message resp_msg(Message::ACK, _src_node_id, get_txn_id(), 0, NULL);
    send_msg(&resp_msg);
}

#if COMMIT_PROTOCOL == TAPIR
void
TxnManager::send_slow_path()
{
    for (uint32_t remote_part : remote_parts_involved) {
        if (prepared_remote_parts.find(remote_part) != prepared_remote_parts.end())
            continue;
        UnstructuredBuffer buffer;
        bool send = _cc_manager->need_prepare_req(remote_part, buffer);
        if (send) {
            Message msg(Message::SLOW_PATH, transport->get_partition_leader(remote_part), get_txn_id(), buffer.size(), buffer.data());
            send_msg(&msg);
        }
    }
}
#endif

void
TxnManager::write_log()
{
#if LOG_ENABLE
    LogRecord * record = NULL;
    if (_txn_state == PREPARING) {
        record = _cc_manager->get_log_record(LOG_PREPARE);
    } else if (_txn_state == COMMITTING) {
        if (_mp_txn)
            record = _cc_manager->get_log_record(LOG_DIST_COMMIT);
        else
            record = _cc_manager->get_log_record(LOG_LOCAL_COMMIT);
    } else if (_txn_state == ABORTING) {
        record = _cc_manager->get_log_record(LOG_ABORT);
    } else
        assert(false);

    if (record) {
        raft_append(record);
        _waiting_log_flush = true;
        _log_start_flush_time = get_sys_clock();
    }
#endif
}

#if COMMIT_PROTOCOL == RAFT_2PC
// This function implements the logic of a 2PC coordinator
RC
TxnManager::coordinator_execute()
{
    TXN_STAGE_BEGIN;

    RC rc = RCOK;

    // If the transaction is waiting to start, change its stage to RUNNING
    TXN_STAGE(WAITING_TO_START)
    {
        assert(_msg == NULL && _log == NULL);
        _txn_restart_time = get_sys_clock();
        TXN_TO_STAGE(RUNNING);
    }

    // Transaction execution logic
    TXN_STAGE(RUNNING)
    {
        if (!waiting_remote() || (waiting_local_requests() && is_txn_ready())) {
            if (_cc_manager->is_signal_abort())
                TXN_TO_STAGE(ABORTING);

            uint64_t tt = get_sys_clock();
            if (waiting_local_requests())
                rc = _cc_manager->process_requests();
            if (rc == RCOK && !waiting_remote())
                rc = _store_procedure->execute();

            if (rc == WAIT)
                _lock_wait_start_time = get_sys_clock();

            // If RCOK or ABORT is returned, proceed to prepare or abort
            // Otherwise the transaction is waiting for remote responses (remote reads)
            INC_FLOAT_STATS(logic, get_sys_clock() - tt);
            if (rc == ABORT)
                TXN_TO_STAGE(ABORTING);
            if (rc == RCOK && !waiting_remote())
                TXN_TO_STAGE(PREPARING);
        }
        if (waiting_remote() && _msg != NULL) {
            // Take care of responses of remote read requests
            assert(_msg != NULL);
            assert(_msg->get_type() == Message::RESP_COMMIT || _msg->get_type() == Message::RESP_ABORT);

            // Decrease _num_resp_expected
            ATOM_SUB_FETCH(_num_resp_expected, 1);
            if (_msg->get_type() == Message::RESP_COMMIT) {
                // Remote responses to read requests
                assert(_msg->get_data_size() > 0);
                _cc_manager->process_remote_resp(_msg->get_src_node_id(), _msg->get_data_size(), _msg->get_data());
                cleanup_current_msg();
            #if CC_ALG == DOCC
                if (!waiting_remote()) {
                    // All remote read requests in this round have returned, handle local reads
                    _cc_manager->process_requests();
                    // Read requests in OCC do not lead to abort, so no need to handle return code here
                }
            #endif
                if (!waiting_remote() && !waiting_local_requests())
                    TXN_TO_STAGE(RUNNING);
            } else if (_msg->get_type() == Message::RESP_ABORT) {
                // Remote responses to abort the transaction (lock-related aborts for 2PL)
                remote_parts_involved.erase(transport->get_node_partition(_msg->get_src_node_id()));
                _remote_txn_abort = true;
                cleanup_current_msg();
                TXN_TO_STAGE(ABORTING);
            }
        }
        return rc;
    }

    // Transaction preparing logic
    TXN_STAGE(PREPARING)
    {
        if (!waiting_remote() && !waiting_log_flush()) {
            assert(_msg == NULL);
            assert(!waiting_local_requests());
            if (_prepare_start_time == 0)
                _prepare_start_time = get_sys_clock();
            // Locally validate whether the transaction can be committed
            rc = _cc_manager->process_prepare_phase_coord();
            if (_mp_txn && rc == ABORT) {
                INC_INT_STATS(dist_txn_local_abort, 1);
            }
            if (rc == ABORT)
                TXN_TO_STAGE(ABORTING);
            if (rc == WAIT) {
                _lock_wait_start_time = get_sys_clock();
                return rc;
            }

            assert(rc == RCOK);
            if (_mp_txn) {
                // Send 2PC prepare messages to all participating leaders
                send_2pc_prepare();
                write_log();
            #if BYPASS_COORD_CONSENSUS
                if (!waiting_log_flush())
                    send_2pc_vote(RCOK);
            #endif
            }

            if (!waiting_remote() && !waiting_log_flush())
                TXN_TO_STAGE(COMMITTING);
        } else if (_msg != NULL) {
            // There are pending remote messages
            assert(_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT
                    || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP);

            uint32_t part_id = transport->get_node_partition(_msg->get_src_node_id());
            if (_msg->get_type() == Message::PREPARED_ABORT) {
                // 2PC abort messages
                rc = ABORT;
                _remote_txn_abort = true;
                // This function updates the hotness statistics
                _cc_manager->process_prepare_resp(ABORT, _msg->get_src_node_id(), _msg->get_data());
                aborted_remote_parts.insert(part_id);
                cleanup_current_msg();
                INC_INT_STATS(dist_txn_2pc_abort, 1);
                TXN_TO_STAGE(ABORTING);
            } else if (_msg->get_type() == Message::APPEND_ENTRIES_RESP) {
            #if BYPASS_LEADER_DECIDE
                num_append_resps[part_id] += 1;
                if (num_append_resps[part_id] != transport->get_partition_quorum_size(part_id) - 1) {
                    cleanup_current_msg();
                    return RCOK;
                }
            #endif
            } else if (_msg->get_type() == Message::COMMITTED) {
                readonly_remote_parts.insert(part_id);
            }
            cleanup_current_msg();
            if (prepared_remote_parts.find(part_id) == prepared_remote_parts.end()) {
                // One more remote partition is prepared
                prepared_remote_parts.insert(part_id);
                if (ATOM_SUB_FETCH(_num_resp_expected, 1) == 0 && !waiting_log_flush())
                    TXN_TO_STAGE(COMMITTING);
            }
        } else if (_log != NULL) {
            _waiting_log_flush = false;
            cleanup_current_log();
        #if BYPASS_COORD_CONSENSUS
            send_2pc_vote(RCOK);
        #endif
            if (!waiting_remote())
                TXN_TO_STAGE(COMMITTING);
        }

        return rc;
    }

    TXN_STAGE(COMMITTING)
    {
    #if CC_ALG == SUNDIAL
        if (_mp_txn) {
            glob_manager->vote_for_local();
        }
    #endif

        if (!waiting_remote() && !waiting_log_flush()) {
            assert(_msg == NULL && _log == NULL);
            assert(!waiting_remote() && !waiting_local_requests());
            _commit_start_time = get_sys_clock();

        #if CC_ALG == DOCC && EARLY_WRITE_INSTALL
            // Install write set for local txns (OCC)
            assert(COMMIT_PROTOCOL == RAFT_2PC);
            if (!_mp_txn) {
                _cc_manager->install_local_write_set();
            }
        #endif

            write_log();
            if (!_mp_txn || !BYPASS_COORD_CONSENSUS) {
                if (waiting_log_flush())
                    return RCOK;
            }
        #if !BYPASS_COORD_CONSENSUS
            send_2pc_decide(COMMIT);
        #endif

            // Commit the transaction locally
            _cc_manager->process_commit_phase_coord(COMMIT);
            _txn_state = COMMITTED;
            _finish_time = get_sys_clock();
            return COMMIT;
        } else if (waiting_log_flush()) {
        #if BYPASS_LEADER_DECIDE
            if (_msg != NULL) {
                assert(_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT
                        || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP);
                cleanup_current_msg();
                return RCOK;
            }
        #endif
            assert(_log != NULL && (_log->type == LOG_LOCAL_COMMIT || _log->type == LOG_DIST_COMMIT));
            _waiting_log_flush = false;
            cleanup_current_log();

        #if !BYPASS_COORD_CONSENSUS
            send_2pc_decide(COMMIT);
        #endif

            // Commit the transaction locally
            _cc_manager->process_commit_phase_coord(COMMIT);
            _txn_state = COMMITTED;
            _finish_time = get_sys_clock();
            return COMMIT;
        }
    }

    TXN_STAGE(ABORTING)
    {
    #if CC_ALG == SUNDIAL
        if (_mp_txn) {
            glob_manager->vote_for_remote();
        }
    #endif

        _commit_start_time = get_sys_clock();
        if (_mp_txn) {
            write_log(); // abort log can be async
            send_2pc_decide(ABORT);
        }
        _store_procedure->txn_abort();
        _cc_manager->process_commit_phase_coord(ABORT);
        _num_resp_expected = 0;
        _txn_state = ABORTED;
        _num_aborts += 1;
        _finish_time = get_sys_clock();
        return ABORT;
    }

    TXN_STAGE(COMMITTED)
    {
        // although committed, may need to process some async messages
        if (_msg != NULL) {
        #if BYPASS_LEADER_DECIDE
            if (_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT 
                || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP) {
            cleanup_current_msg();
            return RCOK;
            }
        #endif
            assert(_msg->get_type() == Message::ACK);
            ATOM_SUB_FETCH(_num_resp_expected, 1);
            cleanup_current_msg();
        } else if (_log != NULL) {
            assert(_log->type == LOG_DIST_COMMIT || _log->type == LOG_LOCAL_COMMIT);
            cleanup_current_log();
            _waiting_log_flush = false;
        }
        return RCOK;
    }

    TXN_STAGE(ABORTED)
    {
        if (_msg != NULL) {
            cleanup_current_msg();
        } else if (_log != NULL) {
            if (_log->type == LOG_ABORT) {
                cleanup_current_log();
                _waiting_log_flush = false;
            }
        } else {
            assert(false);
        }
        return RCOK;
    }

    assert(false);
}

// This function implements the logic of a 2PC participant
RC
TxnManager::participant_execute()
{
    if (_msg) {
        if (_msg->get_type() == Message::PREPARE_REQ) {
            assert(_txn_state == RUNNING || _txn_state == PREPARING);
            TXN_TO_STAGE(PREPARING);
        } else if (_msg->get_type() == Message::COMMIT_REQ) {
            assert(_txn_state == PREPARING);
            // if BYPASS_LEADER_DECIDE==true, we may get commit request before being notified with log flush
            // in this case, we ignore the _msg, and wait for the log flush first
            if (!waiting_log_flush()) {
                TXN_TO_STAGE(COMMITTING);
            } else if (_log == NULL) {
                return RCOK;
            }
        } else if (_msg->get_type() == Message::ABORT_REQ) {
            assert((_txn_state != COMMITTING && _txn_state != COMMITTED) || _cc_manager->is_read_only());
            if (_txn_state == RUNNING || _txn_state == PREPARING)
                _txn_state = ABORTING;
        }
    #if BYPASS_COORD_CONSENSUS
        if (_txn_state < PREPARING && (_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT 
            || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP)) {
            pending_msgs.push_back(_msg);
            _msg = NULL;
            return RCOK;
        }
    #endif
    }

    TXN_STAGE_BEGIN;
    RC rc = RCOK;
    TXN_STAGE(RUNNING)
    {
        if (_cc_manager->is_signal_abort()) {
            rc = ABORT;
        } else if (waiting_local_requests()) {
            rc = _cc_manager->process_requests();
        } else {
            if (!_msg) {
                // This is possible when a distributed txn gets its waiting reservation
                assert(EARLY_WRITE_INSTALL);
                return rc;
            } else {
                // Take care of new remote requests (remote read requests)
                char * data = _msg->get_data();
                UnstructuredBuffer buffer(_msg->get_data());
                uint32_t header_size = _cc_manager->process_remote_req_header( &buffer );
                data += header_size;
                rc = _store_procedure->process_remote_req(_msg->get_data_size() - header_size, data);
            }
        }

        if (rc == RCOK) {
            // Construct responses for remote requests
            UnstructuredBuffer resp_buffer;
            _cc_manager->get_resp_data(resp_buffer);
            Message msg(Message::RESP_COMMIT, _src_node_id, get_txn_id(), resp_buffer.size(), resp_buffer.data());
            send_msg(&msg);
            cleanup_current_msg();
        } else if (rc == ABORT) {
            // Runtime aborts (locking contention, etc.)
            Message msg(Message::RESP_ABORT, _src_node_id, get_txn_id(), 0, NULL);
            send_msg(&msg);
            cleanup_current_msg();
            TXN_TO_STAGE(ABORTING);
        }

        if (rc == WAIT)
            _lock_wait_start_time = get_sys_clock();

        assert(rc == RCOK || rc == ABORT || rc == WAIT);
        return rc;
    }

    TXN_STAGE(PREPARING)
    {
        if (_msg != NULL) {
            if (_msg->get_type() == Message::PREPARE_REQ) {
                if (_prepare_start_time == 0)
                    _prepare_start_time = get_sys_clock();
                assert(_msg != NULL && _msg->get_type() == Message::PREPARE_REQ);
                UnstructuredBuffer buffer;
                // Locally validate whether the transaction can be committed
                rc = _cc_manager->process_prepare_req(_msg->get_data_size(), _msg->get_data(), buffer);

                if (rc == WAIT) {
                    return WAIT;
                }

                cleanup_current_msg();
                if (rc == RCOK) {
                    write_log();
                    if (waiting_log_flush())
                        return RCOK;
                }

                send_2pc_vote(rc);

                if (rc == ABORT) {
                    TXN_TO_STAGE(ABORTING);
                } else if (rc == COMMIT) {
                    TXN_TO_STAGE(COMMITTING);
                }
            }
        #if BYPASS_COORD_CONSENSUS
            else if (_msg->get_type() == Message::PREPARED_ABORT) {
                cleanup_current_msg();
                TXN_TO_STAGE(ABORTING);
            } else if (_msg->get_type() == Message::PREPARED_COMMIT || _msg->get_type() == Message::COMMITTED) {
                uint32_t prepared_part = transport->get_node_partition(_msg->get_src_node_id());
                prepared_remote_parts.insert(prepared_part);
                cleanup_current_msg();
                if (prepared_remote_parts.size() == remote_parts_involved.size() && !waiting_log_flush())
                    TXN_TO_STAGE(COMMITTING);
            } 
          #if BYPASS_LEADER_DECIDE
            else if (_msg->get_type() == Message::APPEND_ENTRIES_RESP) {
                uint32_t part_id = transport->get_node_partition(_msg->get_src_node_id());
                cleanup_current_msg();
                num_append_resps[part_id] += 1;
                if (num_append_resps[part_id] == transport->get_partition_quorum_size(part_id) - 1) {
                    prepared_remote_parts.insert(part_id);
                    if (prepared_remote_parts.size() == remote_parts_involved.size() && !waiting_log_flush()) {
                        TXN_TO_STAGE(COMMITTING);
                    }
                }
            }
          #endif
        #endif
            else
                assert(false);
        } else if (_log != NULL) {
            // continue execution after flushing log
            assert(_log != NULL && _log->type == LOG_PREPARE);
            cleanup_current_log();
            _waiting_log_flush = false;
            send_2pc_vote(rc);
        #if BYPASS_COORD_CONSENSUS
            if (prepared_remote_parts.size() == remote_parts_involved.size())
                TXN_TO_STAGE(COMMITTING);
        #endif
        #if BYPASS_LEADER_DECIDE && !BYPASS_COORD_CONSENSUS
            if (_msg != NULL) {
                assert(_msg->get_type() == Message::COMMIT_REQ);
                TXN_TO_STAGE(COMMITTING);
            }
        #endif
        }

        assert(rc == RCOK);

    #if BYPASS_COORD_CONSENSUS 
        if (!pending_msgs.empty()) {
            assert(_msg == NULL);
            _msg = pending_msgs.front();
            pending_msgs.pop_front();
            TXN_TO_STAGE(PREPARING);
        }
    #endif

        return rc;
    }

    TXN_STAGE(COMMITTING)
    {
        if (!waiting_log_flush()) {
            _commit_start_time = get_sys_clock();
        #if !BYPASS_COORD_CONSENSUS
            assert(_msg != NULL || _cc_manager->is_read_only());
        #endif
            if (_msg != NULL) {
                assert(_msg->get_type() == Message::COMMIT_REQ);
                cleanup_current_msg();
            }

            write_log();
        #if !BYPASS_COMMIT_CONSENSUS
            if (waiting_log_flush())
                return RCOK;
        #endif

            _cc_manager->process_commit_req(COMMIT, 0, NULL);

        #if !BYPASS_COORD_CONSENSUS
            if (!_cc_manager->is_read_only())
                send_2pc_ack();
        #endif
        } else {
            // take care of some left over messages after we bypass some stages or when we commit early as we are readonly
            if (_msg != NULL) {
                assert(_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT
                       || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP ||
                       (_msg->get_type() == Message::ABORT_REQ && _cc_manager->is_read_only()));
                cleanup_current_msg();
                return RCOK;
            }
            // continue after flushing log
            assert(_log != NULL);
            _waiting_log_flush = false;
            cleanup_current_log();

            _cc_manager->process_commit_req(COMMIT, 0, NULL);

        #if !BYPASS_COORD_CONSENSUS
            if (!_cc_manager->is_read_only())
                send_2pc_ack();
        #endif
        } 
        _finish_time = get_sys_clock();
        _txn_state = COMMITTED;
        return COMMIT;
    }

    TXN_STAGE(ABORTING)
    {
        write_log(); // abort log can be async
        _cc_manager->process_commit_req(ABORT, 0, NULL);
        if (_msg)
            cleanup_current_msg();
        _finish_time = get_sys_clock();
        _txn_state = ABORTED;
        return ABORT;
    }

    TXN_STAGE(ABORTED)
    {
        if (_msg != NULL) {
            assert(_msg->get_type() != Message::PREPARE_REQ && _msg->get_type() != Message::COMMIT_REQ);
            cleanup_current_msg();
        } else if (_log != NULL) {
            if (_log->type == LOG_ABORT) {
                cleanup_current_log();
                _waiting_log_flush = false;
            }
        }
        return RCOK;
    }

    TXN_STAGE(COMMITTED)
    {
        // take care of some left over messages after we bypass some stages
        if (_msg != NULL) {
            assert(_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT ||
                   _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP ||
                   (_msg->get_type() == Message::ABORT_REQ && _cc_manager->is_read_only()));
            cleanup_current_msg();
            return RCOK;
        }
        assert(_log != NULL && _log->type == LOG_DIST_COMMIT);
        cleanup_current_log();
        _waiting_log_flush = false;
        return RCOK;
    }

    assert(false);
}
#endif

#if COMMIT_PROTOCOL == TAPIR
RC
TxnManager::coordinator_execute()
{
    TXN_STAGE_BEGIN;

    RC rc = RCOK;

    TXN_STAGE(WAITING_TO_START)
    {
        assert(_msg == NULL && _log == NULL);
        _txn_restart_time = get_sys_clock();
        if (_num_aborts == 0)
            _txn_start_time = _txn_restart_time;
        TXN_TO_STAGE(RUNNING);
    }

    TXN_STAGE(RUNNING)
    {
        if (_msg == NULL) {
            if (_cc_manager->is_signal_abort())
                TXN_TO_STAGE(ABORTING);

            uint64_t tt = get_sys_clock();
            if (waiting_local_requests())
                rc = _cc_manager->process_requests();
            if (rc == RCOK && !waiting_remote())
                rc = _store_procedure->execute();

            if (rc == WAIT)
                _lock_wait_start_time = get_sys_clock();

            INC_FLOAT_STATS(logic, get_sys_clock() - tt);
            if (rc == ABORT)
                TXN_TO_STAGE(ABORTING);
            if (rc == RCOK && !waiting_remote())
                TXN_TO_STAGE(PREPARING);
        } else {
            assert(waiting_remote());
            assert(_msg->get_type() == Message::RESP_COMMIT || _msg->get_type() == Message::RESP_ABORT);

            ATOM_SUB_FETCH(_num_resp_expected, 1);
            if (_msg->get_type() == Message::RESP_COMMIT) {
                assert(_msg->get_data_size() > 0);
                _cc_manager->process_remote_resp(_msg->get_src_node_id(), _msg->get_data_size(), _msg->get_data());
                cleanup_current_msg();
                if (!waiting_remote() && !waiting_local_requests())
                    TXN_TO_STAGE(RUNNING);
            } else if (_msg->get_type() == Message::RESP_ABORT) {
                remote_parts_involved.erase(transport->get_node_partition(_msg->get_src_node_id()));
                _remote_txn_abort = true;
                cleanup_current_msg();
                TXN_TO_STAGE(ABORTING);
            }
        }
        return rc;
    }

    TXN_STAGE(PREPARING)
    {
        if (!waiting_remote() && !waiting_log_flush()) {
            assert(_msg == NULL);
            if (_prepare_start_time == 0)
                _prepare_start_time = get_sys_clock();
            rc = _cc_manager->process_prepare_phase_coord();

            if (rc == WAIT) {
                _cc_manager->_waiting_requests = true;
                _lock_wait_start_time = get_sys_clock();
                assert(CC_ALG == DOCC);
                return rc;
            }
            _cc_manager->_waiting_requests = false;

            if (rc == ABORT)
                TXN_TO_STAGE(ABORTING);

            assert(rc == RCOK);
            if (_mp_txn) {
                send_2pc_prepare();
                VoteStat & vote_stat = partition_votes[g_part_id];
                vote_stat.num_commits += 1;
                vote_stat.leader_vote = VOTE_COMMIT;
                uint32_t super_quorum_size = transport->get_partition_super_quorum_size(g_part_id);
                uint32_t simple_quorum_size = transport->get_partition_quorum_size(g_part_id);
                if (vote_stat.num_commits >= simple_quorum_size) {
                    num_undetermined_partitions -= 1;
                    determined_remote_parts.insert(g_part_id);
                }
                if (vote_stat.num_commits >= super_quorum_size) {
                    _num_resp_expected -= 1;
                    prepared_remote_parts.insert(g_part_id);
                }
            }

            if (!waiting_remote() && !waiting_log_flush())
                TXN_TO_STAGE(COMMITTING);
        } else if (_msg != NULL) {
            assert(_msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP || _msg->get_type() == Message::PREPARED_COMMIT || _msg->get_type() == Message::PREPARED_ABORT);

            uint32_t node_id = _msg->get_src_node_id();
            uint32_t part_id = transport->get_node_partition(node_id);

            if (_msg->get_type() == Message::APPEND_ENTRIES_RESP) {
                VoteStat &vote_stat = partition_votes[part_id];
                uint32_t super_quorum_size = transport->get_partition_super_quorum_size(part_id);
                uint32_t simple_quorum_size = transport->get_partition_quorum_size(part_id);

                if (_msg->get_data_size() > 0) {
                    VoteType vote = *(VoteType*)_msg->get_data();
                    if (vote == VOTE_COMMIT)
                        vote_stat.num_commits += 1;
                    else if (vote == VOTE_ABORT)
                        vote_stat.num_aborts += 1;
                    else
                        assert(false);

                    if (node_id == transport->get_partition_leader(part_id))
                        vote_stat.leader_vote = vote;

                    if (vote_stat.num_aborts >= simple_quorum_size ||
                        ((vote_stat.num_aborts + vote_stat.num_commits) == transport->get_partition_nodes(g_part_id).size() && vote_stat.num_aborts >= vote_stat.num_commits)) {
                        rc = ABORT;
                        _remote_txn_abort = true;
                        _cc_manager->process_prepare_resp(ABORT, 0, NULL);
                        aborted_remote_parts.insert(part_id);
                        cleanup_current_msg();
                        TXN_TO_STAGE(ABORTING);
                    }

                    if (vote_stat.num_commits < super_quorum_size/* || vote_stat.leader_vote == VOTE_NOT_RECEIVED*/) {
                        cleanup_current_msg();
                        if (vote_stat.num_commits >= simple_quorum_size/* && vote_stat.leader_vote == VOTE_COMMIT*/) {
                            if (determined_remote_parts.find(part_id) == determined_remote_parts.end()) {
                                determined_remote_parts.insert(part_id);
                                num_undetermined_partitions -= 1;
                                if (num_undetermined_partitions == 0) {
                                    // start slow path
                                    if (prepared_remote_parts.find(g_part_id) == prepared_remote_parts.end())
                                        write_log();
                                    send_slow_path();
                                }
                            }
                        }
                        return RCOK;
                    }
                    assert(vote_stat.num_commits >= super_quorum_size/* && vote_stat.leader_vote == VOTE_COMMIT*/);
                }
            } else if (_msg->get_type() == Message::COMMITTED) {
                readonly_remote_parts.insert(part_id);
            } else if (_msg->get_type() == Message::PREPARED_COMMIT) {
                // the partition is prepared
            } else if (_msg->get_type() == Message::PREPARED_ABORT) {
                aborted_remote_parts.insert(part_id);
                cleanup_current_msg();
                TXN_TO_STAGE(ABORTING);
            } else
                assert(false);

            // part_id is prepared
            cleanup_current_msg();
            if (prepared_remote_parts.find(part_id) == prepared_remote_parts.end()) {
                prepared_remote_parts.insert(part_id);
                if (ATOM_SUB_FETCH(_num_resp_expected, 1) == 0) {
                    // do not wait for slow path
                    _waiting_log_flush = false;
                    TXN_TO_STAGE(COMMITTING);
                }
            }
        } else if (_log != NULL) {
            // my slow path succeed
            cleanup_current_log();
            _waiting_log_flush = false;
            if (ATOM_SUB_FETCH(_num_resp_expected, 1) == 0) {
                TXN_TO_STAGE(COMMITTING);
            }
        }

        return rc;
    }

    TXN_STAGE(COMMITTING)
    {
        if (!waiting_remote() && !waiting_log_flush()) {
            assert(_msg == NULL && _log == NULL);
            assert(!waiting_remote() && !waiting_local_requests());
            _commit_start_time = get_sys_clock();

            if (_mp_txn) {
                send_2pc_decide(COMMIT);
                _cc_manager->process_commit_phase_coord(COMMIT);
                _txn_state = COMMITTED;
                _finish_time = get_sys_clock();
                return COMMIT;
            } else {
                write_log();
                assert(waiting_log_flush());
                return RCOK;
            }
        } else if (_log != NULL) {
            // leftover messages
            if (_msg != NULL) {
                assert(_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT
                        || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP);
                cleanup_current_msg();
                return RCOK;
            }

            assert(_log != NULL);
            if (_log->type == LOG_PREPARE) {
                // the parallel slow path finished, but we can ignore now
                cleanup_current_log();
                return RCOK;
            }
            assert(_log->type == LOG_LOCAL_COMMIT);
            _waiting_log_flush = false;
            cleanup_current_log();
            assert(!_mp_txn);
            _cc_manager->process_commit_phase_coord(COMMIT);
            _txn_state = COMMITTED;
            _finish_time = get_sys_clock();
            return COMMIT;
        }
    }

    TXN_STAGE(ABORTING)
    {
        _commit_start_time = get_sys_clock();
        if (_mp_txn)
            send_2pc_decide(ABORT);
        _store_procedure->txn_abort();
        _cc_manager->process_commit_phase_coord(ABORT);
        _cc_manager->_waiting_requests = false;
        _num_resp_expected = 0;
        _txn_state = ABORTED;
        _num_aborts += 1;
        _finish_time = get_sys_clock();
        return ABORT;
    }

    TXN_STAGE(COMMITTED)
    {
        // although committed, may need to process some async messages
        if (_msg != NULL) {
             if (_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT 
                 || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP) {
                cleanup_current_msg();
                return RCOK;
             }
            assert(_msg->get_type() == Message::ACK);
            ATOM_SUB_FETCH(_num_resp_expected, 1);
            cleanup_current_msg();
        } else if (_log != NULL) {
            assert(_log->type == LOG_DIST_COMMIT || _log->type == LOG_PREPARE);
            cleanup_current_log();
            _waiting_log_flush = false;
        }
        return RCOK;
    }

    TXN_STAGE(ABORTED)
    {
        if (_msg != NULL) {
            cleanup_current_msg();
        } else if (_log != NULL) {
            if (_log->type == LOG_ABORT) {
                cleanup_current_log();
                _waiting_log_flush = false;
            }
        } else {
            // wakeup due to geting a lock that we were waiting before abort
            _cc_manager->_waiting_requests = false;
        }

        return RCOK;
    }

    assert(false);
}

RC
TxnManager::participant_execute()
{
    if (_msg) {
        if (_msg->get_type() == Message::PREPARE_REQ || _msg->get_type() == Message::SLOW_PATH) {
            assert(_txn_state == RUNNING || _txn_state == PREPARING);
            coord_part = transport->get_node_partition(_msg->get_src_node_id());
            TXN_TO_STAGE(PREPARING);
        } else if (_msg->get_type() == Message::COMMIT_REQ) {
            assert(_txn_state == PREPARING);
            _waiting_log_flush = false;
            TXN_TO_STAGE(COMMITTING);
        } else if (_msg->get_type() == Message::ABORT_REQ) {
            assert((_txn_state != COMMITTING && _txn_state != COMMITTED) || _cc_manager->is_read_only());
            _waiting_log_flush = false;
            TXN_TO_STAGE(ABORTING);
        } else if (_txn_state < PREPARING && (_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT 
            || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP)) {
            pending_msgs.push_back(_msg);
            _msg = NULL;
            return RCOK;
        }
    }

    if (_log) {
        assert(_log->type == LOG_PREPARE);
        if (g_role != ROLE_LEADER) {
            cleanup_current_log();
            return RCOK;
        }
    }

    TXN_STAGE_BEGIN;
    RC rc = RCOK;
    TXN_STAGE(RUNNING)
    {
        if (_cc_manager->is_signal_abort()) {
            rc = ABORT;
        } else if (waiting_local_requests()) {
            rc = _cc_manager->process_requests();
        } else {
            // new remote request
            char * data = _msg->get_data();
            UnstructuredBuffer buffer(_msg->get_data());
            uint32_t header_size = _cc_manager->process_remote_req_header( &buffer );
            data += header_size;
            rc = _store_procedure->process_remote_req(_msg->get_data_size() - header_size, data);
        }

        if (rc == RCOK) {
            // done processing request
            UnstructuredBuffer resp_buffer;
            _cc_manager->get_resp_data(resp_buffer);
            Message msg(Message::RESP_COMMIT, _src_node_id, get_txn_id(), resp_buffer.size(), resp_buffer.data());
            send_msg(&msg);
            cleanup_current_msg();
        } else if (rc == ABORT) {
            Message msg(Message::RESP_ABORT, _src_node_id, get_txn_id(), 0, NULL);
            send_msg(&msg);
            cleanup_current_msg();
            TXN_TO_STAGE(ABORTING);
        }

        if (rc == WAIT)
            _lock_wait_start_time = get_sys_clock();

        assert(rc == RCOK || rc == ABORT || rc == WAIT);
        return rc;
    }

    TXN_STAGE(PREPARING)
    {
        if (_msg != NULL) {
            assert(_msg != NULL);
            if (_msg->get_type() == Message::PREPARE_REQ) {
                if (_prepare_start_time == 0)
                    _prepare_start_time = get_sys_clock();
                UnstructuredBuffer buffer;
                rc = _cc_manager->process_prepare_req(_msg->get_data_size(), _msg->get_data(), buffer);

                if (rc == WAIT) {
                    _cc_manager->_waiting_requests = true;
                    _lock_wait_start_time = get_sys_clock();
                    return WAIT;
                }
                _cc_manager->_waiting_requests = false;
                cleanup_current_msg();
                send_2pc_fast_vote(rc);

                // if rc == ABORT, release locks, wait for the prepare log or the abort request
                // if rc == RCOK, wait for commit / abort request
                // if rc == COMMIT, do nothing

                if (rc == ABORT) {
                    _cc_manager->release_all_locks();
                    rc = RCOK;
                } if (rc == COMMIT) {
                    TXN_TO_STAGE(COMMITTING);
                }
            } else if (_msg->get_type() == Message::SLOW_PATH) {
                assert(g_role == ROLE_LEADER);
                cleanup_current_msg();
                write_log();
            } else if (_msg->get_type() == Message::COMMIT_REQ) {
                TXN_TO_STAGE(COMMITTING);
            } else if (_msg->get_type() == Message::ABORT_REQ) {
                TXN_TO_STAGE(ABORTING);
            } else if (_msg->get_type() == Message::APPEND_ENTRIES_RESP || _msg->get_type() == Message::PREPARED_COMMIT || _msg->get_type() == Message::PREPARED_ABORT) {
                // participants do not handle these msgs
            } else {
                assert(false);
            }

            if (!pending_msgs.empty()) {
                _msg = pending_msgs.front();
                pending_msgs.pop_front();
                if (_msg->get_type() == Message::COMMIT_REQ) {
                    TXN_TO_STAGE(COMMITTING);
                } else if (_msg->get_type() == Message::ABORT_REQ) {
                    TXN_TO_STAGE(ABORTING);
                } else if (_msg->get_type() == Message::APPEND_ENTRIES_RESP ||
                           _msg->get_type() == Message::PREPARED_COMMIT || _msg->get_type() == Message::PREPARED_ABORT) {
                    // participants do not handle these msgs
                } else
                    assert(false);
            }
            return RCOK;
        } else if (_log != NULL) {
            assert(g_role == ROLE_LEADER);
            cleanup_current_log();
            _waiting_log_flush = false;
            send_2pc_vote(RCOK);
            return RCOK;
        }
    }

    TXN_STAGE(COMMITTING)
    {
        if (_commit_start_time == 0) {
            _commit_start_time = get_sys_clock();
        }

        if (_cc_manager->get_cc_state() == CCManager::CC_PREPARED) {
            if (_msg != NULL)
                _cc_manager->process_commit_req(COMMIT, _msg->get_data_size(), _msg->get_data());
            else
                _cc_manager->process_commit_req(COMMIT, 0, NULL);
        } else {
            // assert(g_role != ROLE_LEADER);
            rc = _cc_manager->force_write_set();
        }

        if (_msg != NULL) {
            assert(_msg->get_type() == Message::COMMIT_REQ);
            cleanup_current_msg();
        }
        
        assert(rc != ABORT);
        if (rc == WAIT)
            return RCOK;
        _finish_time = get_sys_clock();
        _txn_state = COMMITTED;
        return COMMIT;
    }

    TXN_STAGE(ABORTING)
    {
        if (_msg != NULL) {
            assert(_msg->get_type() == Message::ABORT_REQ);
            cleanup_current_msg();
        } 
        _cc_manager->process_commit_req(ABORT, 0, NULL);
        _finish_time = get_sys_clock();
        _txn_state = ABORTED;
        return ABORT;
    }

    TXN_STAGE(ABORTED)
    {
        if (_msg != NULL) {
            assert(_msg->get_type() != Message::PREPARE_REQ && _msg->get_type() != Message::COMMIT_REQ);
            cleanup_current_msg();
        } else if (_log != NULL) {
            if (_log->type == LOG_ABORT) {
                cleanup_current_log();
                _waiting_log_flush = false;
            }
        } else {
            // wake up due to getting a lock that we were waiting before abort
            _cc_manager->_waiting_requests = false;
        }
        return RCOK;
    }

    TXN_STAGE(COMMITTED)
    {
        // take care of some left over messages after we bypass some stages
        if (_msg != NULL) {
            assert(_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT
                   || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP ||
                   (_msg->get_type() == Message::ABORT_REQ && _cc_manager->is_read_only()));
            cleanup_current_msg();
            return RCOK;
        }
        assert(_log != NULL && (_log->type == LOG_DIST_COMMIT || _log->type == LOG_PREPARE));
        cleanup_current_log();
        _waiting_log_flush = false;
        return RCOK;
    }

    assert(false);
}
#endif

#if COMMIT_PROTOCOL == GPAC
RC
TxnManager::coordinator_execute()
{
    TXN_STAGE_BEGIN;

    RC rc = RCOK;

    TXN_STAGE(WAITING_TO_START)
    {
        assert(_msg == NULL && _log == NULL);
        _txn_restart_time = get_sys_clock();
        if (_num_aborts == 0)
            _txn_start_time = _txn_restart_time;
        TXN_TO_STAGE(RUNNING);
    }

    TXN_STAGE(RUNNING) 
    {
        if (_msg == NULL) {
            if (_cc_manager->is_signal_abort())
                TXN_TO_STAGE(ABORTING);

            uint64_t tt = get_sys_clock();
            if (waiting_local_requests())
                rc = _cc_manager->process_requests();
            if (rc == RCOK && !waiting_remote())
                rc = _store_procedure->execute();
            
            if (rc == WAIT)
                _lock_wait_start_time = get_sys_clock();

            INC_FLOAT_STATS(logic, get_sys_clock() - tt);
            if (rc == ABORT)
                TXN_TO_STAGE(ABORTING);
            if (rc == RCOK && !waiting_remote())
                TXN_TO_STAGE(PREPARING);
        } else {
            assert(waiting_remote());
            assert(_msg->get_type() == Message::RESP_COMMIT || _msg->get_type() == Message::RESP_ABORT);

            ATOM_SUB_FETCH(_num_resp_expected, 1);
            if (_msg->get_type() == Message::RESP_COMMIT) {
                assert(_msg->get_data_size() > 0);
                _cc_manager->process_remote_resp(_msg->get_src_node_id(), _msg->get_data_size(), _msg->get_data());
                cleanup_current_msg();
                if (!waiting_remote() && !waiting_local_requests())
                    TXN_TO_STAGE(RUNNING);
            } else if (_msg->get_type() == Message::RESP_ABORT) {
                remote_parts_involved.erase(transport->get_node_partition(_msg->get_src_node_id()));
                _remote_txn_abort = true;
                cleanup_current_msg();
                TXN_TO_STAGE(ABORTING);
            }
        }
        return rc;
    }

    TXN_STAGE(PREPARING) 
    {
        if (!waiting_remote() && !waiting_log_flush()) {
            assert(_msg == NULL);
            if (_prepare_start_time == 0)
                _prepare_start_time = get_sys_clock();
            rc = _cc_manager->process_prepare_phase_coord();

            if (rc == WAIT) {
                _cc_manager->_waiting_requests = true;
                _lock_wait_start_time = get_sys_clock();
                assert(CC_ALG == DOCC);
                return rc;
            }
            _cc_manager->_waiting_requests = false;

            if (rc == ABORT)
                TXN_TO_STAGE(ABORTING);

            assert(rc == RCOK);
            if (_mp_txn) {
                send_2pc_prepare();
                VoteStat &vote_stat = partition_votes[g_part_id];
                vote_stat.num_commits += 1;
                vote_stat.leader_vote = VOTE_COMMIT;
                uint32_t super_quorum_size = transport->get_partition_super_quorum_size(g_part_id);
                uint32_t simple_quorum_size = transport->get_partition_quorum_size(g_part_id);
                if (vote_stat.num_commits >= simple_quorum_size) {
                    num_undetermined_partitions -= 1;
                    determined_remote_parts.insert(g_part_id);
                }
                if (vote_stat.num_commits >= super_quorum_size) {
                    _num_resp_expected -= 1;
                    prepared_remote_parts.insert(g_part_id);
                }
            }
            if (!waiting_remote() && !waiting_log_flush())
                TXN_TO_STAGE(COMMITTING);
        } else if (_msg != NULL) {
            assert(_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT
                    || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP);

            uint32_t node_id = _msg->get_src_node_id();
            uint32_t part_id = transport->get_node_partition(node_id);

            if (_msg->get_type() == Message::PREPARED_ABORT) {
                rc = ABORT;
                _remote_txn_abort = true;
                _cc_manager->process_prepare_resp(ABORT, _msg->get_src_node_id(), _msg->get_data());
                aborted_remote_parts.insert(part_id);
                cleanup_current_msg();
                TXN_TO_STAGE(ABORTING);
            } else if (_msg->get_type() == Message::APPEND_ENTRIES_RESP) {
                VoteType vote = *(VoteType*)_msg->get_data();
                VoteStat &vote_stat = partition_votes[part_id];
                if (vote == VOTE_COMMIT)
                    vote_stat.num_commits += 1;
                else if (vote == VOTE_ABORT)
                    vote_stat.num_aborts += 1;
                else
                    assert(false);
                if (node_id == transport->get_partition_leader(part_id))
                    vote_stat.leader_vote = vote;

                uint32_t quorum_size = transport->get_partition_quorum_size(part_id);
                if (vote_stat.num_aborts >= quorum_size || (vote_stat.num_aborts + vote_stat.num_commits == transport->get_partition_nodes(part_id).size() && vote_stat.num_aborts >= vote_stat.num_commits)) {
                    rc = ABORT;
                    _remote_txn_abort = true;
                    _cc_manager->process_prepare_resp(ABORT, 0, NULL);
                    aborted_remote_parts.insert(part_id);
                    cleanup_current_msg();
                    TXN_TO_STAGE(ABORTING);
                } else if (vote_stat.num_commits < quorum_size) {
                    cleanup_current_msg();
                    return RCOK;
                }
                assert(vote_stat.num_commits >= quorum_size);
            } else if (_msg->get_type() == Message::COMMITTED) {
                readonly_remote_parts.insert(part_id);
            }
            // part_id is prepared
            cleanup_current_msg();
            if (prepared_remote_parts.find(part_id) == prepared_remote_parts.end()) {
                prepared_remote_parts.insert(part_id);
                if (ATOM_SUB_FETCH(_num_resp_expected, 1) == 0 && !waiting_log_flush())
                    TXN_TO_STAGE(COMMITTING);
            }

        } else if (_log != NULL) {
            assert(false);
            _waiting_log_flush = false;
            cleanup_current_log();
            if (!waiting_remote())
                TXN_TO_STAGE(COMMITTING);
        }

        return rc;
    }

    TXN_STAGE(COMMITTING)
    {
        if (!waiting_remote() && !waiting_log_flush()) {
            assert(_msg == NULL && _log == NULL);
            assert(!waiting_remote() && !waiting_local_requests());
            _commit_start_time = get_sys_clock();

            if (_mp_txn) {
                
                write_log();
                if (waiting_log_flush())
                    return RCOK;
                
                send_2pc_decide(COMMIT);
                _cc_manager->process_commit_phase_coord(COMMIT);
                _txn_state = COMMITTED;
                _finish_time = get_sys_clock();
                return COMMIT;
            } else {
                write_log();
                assert(waiting_log_flush());
                return RCOK;
            }
        } else if (waiting_log_flush()) {
            // leftover messages
            if (_msg != NULL) {
                assert(_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT
                        || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP);
                cleanup_current_msg();
                return RCOK;
            }

            assert(_log != NULL && (_log->type == LOG_LOCAL_COMMIT || _log->type == LOG_DIST_COMMIT));
            _waiting_log_flush = false;
            cleanup_current_log();
            send_2pc_decide(COMMIT);
            _cc_manager->process_commit_phase_coord(COMMIT);
            _txn_state = COMMITTED;
            _finish_time = get_sys_clock();
            return COMMIT;
        }
    }

    TXN_STAGE(ABORTING)
    {
        _commit_start_time = get_sys_clock();
        if (_mp_txn) {
            write_log();
            send_2pc_decide(ABORT);
        }
        _store_procedure->txn_abort();
        _cc_manager->process_commit_phase_coord(ABORT);
        _cc_manager->_waiting_requests = false;
        _num_resp_expected = 0;
        _txn_state = ABORTED;
        _num_aborts += 1;
        _finish_time = get_sys_clock();
        return ABORT;
    }

    TXN_STAGE(COMMITTED)
    {
        // although committed, may need to process some async messages
        if (_msg != NULL) {
            if (_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT 
                || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP) {
            cleanup_current_msg();
            return RCOK;
            }
            assert(_msg->get_type() == Message::ACK);
            ATOM_SUB_FETCH(_num_resp_expected, 1);
            cleanup_current_msg();
        } else if (_log != NULL) {
            assert(_log->type == LOG_DIST_COMMIT);
            cleanup_current_log();
            _waiting_log_flush = false;
        }
        return RCOK;
    }

    TXN_STAGE(ABORTED)
    {
        if (_msg != NULL) {
            cleanup_current_msg();
        } else if (_log != NULL) {
            if (_log->type == LOG_ABORT) {
                cleanup_current_log();
                _waiting_log_flush = false;
            }
        } else {
            // wakeup due to geting a lock that we were waiting before abort
            _cc_manager->_waiting_requests = false;
        }

        return RCOK;
    }

    assert(false);
}

RC
TxnManager::participant_execute()
{
    if (_log) {
        _waiting_log_flush = false;
        cleanup_current_log();
        return RCOK;
    }

    if (_msg) {
        assert(_log == NULL);
        if (_msg->get_type() == Message::PREPARE_REQ) {
            assert(_txn_state == RUNNING || _txn_state == PREPARING);
            coord_part = transport->get_node_partition(_msg->get_src_node_id());
            TXN_TO_STAGE(PREPARING);
        } else if (_msg->get_type() == Message::COMMIT_REQ) {
            if (_txn_state == PREPARING) {
                TXN_TO_STAGE(COMMITTING);
            } else {
                cleanup_current_msg();
                return RCOK;
            }
        } else if (_msg->get_type() == Message::ABORT_REQ) {
            assert((_txn_state != COMMITTING && _txn_state != COMMITTED) || _cc_manager->is_read_only());
            TXN_TO_STAGE(ABORTING);
        } else if (_txn_state < PREPARING && (_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT 
            || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP)) {
            pending_msgs.push_back(_msg);
            _msg = NULL;
            return RCOK;
        }
    }

    TXN_STAGE_BEGIN;
    RC rc = RCOK;
    TXN_STAGE(RUNNING)
    {
        if (_cc_manager->is_signal_abort()) {
            rc = ABORT;
        } else if (waiting_local_requests()) {
            rc = _cc_manager->process_requests();
        } else {
            // new remote request
            char * data = _msg->get_data();
            UnstructuredBuffer buffer(_msg->get_data());
            uint32_t header_size = _cc_manager->process_remote_req_header( &buffer );
            data += header_size;
            rc = _store_procedure->process_remote_req(_msg->get_data_size() - header_size, data);
        }

        if (rc == RCOK) {
            // done processing request
            UnstructuredBuffer resp_buffer;
            _cc_manager->get_resp_data(resp_buffer);
            Message msg(Message::RESP_COMMIT, _src_node_id, get_txn_id(), resp_buffer.size(), resp_buffer.data());
            send_msg(&msg);
            cleanup_current_msg();
        } else if (rc == ABORT) {
            Message msg(Message::RESP_ABORT, _src_node_id, get_txn_id(), 0, NULL);
            send_msg(&msg);
            cleanup_current_msg();
            TXN_TO_STAGE(ABORTING);
        }

        if (rc == WAIT)
            _lock_wait_start_time = get_sys_clock();

        assert(rc == RCOK || rc == ABORT || rc == WAIT);
        return rc;
    }

    TXN_STAGE(PREPARING)
    {
        if (_msg != NULL) {
            if (_prepare_start_time == 0)
                _prepare_start_time = get_sys_clock();
            
            if (_msg->get_type() == Message::PREPARE_REQ) {
                UnstructuredBuffer buffer;
                rc = _cc_manager->process_prepare_req(_msg->get_data_size(), _msg->get_data(), buffer);
                if (rc == WAIT) {
                    _cc_manager->_waiting_requests = true;
                    _lock_wait_start_time = get_sys_clock();
                    return WAIT;
                }
                _cc_manager->_waiting_requests = false;
                cleanup_current_msg();
                if (g_role == ROLE_LEADER && rc == ABORT && _cc_manager->is_read_only()) {
                    send_2pc_vote(ABORT);
                } else {
                    send_2pc_fast_vote(rc);
                }

                // if rc == ABORT, release locks, wait for the prepare log or the abort request
                // if rc == RCOK, wait for commit / abort request
                // if rc == commit, do nothing

                if (rc == ABORT) {
                    _cc_manager->release_all_locks();
                    rc = RCOK;
                } if (rc == COMMIT) {
                    TXN_TO_STAGE(COMMITTING);
                }
            } else if (_msg->get_type() == Message::COMMIT_REQ) {
                TXN_TO_STAGE(COMMITTING);
            } else if (_msg->get_type() == Message::ABORT_REQ) {
                TXN_TO_STAGE(ABORTING);
            } else if (_msg->get_type() == Message::PREPARED_COMMIT || _msg->get_type() == Message::PREPARED_ABORT) {
                // do nothing
            } else
                assert(false);

            if (!pending_msgs.empty()) {
                _msg = pending_msgs.front();
                pending_msgs.pop_front();
                if (_msg->get_type() == Message::COMMIT_REQ) {
                    TXN_TO_STAGE(COMMITTING);
                } else if (_msg->get_type() == Message::ABORT_REQ) {
                    TXN_TO_STAGE(ABORTING);
                } else if (_msg->get_type() == Message::PREPARED_COMMIT || _msg->get_type() == Message::PREPARED_ABORT) {
                    TXN_TO_STAGE(PREPARING);
                } else
                    assert(false);
            }

            return RCOK;
        } 
        assert(false);
    }

    TXN_STAGE(COMMITTING)
    {
        if (_commit_start_time == 0)
            _commit_start_time = get_sys_clock();

        if (_cc_manager->get_cc_state() == CCManager::CC_PREPARED) {
            if (_msg != NULL)
                _cc_manager->process_commit_req(COMMIT, _msg->get_data_size(), _msg->get_data());
            else
                _cc_manager->process_commit_req(COMMIT, 0, NULL);
        } else {
            rc = _cc_manager->force_write_set();
        }

        if (_msg != NULL) {
            assert(_msg->get_type() == Message::COMMIT_REQ);
            cleanup_current_msg();
        }
        
        assert(rc != ABORT);
        if (rc == WAIT)
            return RCOK;
        
        if (g_role == ROLE_LEADER)
            write_log();
        
        _finish_time = get_sys_clock();
        _txn_state = COMMITTED;
        return COMMIT;
    }

    TXN_STAGE(ABORTING)
    {
        if (_msg != NULL) {
            assert(_msg->get_type() == Message::ABORT_REQ);
            cleanup_current_msg();
        } 
        _cc_manager->process_commit_req(ABORT, 0, NULL);
        
        if (g_role == ROLE_LEADER)
            write_log();
        
        _finish_time = get_sys_clock();
        _txn_state = ABORTED;
        return ABORT;
    }

    TXN_STAGE(ABORTED)
    {
        if (_msg != NULL) {
            assert(_msg->get_type() != Message::PREPARE_REQ && _msg->get_type() != Message::COMMIT_REQ);
            cleanup_current_msg();
        } else if (_log != NULL) {
            if (_log->type == LOG_ABORT) {
                cleanup_current_log();
                _waiting_log_flush = false;
            }
        } else {
            // wake up due to getting a lock that we were waiting before abort
            _cc_manager->_waiting_requests = false;
        }
        return RCOK;
    }

    TXN_STAGE(COMMITTED)
    {
        // take care of some left over messages after we bypass some stages
        if (_msg != NULL) {
            assert(_msg->get_type() == Message::PREPARED_ABORT || _msg->get_type() == Message::PREPARED_COMMIT
                   || _msg->get_type() == Message::COMMITTED || _msg->get_type() == Message::APPEND_ENTRIES_RESP ||
                   (_msg->get_type() == Message::ABORT_REQ && _cc_manager->is_read_only()));
            cleanup_current_msg();
            return RCOK;
        }
        assert(_log != NULL && _log->type == LOG_DIST_COMMIT);
        cleanup_current_log();
        _waiting_log_flush = false;
        return RCOK;
    }

    assert(false);
}
#endif

RC
TxnManager::applier_execute()
{
    if (_log->type == LOG_PREPARE)
        _mp_txn = true;
    else if (_log->type == LOG_LOCAL_COMMIT)
        _mp_txn = false;

    RC rc = _cc_manager->apply_log_record(_log);
    if (rc == COMMIT)
        _txn_state = COMMITTED;
    else if (rc == ABORT)
        _txn_state = ABORTED;
    return rc;
}

RC
TxnManager::calvin_execute()
{
    RC rc = RCOK;

    if (g_role == ROLE_LEADER) {
        if (_msg == NULL) {
            rc = _store_procedure->execute();
            if (rc == RCOK && !waiting_remote())
                goto calvin_commit;
            else
                return WAIT;
        } else if (waiting_remote() && _msg != NULL) {
            assert(_msg->get_type() == Message::RESP_COMMIT);

            ATOM_SUB_FETCH(_num_resp_expected, 1);
            _cc_manager->process_remote_resp(_msg->get_src_node_id(), _msg->get_data_size(), _msg->get_data());
            cleanup_current_msg();
            if (waiting_remote())
                return WAIT;
        }
    calvin_commit:
        _cc_manager->process_commit_phase_coord(COMMIT);

        // send msg to others in local DC to commit the txn
        for (uint32_t node : local_nodes_involved) {
            Message msg(Message::COMMIT_REQ, node, get_txn_id(), 0, NULL);
            send_msg(&msg);
        }

        return COMMIT;
    } else if (g_role == ROLE_REPLICA) {
        assert(_msg != NULL);
        if (_msg->get_type() == Message::REQ) {
            char * data = _msg->get_data();
            UnstructuredBuffer buffer(_msg->get_data());
            uint32_t header_size = _cc_manager->process_remote_req_header( &buffer );
            data += header_size;
            rc = _store_procedure->process_remote_req(_msg->get_data_size() - header_size, data);
            assert(rc == RCOK);

            UnstructuredBuffer resp_buffer;
            _cc_manager->get_resp_data(resp_buffer);
            Message msg(Message::RESP_COMMIT, _src_node_id, get_txn_id(), resp_buffer.size(), resp_buffer.data());
            send_msg(&msg);
            cleanup_current_msg();

            return RCOK;
        } else if (_msg->get_type() == Message::COMMIT_REQ) {
            _cc_manager->process_commit_req(COMMIT, 0, NULL);
            return COMMIT;
        } else
            assert(false);
    } else
        assert(false);

    return COMMIT;
}

// This function puts the remote message msg into _msg if _msg is NULL
// Otherwise puts it into pending_msgs
RC
TxnManager::process_msg(Message * msg)
{
    // we haven't finished processing the previous msg, add it to pending msgs
    if (_msg != NULL) {
        // currently, pending msgs will only appear in participants, add this invariant for debugging only
        assert(is_sub_txn());
        if (msg->get_type() != Message::ABORT_REQ) {
            pending_msgs.push_back(msg);
            return RCOK;
        }
        cleanup_current_msg();
    }

    if (msg->is_response() && waiting_remote())
        _net_wait_time += get_sys_clock() - _net_wait_start_time;

    _msg = msg;
    _src_node_id = msg->get_src_node_id();

    // Call execute() to process _msg
    return execute();
}

// This function puts the remote log record into _log
RC
TxnManager::process_log(LogRecord * record)
{
    _log = record;
    if (waiting_log_flush())
        _log_total_flush_time += get_sys_clock() - _log_start_flush_time;

    // Call execute() to process _log
    return execute();
}

void
TxnManager::cleanup_current_msg()
{
    if (_msg->get_data_size() > 0)
        delete[] _msg->get_data();
    delete _msg;
    _msg = NULL;
}

void
TxnManager::cleanup_current_log()
{
    if (_log != NULL)
        delete[] (char *)_log;
    _log = NULL;
}

void
TxnManager::send_msg(Message * msg)
{
    assert(msg->get_dest_id() != g_node_id);
    _net_wait_start_time = get_sys_clock();
    _msg_count[msg->get_type()] ++;
    _msg_size[msg->get_type()] += msg->get_packet_len();
    transport->send_msg(msg);
    INC_FLOAT_STATS(time_write_output_queue, get_sys_clock() - _net_wait_start_time);
}

void
TxnManager::schedule_retry() 
{
    uint32_t backoff_times = _num_aborts;
    if (backoff_times > MAX_BACKOFF_TIMES)
        backoff_times = MAX_BACKOFF_TIMES;

    // No exponential backoff for distributed txns
    uint64_t penalty;
    if (is_mp_txn())
        penalty = g_abort_penalty;
    else
        penalty = g_abort_penalty * (1 << (backoff_times - 1));
    // Randomized backoff
    _ready_time = get_sys_clock() + penalty * glob_manager->rand_double();
}

void
TxnManager::update_stats()
{
    if (is_sub_txn()) {
        if (_txn_state == COMMITTED) {
            INC_INT_STATS(num_remote_commits, 1);
            INC_REMOTE_FLOAT_STATS(wait, _lock_wait_time);
            INC_REMOTE_FLOAT_STATS(prepare, _commit_start_time - _prepare_start_time);
            INC_REMOTE_FLOAT_STATS(commit, _finish_time - _commit_start_time);
        }
        return;
    }

    if (_mp_txn) {
        INC_INT_STATS(num_home_mp, 1);
        if (_txn_state == COMMITTED)
            INC_INT_STATS(num_home_mp_commits, 1);
    } else {
        INC_INT_STATS(num_home_sp, 1);
        if (_txn_state == COMMITTED)
            INC_INT_STATS(num_home_sp_commits, 1);
    }

#if WORKLOAD == TPCC && STATS_ENABLE
    if (is_coord()) {
        uint32_t type = ((QueryTPCC *)_store_procedure->get_query())->type;
        if (_txn_state == COMMITTED) {
            stats->_stats[GET_THD_ID]->_commits_per_txn_type[type]++;
            stats->_stats[GET_THD_ID]->_latency_per_txn_type[type] += _finish_time - _txn_start_time;
        } else if (_txn_state == ABORTED)
            stats->_stats[GET_THD_ID]->_aborts_per_txn_type[type]++;
    }
#elif WORKLOAD == TPCE && STATS_ENABLE
    if (is_coord()) {
        uint32_t type = ((QueryTPCE *)_store_procedure->get_query())->type;
        if (_txn_state == COMMITTED) {
            stats->_stats[GET_THD_ID]->_commits_per_txn_type[type]++;
            stats->_stats[GET_THD_ID]->_latency_per_txn_type[type]         += _finish_time - _txn_start_time;
            stats->_stats[GET_THD_ID]->_execution_phase_per_txn_type[type] += _prepare_start_time - _txn_restart_time;
            stats->_stats[GET_THD_ID]->_prepare_phase_per_txn_type[type]   += _commit_start_time - _prepare_start_time;
            stats->_stats[GET_THD_ID]->_commit_phase_per_txn_type[type]    += _finish_time - _commit_start_time;
            stats->_stats[GET_THD_ID]->_abort_time_per_txn_type[type]      += _txn_restart_time - _txn_start_time;
            if (_mp_txn) {
                stats->_stats[GET_THD_ID]->_mp_commits_per_txn_type[type]++;
                stats->_stats[GET_THD_ID]->_mp_latency_per_txn_type[type]         += _finish_time - _txn_start_time;
                stats->_stats[GET_THD_ID]->_mp_execution_phase_per_txn_type[type] += _prepare_start_time - _txn_restart_time;
                stats->_stats[GET_THD_ID]->_mp_prepare_phase_per_txn_type[type]   += _commit_start_time - _prepare_start_time;
                stats->_stats[GET_THD_ID]->_mp_commit_phase_per_txn_type[type]    += _finish_time - _commit_start_time;
                stats->_stats[GET_THD_ID]->_mp_abort_time_per_txn_type[type]      += _txn_restart_time - _txn_start_time;
            }
        } else if (_txn_state == ABORTED) {
            stats->_stats[GET_THD_ID]->_aborts_per_txn_type[type]++;
            if (_mp_txn) {
                stats->_stats[GET_THD_ID]->_mp_aborts_per_txn_type[type]++;
            }
        }
    }
#endif

    if ( _txn_state == COMMITTED ) {
        uint64_t latency = _finish_time - _txn_start_time;

        INC_INT_STATS(num_commits, 1);
        INC_FLOAT_STATS(txn_latency, latency);
        INC_FLOAT_STATS(execute_phase, _prepare_start_time - _txn_restart_time);
        INC_FLOAT_STATS(prepare_phase, _commit_start_time - _prepare_start_time);
        INC_FLOAT_STATS(commit_phase, _finish_time - _commit_start_time);
        INC_FLOAT_STATS(abort, _txn_restart_time - _txn_start_time);

        INC_FLOAT_STATS(wait, _lock_wait_time);
        INC_FLOAT_STATS(network, _net_wait_time);
        INC_FLOAT_STATS(time_write_log, _log_total_flush_time);
        INC_FLOAT_STATS(time_debug1, _time_debug1);
        INC_FLOAT_STATS(time_debug2, _time_debug2);
        INC_FLOAT_STATS(time_debug3, _time_debug3);
        INC_FLOAT_STATS(time_debug4, _time_debug4);

    #if COLLECT_LATENCY
        LatencyEntry entry;
        entry.latency           = latency;
        entry.execution_latency = _prepare_start_time - _txn_restart_time;
        entry.prepare_latency   = _commit_start_time - _prepare_start_time;
        entry.commit_latency    = _finish_time - _commit_start_time;
        entry.abort             = _txn_restart_time - _txn_start_time;
        entry.network           = _net_wait_time;
        entry.abort_count       = _num_aborts;

        vector<LatencyEntry> & all = stats->_stats[GET_THD_ID]->all_latency;
        all.push_back(entry);
        if (!_mp_txn) {
            vector<LatencyEntry> & sp = stats->_stats[GET_THD_ID]->sp_latency;
            sp.push_back(entry);
        } else {
            vector<LatencyEntry> & mp = stats->_stats[GET_THD_ID]->mp_latency;
            mp.push_back(entry);
        }
    #endif

        if (!is_sub_txn()) {
            for (uint32_t i = 0; i < Message::NUM_MSG_TYPES; i ++) {
                if (i == Message::PREPARED_ABORT)
                    M_ASSERT(_msg_count[i] == 0, "txn=%ld\n", get_txn_id());
                stats->_stats[GET_THD_ID]->_msg_committed_count[i] += _msg_count[i];
                stats->_stats[GET_THD_ID]->_msg_committed_size[i] += _msg_size[i];
            }
        }

        if (_mp_txn) {
            INC_MP_FLOAT_STATS(txn_latency, latency);
            INC_MP_FLOAT_STATS(execute_phase, _prepare_start_time - _txn_restart_time);
            INC_MP_FLOAT_STATS(prepare_phase, _commit_start_time - _prepare_start_time);
            INC_MP_FLOAT_STATS(commit_phase, _finish_time - _commit_start_time);
            INC_MP_FLOAT_STATS(abort, _txn_restart_time - _txn_start_time);
            INC_MP_FLOAT_STATS(retry, _num_aborts);

            INC_MP_FLOAT_STATS(wait, _lock_wait_time);
            INC_MP_FLOAT_STATS(network, _net_wait_time);
        } else {
            INC_SP_FLOAT_STATS(txn_latency, latency);
            INC_SP_FLOAT_STATS(execute_phase, _prepare_start_time - _txn_restart_time);
            INC_SP_FLOAT_STATS(prepare_phase, _commit_start_time - _prepare_start_time);
            INC_SP_FLOAT_STATS(commit_phase, _finish_time - _commit_start_time);
            INC_SP_FLOAT_STATS(abort, _txn_restart_time - _txn_start_time);
            INC_SP_FLOAT_STATS(retry, _num_aborts);

            INC_SP_FLOAT_STATS(wait, _lock_wait_time);
            INC_SP_FLOAT_STATS(network, _net_wait_time);
        }
    } else if ( _txn_state == ABORTED ) {
        INC_INT_STATS(num_aborts, 1);
        if (_remote_txn_abort)
            INC_INT_STATS(num_aborts_remote, 1);
    } else
        assert(false);
}
