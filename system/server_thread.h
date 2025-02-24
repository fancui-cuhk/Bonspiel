#pragma once

#include "global.h"
#include <queue>
#include <stack>
#include <list>
#include "thread.h"

class workload;
class QueryBase;
class Transport;
class TxnManager;
class Message;

class ServerThread : public Thread {
public:
    ServerThread(uint64_t thd_id);
    RC run();
    uint64_t     get_new_txn_id();
    TxnManager * get_txn(uint64_t txn_id);
    void         add_txn(TxnManager * txn);
    void         remove_txn(TxnManager * txn);

    //TxnManager * get_native_txn() { return _native_txn; }
private:
    void handle_req_finish(RC rc, TxnManager * &txn_man);
    map<uint64_t, TxnManager *> _txn_table;
    map<uint64_t, TxnManager *> _debug_txn_table;
    set<TxnManager *>    _native_txns;
    set<TxnManager *>    _sub_txns;
    Message *        _msg;
    TxnManager *     _txn_man;
    TxnManager *     _new_txn;
    // So only malloc at the beginning

    uint64_t     _client_node_id;
    uint64_t     _max_num_active_txns;
    uint64_t     _max_txn_id;
    // For timestamp allocation
    uint64_t     _curr_ts;
    bool         already_printed_debug;

    uint64_t     _num_sp_to_gen;
    uint64_t     _num_mp_to_gen;

    TxnManager * get_debug_txn(uint64_t txn_id);
};
//__attribute__ ((aligned(64)));
