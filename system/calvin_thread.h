#pragma once

#include "global.h"
#include "thread.h"
#include "message.h"

class workload;
class QueryBase;
class Transport;
class TxnManager;
class Message;

class CalvinThread : public Thread
{
public:
    CalvinThread(uint64_t thd_id);
    RC           run();
    uint64_t     get_new_txn_id();

private:
    uint32_t     _epoch_id;
    uint64_t     _cur_txn_id;
    TxnManager * _txn_man;
};
