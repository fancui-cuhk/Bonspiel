#pragma once

#include "global.h"
#include "thread.h"
#include "message.h"

class workload;
class QueryBase;
class Transport;
class TxnManager;
class Message;

class CalvinWorkerThread : public Thread
{
public:
    CalvinWorkerThread(uint64_t thd_id);
    RC run();
};
