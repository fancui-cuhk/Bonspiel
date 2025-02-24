#pragma once

#include "global.h"
#include "thread.h"
#include "message.h"

class Transport;

class OutputThread : public Thread
{
public:
    OutputThread(uint64_t thd_id, Transport *transport);
    RC run();
    void signal_to_stop();

private:
    Transport * _transport;
    int         _signal_fd;
};

