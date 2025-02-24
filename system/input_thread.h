#pragma once

#include "global.h"
#include "thread.h"
#include "message.h"

class Transport;

class InputThread : public Thread
{
public:
    InputThread(uint64_t thd_id, Transport * transport);
    RC run();
    void signal_to_stop();

private:
    void        dealwithMsg(Message * msg);

    Transport * _transport;
    int         _signal_fd;
};
