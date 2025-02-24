#pragma once

#include "global.h"
#include "thread.h"
#include "message.h"

class HotnessThread : public Thread
{
public:
    HotnessThread(uint64_t thd_id, uint32_t ins_id);
    RC run();
    void signal_to_stop();

private:
    bool     _stop_signal;
    uint32_t _ins_id;

    void _send_hotness_table();
};
