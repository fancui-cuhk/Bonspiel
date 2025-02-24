#include "hotness_thread.h"
#include "global.h"
#include "helper.h"
#include "transport.h"
#include "message.h"
#include "packetize.h"
#include "manager.h"

HotnessThread::HotnessThread(uint64_t thd_id, uint32_t ins_id)
    : Thread(thd_id, HOTNESS_THREAD)
{
    _stop_signal = false;
    _ins_id = ins_id;
}

void
HotnessThread::signal_to_stop()
{
    _stop_signal = true;
}

RC
HotnessThread::run()
{
    glob_manager->init_rand(get_thd_id());
    glob_manager->set_thd_id(get_thd_id());
    assert(glob_manager->get_thd_id() == get_thd_id());
#if SET_AFFINITY
    pin_thread_by_id(pthread_self(), _thd_id);
#endif
    pthread_barrier_wait(&global_barrier);

    // This thread periodically sends the local hotness table to other leaders
    // It only works on leaders
    if (!HOTNESS_ENABLE || g_node_id != transport->get_partition_leader(g_part_id))
        return RCOK;

    Message * msg = NULL;
    uint64_t last_send = get_sys_clock();
    InOutQueue * msg_queue = hotness_msg_queues[_ins_id];

    while (!_stop_signal) {
        uint64_t t0 = get_sys_clock();
        uint64_t elapsed = (t0 - last_send) / BILLION;

        if (elapsed > g_hotness_interval) {
            _send_hotness_table();
            last_send = t0;
        }

        while (msg_queue->pop(msg)) {
            assert(msg->get_type() == Message::HOTNESS);
            uint32_t src_node_id = msg->get_src_node_id();
            uint32_t src_part_id = transport->get_node_partition(src_node_id);
            cout << "Received remote hotness table from partition " << src_part_id << endl;
            UnstructuredBuffer buffer(msg->get_data());
            glob_manager->apply_remote_hotness_table(src_part_id, buffer);
        }

        PAUSE10;
    }

    return RCOK;
}

void
HotnessThread::_send_hotness_table()
{
    UnstructuredBuffer buffer;
    uint32_t size = glob_manager->get_local_hotness_table(buffer);
    assert(buffer.size() == size);
    for (uint32_t part_id = 0; part_id < g_num_parts; part_id++) {
        if (part_id == g_part_id)
            continue;
        uint32_t part_leader = transport->get_partition_leader(part_id);
        Message hotness_msg(Message::HOTNESS, part_leader, _ins_id, buffer.size(), buffer.data());
        transport->send_msg(&hotness_msg);
        cout << "Local hotness table sent to partition " << part_id << endl;
    }
}
