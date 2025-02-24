#include <iomanip>
#include "output_thread.h"
#include "global.h"
#include "helper.h"
#include "message.h"
#include "transport.h"
#include "workload.h"
#include "manager.h"
#include <sys/epoll.h>
#include "sys/eventfd.h"
#include "sys/timerfd.h"

OutputThread::OutputThread(uint64_t thd_id, Transport *transport)
    : Thread(thd_id, OUTPUT_THREAD)
{

    _transport = transport;
    _signal_fd = eventfd(0, 0);
}

void
OutputThread::signal_to_stop()
{
    assert(_signal_fd > 0);
    uint64_t signal = 1;
    write(_signal_fd, &signal, sizeof(signal));
}

RC
OutputThread::run()
{
    glob_manager->init_rand( get_thd_id() );
    glob_manager->set_thd_id( get_thd_id() );
    assert( glob_manager->get_thd_id() == get_thd_id() );
#if SET_AFFINITY
    //pin_thread_by_id(pthread_self(), get_thd_id());
#endif
    pthread_barrier_wait( &global_barrier );

    if (g_num_nodes == 1)
        return RCOK;

    uint32_t total_num_sockets = g_num_nodes - 1;
    uint32_t num_responsible_sockets = (total_num_sockets + g_num_output_threads - 1) / g_num_output_threads;
    uint32_t output_thd_id = GET_THD_ID % g_num_output_threads;


    int epoll_fd = epoll_create(num_responsible_sockets + 2);
    int listening_nodes = 0;
    assert(epoll_fd > 0);
    for (uint64_t node_id = 0; node_id < g_num_nodes; node_id++) {
        if (node_id == g_node_id) continue;
        if (node_id % g_num_output_threads != output_thd_id) continue;
        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.ptr = new uint32_t(node_id);
        assert(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, _transport->_send_buffers[node_id].event, &ev) == 0);
        listening_nodes += 1;
    }

    if (listening_nodes == 0)
        return RCOK;

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.ptr = new uint32_t(g_node_id);
    assert(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, _signal_fd, &ev) == 0);

    int timer_fd = timerfd_create(CLOCK_MONOTONIC, 0);
    assert(timer_fd > 0);
    struct itimerspec timer_spec;
    timer_spec.it_interval.tv_nsec = BUFFER_SEND_TIME_THREAS / 2;
    timer_spec.it_interval.tv_sec = 0;
    timer_spec.it_value = timer_spec.it_interval;
    assert(timerfd_settime(timer_fd, 0, &timer_spec, NULL) == 0);
    ev.data.ptr = new uint32_t(-1);
    assert(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, timer_fd, &ev) == 0);

    struct epoll_event * events = new struct epoll_event[num_responsible_sockets + 2];
    bool stop_signal = false;

#if DEBUG_TRANSPORT
    vector<vector<uint64_t>> debug_blocking_time;
    debug_blocking_time.assign(g_num_nodes, vector<uint64_t>());
#endif

    vector<uint64_t> remaining_work;
    remaining_work.assign(g_num_nodes, 0);
    while (!stop_signal) {
        bool timer_triggered = false;
        uint64_t t1 = get_sys_clock();
        int num_events = epoll_wait(epoll_fd, events, num_responsible_sockets + 2, -1);
        INC_FLOAT_STATS(time_output_idle, get_sys_clock() - t1);
        if (num_events == -1)
            continue;

        for (int i = 0; i < num_events; i++) {
            if (events[i].events & EPOLLIN) {
                uint32_t node_id = *(uint32_t*) events[i].data.ptr;
                if (node_id == g_node_id) {
                    uint64_t count;
                    read(_signal_fd, &count, sizeof(count));
                    stop_signal = true;
                    continue;
                } else if (node_id == (uint32_t)-1) {
                    uint64_t count;
                    read(timer_fd, &count, sizeof(count));
                    timer_triggered = true;
                } else {
                    uint64_t count = 0;
                    read(_transport->_send_buffers[node_id].event, &count, sizeof(count));
                    if (!_transport->remote_connection_ok(node_id))
                        continue;
                    remaining_work[node_id] += count;
                    /*
                    for (uint64_t c = 1; c <= count; c++) {
                        uint64_t t2 = get_sys_clock();
                        ssize_t bytes = _transport->send_one_segment(node_id);
                        if (!_transport->remote_connection_ok(node_id))
                            continue;
                    #if DEBUG_TRANSPORT
                        debug_blocking_time[node_id].push_back(get_sys_clock() - t2);
                    #endif
                        assert(bytes > 0);
                        INC_FLOAT_STATS(time_send_msg, get_sys_clock() - t2);
                        INC_FLOAT_STATS(bytes_sent, bytes);
                    }
                    */
                }
            }
        }

        if (timer_triggered) {
            uint64_t curr_time = get_sys_clock();
            for (uint32_t node_id = 0; node_id < g_num_nodes; node_id++) {
                if (node_id == g_node_id) continue;
                if (node_id % g_num_output_threads != output_thd_id) continue;
                if (!_transport->remote_connection_ok(node_id)) continue;
                if (remaining_work[node_id] > 0) continue;
                if (curr_time - _transport->_send_buffers[node_id].last_sent_time > BUFFER_SEND_TIME_THREAS) {
                    if (_transport->seal_current_seg(node_id)) {
                        remaining_work[node_id] += 1;
                        /*
                        uint64_t t2 = get_sys_clock();
                        ssize_t bytes = _transport->send_one_segment(node_id);
                        if (!_transport->remote_connection_ok(node_id)) 
                            continue;
                    #if DEBUG_TRANSPORT
                        debug_blocking_time[node_id].push_back(get_sys_clock() - t2);
                    #endif
                        assert(bytes > 0);
                        INC_FLOAT_STATS(time_send_msg, get_sys_clock() - t2);
                        INC_FLOAT_STATS(bytes_sent, bytes);
                        INC_FLOAT_STATS(dummy_bytes_sent, bytes);
                        */
                    }
                }
            }
        }

        uint64_t t2 = get_sys_clock();
        for (uint32_t node_id = 0; node_id < g_num_nodes; node_id ++) {
            uint64_t &segments = remaining_work[node_id];
            while (segments > 0) {
                ssize_t bytes = 0;
                Transport::SendStatus status = transport->send_one_segment(node_id, bytes);
                if (transport->get_node_dc(node_id) != transport->get_node_dc(g_node_id))
                    INC_FLOAT_STATS(cross_dc_sent, bytes);
                INC_FLOAT_STATS(bytes_sent, bytes);
                if (status == Transport::SEND_FINISHED) {
                    segments -= 1;
                } else if (status == Transport::PARTIALLY_SENT) {
                    break;
                } else if (status == Transport::DISCONNECTED) {
                    segments = 0;
                } else 
                    assert(false);
            }
        }
        INC_FLOAT_STATS(time_send_msg, get_sys_clock() - t2);
    }

#if DEBUG_TRANSPORT
    for (uint32_t node_id = 0; node_id < g_num_nodes; node_id++) {
        if (node_id == g_node_id) continue;
        if (node_id % g_num_output_threads != output_thd_id) continue;
        uint64_t sum = 0;
        for (uint64_t lat : debug_blocking_time[node_id]) {
            sum += lat;
        }
        printf("block thd %u, to %u : %lu\n", output_thd_id, node_id, sum / debug_blocking_time[node_id].size());
    }
#endif
    // flush all the remaining messages
    for (uint32_t node_id = 0; node_id < g_num_nodes; node_id++) {
        if (node_id == g_node_id) continue;
        if (node_id % g_num_output_threads != output_thd_id) continue;
        if (!_transport->remote_connection_ok(node_id)) continue;
        _transport->seal_current_seg(node_id);
        ssize_t bytes = 0;
        Transport::SendStatus status;
        do {
            uint64_t t2 = get_sys_clock();
            status = _transport->send_one_segment(node_id, bytes);
            if (transport->get_node_dc(node_id) != transport->get_node_dc(g_node_id))
                INC_FLOAT_STATS(cross_dc_sent, bytes);
            INC_FLOAT_STATS(time_send_msg, get_sys_clock() - t2);
            INC_FLOAT_STATS(bytes_sent, bytes);
            INC_FLOAT_STATS(dummy_bytes_sent, bytes);

        } while (status == Transport::PARTIALLY_SENT || status == Transport::SEND_FINISHED);
    }
    return RCOK;
}