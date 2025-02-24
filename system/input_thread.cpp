#include "input_thread.h"
#include "global.h"
#include "helper.h"
#include "message.h"
#include "transport.h"
#include "workload.h"
#include "manager.h"
#include "server_thread.h"
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include "raft_thread.h"

InputThread::InputThread(uint64_t thd_id, Transport *transport)
    : Thread(thd_id, INPUT_THREAD)
{
    _transport = transport;
    _signal_fd = eventfd(0, 0);
    assert(_signal_fd > 0);
}

void
InputThread::signal_to_stop()
{
    assert(_signal_fd > 0);
    uint64_t signal = 1;
    write(_signal_fd, &signal, sizeof(signal));
}

void InputThread::dealwithMsg(Message * msg)
{
    assert(msg != NULL);
    if (msg->get_type() == Message::DUMMY) {
        delete msg;
        return;
    }
    INC_FLOAT_STATS(bytes_received, msg->get_packet_len());
    stats->_stats[GET_THD_ID]->_msg_count[msg->get_type()] ++;
    stats->_stats[GET_THD_ID]->_msg_size[msg->get_type()] += msg->get_packet_len();
    if (msg->get_type() == Message::TERMINATE) {
        uint32_t total_terminated = ATOM_ADD_FETCH(num_terminated_nodes, 1);
        delete msg;
        if (total_terminated == g_num_nodes - 1)
            glob_manager->set_remote_done();
        return;
    }

    if (msg->get_type() == Message::PING) {
        uint64_t start = *(uint64_t*) msg->get_data();
        Message resp(Message::PONG, msg->get_src_node_id(), 0, sizeof(start), (char*)&start);
        transport->send_msg(&resp);
        delete msg;
        return;
    } 

    if (msg->get_type() == Message::PONG) {
        uint64_t start = *(uint64_t*)msg->get_data();
        uint64_t end = get_sys_clock();
        uint32_t node_id = msg->get_src_node_id();
        uint32_t part_id = _transport->get_node_partition(node_id);
        uint64_t &lat = _transport->_ping_latencies[node_id];
        assert(end >= start);
        lat = lat * (1 - LATENCY_LOW_PASS) + (end - start) * LATENCY_LOW_PASS;
        if (lat < _transport->_ping_latencies[_transport->_partition_closest_node[part_id]])
            _transport->_partition_closest_node[part_id] = node_id;
        delete msg;
        return;
    }

    if (msg->get_type() == Message::HOTNESS) {
        hotness_msg_queues[msg->get_txn_id()]->push(msg);
        return;
    }

    if (msg->get_type() >= Message::RAFT_REQUESTVOTE) {
        raft_msg_queues[msg->get_txn_id()]->push(msg); 
        return;
    }

    uint32_t queue_id = 0;
    queue_id = glob_manager->txnid_to_server_thread(msg->get_txn_id());
    input_queues[queue_id]->push(msg);
#if DEBUG_MSG_WAIT_INQUEUE
    msg_arrival_times->push(get_sys_clock());
#endif
}

RC
InputThread::run()
{
    glob_manager->init_rand( get_thd_id() );
    glob_manager->set_thd_id( get_thd_id() );
    //pin_thread_by_id(pthread_self(), _thd_id);
    pthread_barrier_wait( &global_barrier );

    if (g_num_nodes == 1) {
        cout << "running on single node\n";
        return RCOK;
    }

    uint32_t total_num_sockets = g_num_nodes - 1;
    uint32_t num_responsible_sockets = (total_num_sockets + g_num_input_threads - 1) / g_num_input_threads;
    uint32_t input_thd_id = _thd_id % g_num_input_threads;

    int epoll_fd = epoll_create(num_responsible_sockets + 1);
    assert(epoll_fd > 0);
    for (uint64_t node_id = 0; node_id < g_num_nodes; node_id++) {
        if (node_id == g_node_id) continue;
        if (node_id % g_num_input_threads != input_thd_id) continue;
        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.ptr = new uint32_t(node_id);
        assert(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, _transport->_local_socks[node_id], &ev) == 0);
    }

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.ptr = new uint32_t(g_node_id);
    assert(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, _signal_fd, &ev) == 0);

    struct epoll_event * events = new struct epoll_event[num_responsible_sockets + 1];
    bool stop_signal = false;
    vector<Message*> messages;
    while (!stop_signal)
    {
        uint64_t t1 = get_sys_clock();
        int num_events = epoll_wait(epoll_fd, events, num_responsible_sockets + 1, -1);
        if (num_events == -1)
            continue;
        INC_FLOAT_STATS(time_recv_msg, get_sys_clock() - t1);
        uint64_t t2 = get_sys_clock();
        for (int i = 0; i < num_events; i++) {
            if (events[i].events & EPOLLIN) {
                uint32_t node_id = *(uint32_t*)events[i].data.ptr;
                if (node_id == g_node_id) {
                    stop_signal = true;
                    break;
                }
                _transport->get_messages(node_id, messages);
                for (auto msg : messages)
                    dealwithMsg(msg);
                messages.clear();
            } 
        }
        INC_FLOAT_STATS(time_write_queue, get_sys_clock() - t2);
    }
    return RCOK;
}