#pragma once

#include "global.h"
#include <sys/socket.h>
#include <netdb.h>
#include <deque>

class Message;
class InputThread;
class OutputThread;

class Transport
{
public:
    enum SendStatus {
        DISCONNECTED,
        ALREADY_SENT,
        NOT_READY_TO_SEND,
        NOTHING_TO_SEND,
        PARTIALLY_SENT,
        SEND_FINISHED,
    };

    Transport(uint32_t transport_id, char *topo_config_path);
    ~Transport();
    void        test_connect();
    void        sync_terminate();
    void        send_msg(Message * msg); 
    Message *   get_one_message(uint32_t node_id);
    void        get_messages(uint32_t node_id, vector<Message*>& messages, int32_t limit=-1);

    uint32_t                get_my_part_id() { return g_part_id; }
    uint32_t                get_my_node_id() { return g_node_id; }
    uint32_t                get_partition_leader(uint32_t part_id) { return _partition_leaders[part_id]; }
    vector<uint32_t>&       get_partition_nodes(uint32_t part_id) { return _partitions[part_id]; }
    uint32_t                get_node_partition(uint32_t node_id) { return _node_to_part[node_id]; }
    uint32_t                get_node_leader(uint32_t node_id) { return _partition_leaders[_node_to_part[node_id]]; }
    uint32_t                get_node_dc(uint32_t node_id) { return _node_to_dc[node_id]; }
    uint32_t                get_closest_node_of_partition (uint32_t part_id) { /*return _partitions[part_id][0];*/ return _partition_closest_node[part_id]; }
    uint64_t                get_ping_latency(uint32_t node_id) { return _ping_latencies[node_id]; }
    uint64_t                get_partition_quorum_size(uint32_t part_id) { return _partitions[part_id].size() / 2 + 1; }
    bool                    remote_connection_ok(uint32_t node_id) { return _remote_socks_ok[node_id]; }
    uint64_t                get_partition_super_quorum_size(uint32_t part_id) { return (_partitions[part_id].size() * 3 + 3) / 4; }
    friend class InputThread;
    friend class OutputThread;

    // for output thread
    bool        seal_current_seg(uint32_t node_id);
    SendStatus  send_one_segment(uint32_t node_id);
    SendStatus  send_one_segment(uint32_t node_id, ssize_t &sent_size);

#if DEBUG_TRANSPORT
    void        print_debug();
#endif
private:

    union AllocFlag {
        uint64_t all;
        struct {
            uint32_t sealed;
            uint32_t alloc_size;
        } sep;
    };
    struct BufferSeg {
        AllocFlag alloc;
        uint32_t  ready_size;
        uint32_t  flushed_size;
        char      buffer[SEND_BUFFER_SIZE];
        BufferSeg() { 
            alloc.sep.sealed = 0; 
            alloc.sep.alloc_size = 0; 
            ready_size = 0; 
            flushed_size = 0;
        }
    };
    struct SendBuffer {
        int      event;
        uint64_t last_sent_time;
        BufferSeg*  curr_seg;
        BufferSeg*  sending_seg;
        boost::lockfree::queue<BufferSeg*> flush_queue;
        SendBuffer() : flush_queue(512) {}
    };

    uint32_t                    _transport_id;
    vector<vector<uint32_t>>    _partitions;
    map<uint32_t, uint32_t>     _node_to_part;
    map<uint32_t, uint32_t>     _node_to_dc;
    vector<uint32_t>            _partition_leaders;
    vector<uint32_t>            _partition_closest_node;
    vector<string>              _urls;

    struct addrinfo *     _local_info;
    struct addrinfo *     _remote_info;
    int *                 _local_socks;
    int *                 _remote_socks;
    bool *                _remote_socks_ok;

    char **               _recv_buffer;
    uint32_t *            _recv_buffer_lower;
    uint32_t *            _recv_buffer_upper;
    SendBuffer *          _send_buffers;

#if DEBUG_TRANSPORT
    vector<vector<uint64_t>> _debug_queue_time;
#endif

    uint64_t *            _ping_latencies;
    void     parse_url(char *filename);
    uint32_t get_port_num(uint32_t node_id);
};