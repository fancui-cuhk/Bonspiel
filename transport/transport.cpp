#include "transport.h"
#include "message.h"
#include "global.h"
#include "helper.h"
#include "manager.h"
#include <fcntl.h>
#include <cstring>
#include <errno.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/eventfd.h>
//////////////////////////////////////////
// Each node listens to port correpsonding to each remote node.
// Each node pushes to the corresponing node's corresponding port
// For 4 nodes, for exmaple,
// node 0 listens to START_PORT + 1 for message from node 1
//                   START_PORT + 2 for message from node 2
//                   START_PORT + 3 for message from node 3
//////////////////////////////////////////

#define PRINT_DEBUG_INFO false

void
Transport::parse_url(char *filename)
{
    char hostname[1024];
    gethostname(hostname, 1024);
    g_hostname = std::string(hostname);
    printf("[!] My Hostname is %s\n", hostname);

    ifstream file(ifconfig_file);
    assert(file.is_open());
    uint32_t node_id = 0;
    bool found_myself = false;
    string line;
    int32_t curr_part_id = -1;
    std::map<string, uint32_t> url_prefix_to_dc;
    uint32_t curr_dc_id = 0;
    while (getline(file, line)) {
        if (line[0] == '#')
            continue;
        if (line[0] == '=' && line[1] == 'P') {
            curr_part_id += 1;
            _partitions.emplace_back();
        } else if (line.size() > 1){
            if (_partitions.back().empty())
                _partition_leaders.push_back(node_id);
            _partitions.back().push_back(node_id);
            _node_to_part[node_id] = curr_part_id;
            _urls.push_back(line);

            string prefix = line.substr(0, 2);
            if (url_prefix_to_dc.find(prefix) == url_prefix_to_dc.end()) {
                url_prefix_to_dc[prefix] = curr_dc_id;
                curr_dc_id += 1;
            }
            _node_to_dc[node_id] = url_prefix_to_dc[prefix];
            if (g_hostname == line) {
                found_myself = true;
                g_node_id = node_id;
                g_part_id = curr_part_id;
                if (_partitions.back()[0] == node_id)
                    g_role = ROLE_LEADER;
                else
                    g_role = ROLE_REPLICA;
            }
            node_id += 1;
        }
    }

    g_num_nodes = node_id;
    g_num_parts = _partitions.size();

    M_ASSERT(found_myself, "the node %s is not in ifconfig.txt", hostname);
    printf("Hostname: %s. Node ID = %d. \n", hostname, g_node_id);
    file.close();
}

Transport::Transport(uint32_t transport_id, char * topo_config_path)
{
    parse_url(topo_config_path);
    _transport_id = transport_id;
    if (g_num_nodes == 1)
        return;

    _local_info   = new addrinfo [g_num_nodes];
    _remote_info  = new addrinfo [g_num_nodes];
    _local_socks  = new int [g_num_nodes];
    _remote_socks = new int [g_num_nodes];
    _remote_socks_ok = new bool [g_num_nodes];

    _recv_buffer = new char * [g_num_nodes];
    _recv_buffer_lower = new uint32_t [g_num_nodes];
    _recv_buffer_upper = new uint32_t [g_num_nodes];

    _ping_latencies = new uint64_t [g_num_nodes];

    _send_buffers = new SendBuffer[g_num_nodes];
    for (uint32_t i = 0; i < g_num_nodes; i ++) {
        _ping_latencies[i] = 0;
        if (i == g_node_id) continue;
        _recv_buffer[i] = new char [RECV_BUFFER_SIZE];
        _recv_buffer_lower[i] = 0;
        _recv_buffer_upper[i] = 0;
        _remote_socks_ok[i] = false;
        _send_buffers[i].event = eventfd(0, 0);
        assert(_send_buffers[i].event > 0);
        _send_buffers[i].curr_seg = new BufferSeg();
        _send_buffers[i].last_sent_time = get_sys_clock();
        _send_buffers[i].sending_seg = NULL;
    }

    // Create local socket for each remote server
    // For thread i on a node, it listens to node j on port = START_PORT + i * g_num_nodes + j
    memset(_local_info, 0, sizeof(addrinfo) * g_num_nodes);
    for (uint32_t i = 0; i < g_num_nodes; i ++) {
        if (i == g_node_id)
            continue;
        // get addr information
        _local_info[i].ai_family = AF_UNSPEC;     // IP version not specified. Can be both.
        _local_info[i].ai_socktype = SOCK_STREAM; // Use SOCK_STREAM for TCP or SOCK_DGRAM for UDP.
        _local_info[i].ai_flags = AI_PASSIVE;
        struct addrinfo *host_info_list;          // Pointer to the to the linked list of host_info's.
        int status = getaddrinfo(NULL, std::to_string(get_port_num(i)).c_str(), &_local_info[i], &host_info_list);
        assert (status == 0);

        // create socket
        int socketfd = socket(host_info_list->ai_family, host_info_list->ai_socktype, host_info_list->ai_protocol);
        _local_socks[i] = socketfd;
        assert (socketfd != -1);
        // bind socket
        // we make use of the setsockopt() function to make sure the port is not in use.
        int yes = 1;
        status = setsockopt(socketfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
        assert (status == 0);
        status = bind(socketfd, host_info_list->ai_addr, host_info_list->ai_addrlen);
        // set the socket to non-blocking
        assert (status != -1);
        status = listen(socketfd, 5);
        assert(status != -1);
        fcntl(socketfd, F_SETFL, O_NONBLOCK);
    }
    cout << "Local Socket initialized"  << endl;

    // Establish connection to remote servers.
    memset(_remote_info, 0, sizeof(addrinfo) * g_num_nodes);
    for (uint32_t i = 0; i < g_num_nodes; i++) {
        if (i == g_node_id) continue;
        // get addr information
        _remote_info[i].ai_family   = AF_UNSPEC;     // IP version not specified. Can be both.
        _remote_info[i].ai_socktype = SOCK_STREAM;   // Use SOCK_STREAM for TCP or SOCK_DGRAM for UDP.
        struct addrinfo *host_info_list;             // Pointer to the to the linked list of host_info's.
        string server_name = _urls[i];
        int status;
        do {
            status = getaddrinfo(server_name.c_str(), std::to_string(get_port_num(g_node_id)).c_str(), &_remote_info[i], &host_info_list);
        } while (status != 0);

        // create socket
        int socketfd;
        do {
            socketfd = socket(host_info_list->ai_family, host_info_list->ai_socktype, host_info_list->ai_protocol);
        } while (socketfd == -1);
        _remote_socks[i] = socketfd;
        _remote_socks_ok[i] = true;

        do {
            status = connect(socketfd, host_info_list->ai_addr, host_info_list->ai_addrlen);
            PAUSE10;
        } while (status == -1);

        //int send_buffer_size = 33554432;
        //setsockopt(socketfd, SOL_SOCKET, SO_SNDBUF, &send_buffer_size, sizeof(send_buffer_size));
        int flag = 1;
        setsockopt(socketfd, SOL_SOCKET, TCP_NODELAY, &flag, sizeof(int));
        flag = 0;
        setsockopt(socketfd, SOL_SOCKET, TCP_QUICKACK, &flag, sizeof(int));
    }

    // Accept connection from remote servers.
    for (uint32_t i = 0; i < g_num_nodes; i++) {
        if (i == g_node_id) continue;
        int new_sd;
        struct sockaddr_storage their_addr;
        socklen_t addr_size = sizeof(their_addr);
        int socketfd = _local_socks[i];
        do {
            new_sd = accept(socketfd, (struct sockaddr *)&their_addr, &addr_size);
        } while (new_sd == -1);

        //int recv_buffer_size = 33554432;
        //setsockopt(new_sd, SOL_SOCKET, SO_RCVBUF, &recv_buffer_size, sizeof(recv_buffer_size));
        int flag = 1;
        setsockopt(new_sd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
        flag = 0;
        setsockopt(new_sd, SOL_SOCKET, TCP_QUICKACK, &flag, sizeof(int));
        _local_socks[i] = new_sd;
        //fcntl(new_sd, F_SETFL, O_NONBLOCK);
    }
#if DEBUG_TRANSPORT
    _debug_queue_time.assign(g_num_nodes, vector<uint64_t>());
#endif
    cout << "Socket initialized" << endl;
}

Transport::~Transport()
{
    for (uint32_t i = 0; i < g_num_nodes; i++) {
        if (i != g_node_id) {
            close(_local_socks[i]);
            close(_remote_socks[i]);
        }
    }
}

#if DEBUG_TRANSPORT
void
Transport::print_debug()
{
    for (uint32_t node_id = 0; node_id < g_num_nodes; node_id ++) {
        if (node_id == g_node_id) continue;
        uint64_t sum = 0;
        for (uint64_t lat : _debug_queue_time[node_id]) {
            sum += lat;
        }
        printf("queue time %u: %lu\n", node_id, sum / _debug_queue_time[node_id].size());
    }
}
#endif

void
Transport::send_msg(Message *msg)
{
    uint32_t         dest = msg->get_dest_id();
    SendBuffer&      send_buffer = _send_buffers[dest];
    uint32_t         msg_size = msg->get_packet_len();
    AllocFlag        old_flag;
    AllocFlag        new_flag;
    uint64_t         tt = get_sys_clock();
    if (msg->get_packet_len() >= MAX_MESSAGE_SIZE) {
        cout << "Message size exceeds limit: " << msg->get_packet_len() << endl;
        if (msg->get_type() == Message::RAFT_APPEND_ENTRIES)
            cout << "This is an append entry message." << endl;
    }
    assert(msg->get_packet_len() < MAX_MESSAGE_SIZE);

    while (true) {
        __sync_synchronize();
        BufferSeg *seg = send_buffer.curr_seg;

        old_flag.all = seg->alloc.all; //take snapshot
        // 1. check whether it is sealed
        if (old_flag.sep.sealed) {
            // cannot do anything if it is sealed
            PAUSE10;
            continue;
        }

        // 2. check whether it has enough space
        if (SEND_BUFFER_SIZE - old_flag.sep.alloc_size < msg_size) {
            // 2.1. no enough space, try to seal it
            new_flag.all = old_flag.all;
            new_flag.sep.sealed = 1;
            if (ATOM_CAS(seg->alloc.all, old_flag.all, new_flag.all)) {
                // 2.3 sealed, advance to the next seg
                send_buffer.flush_queue.push(seg);
                __sync_synchronize();
                BufferSeg * next_seg = new BufferSeg();
                send_buffer.curr_seg = next_seg;
                uint64_t signal = 1;
                write(send_buffer.event, &signal, sizeof(signal));
                continue;
            } else {
                // 2.2 not sealed, retry
                continue;
                PAUSE10;
            }
        }

        // 3. it has enough space, try to alloc
        new_flag = old_flag;
        new_flag.sep.alloc_size += msg_size;
        if (ATOM_CAS(seg->alloc.all, old_flag.all, new_flag.all)) {
            // 3.1. allocated space, write data
            msg->to_packet(seg->buffer + old_flag.sep.alloc_size);
            ATOM_ADD_FETCH(seg->ready_size, msg_size);
            break;
        }
    }
    INC_FLOAT_STATS(time_write_msg, get_sys_clock() - tt);
}

bool
Transport::seal_current_seg(uint32_t node_id)
{
    assert(node_id != g_node_id);

    SendBuffer&      send_buffer = _send_buffers[node_id];
    AllocFlag        old_flag;
    AllocFlag        new_flag;

    while (true) {
        __sync_synchronize();
        BufferSeg *seg = send_buffer.curr_seg;

        old_flag.all = seg->alloc.all; //take snapshot
        if (old_flag.sep.sealed || old_flag.sep.alloc_size == 0)
            return false;

        new_flag.all = old_flag.all;
        new_flag.sep.sealed = 1;
        if (ATOM_CAS(seg->alloc.all, old_flag.all, new_flag.all)) {
            // sealed, advance to the next seg
            send_buffer.flush_queue.push(seg);
            __sync_synchronize();
            BufferSeg * next_seg = new BufferSeg();
            send_buffer.curr_seg = next_seg;
            return true;
        }
    }
}

Transport::SendStatus
Transport::send_one_segment(uint32_t node_id)
{
    ssize_t sent_size = 0;
    return send_one_segment(node_id, sent_size);
}

Transport::SendStatus
Transport::send_one_segment(uint32_t node_id, ssize_t &sent_size)
{
    assert(node_id != g_node_id);

    SendBuffer &    send_buffer = _send_buffers[node_id];
    int             socket = _remote_socks[node_id];
    BufferSeg *     seg = NULL;

    if (!_remote_socks_ok[node_id])
        return DISCONNECTED;

    if (send_buffer.sending_seg != NULL)
        seg = send_buffer.sending_seg;
    else {
        send_buffer.flush_queue.pop(seg);
        send_buffer.sending_seg = seg;
    }

    if (seg == NULL)
        return NOTHING_TO_SEND;
    
    assert(seg->alloc.sep.sealed && seg->alloc.sep.alloc_size != 0);

    while (seg->alloc.sep.alloc_size > seg->ready_size) {
        PAUSE10;
        __sync_synchronize();
        continue;
    }

#if DEBUG_TRANSPORT
    // sanity checking
    assert(seg.alloc.sep.alloc_size == seg.ready_size && seg.alloc.sep.sealed);
    size_t offset = 0;
    while (offset < seg.ready_size) {
        Message * msg = (Message *) (seg.buffer + offset);
        assert(msg->get_type() < Message::NUM_MSG_TYPES);
        size_t len = msg->get_packet_len();
        assert(len < MAX_MESSAGE_SIZE);
        if (msg->get_type() == Message::PING) {
            uint64_t start_time = *(uint64_t*) (seg.buffer + offset + sizeof(Message));
            _debug_queue_time[msg->get_dest_id()].push_back(get_sys_clock() - start_time);
        }
        offset += len;
    }
#endif
    sent_size = 0;
    while (seg->flushed_size < seg->ready_size) {
        ssize_t bytes = send(socket, seg->buffer + seg->flushed_size, seg->ready_size - seg->flushed_size, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (bytes > 0) {
            seg->flushed_size += bytes;
            sent_size += bytes;
        } else if (errno == EPIPE || errno == ECONNRESET) {
            _remote_socks_ok[node_id] = false;
            return DISCONNECTED;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return PARTIALLY_SENT;
        } else 
            M_ASSERT(false, "errno: %d", errno);
    }
    assert(seg->flushed_size == seg->ready_size);
    send_buffer.sending_seg = NULL;
    delete seg;
    send_buffer.last_sent_time = get_sys_clock();
    return SEND_FINISHED;
}

Message*
Transport::get_one_message(uint32_t node_id)
{
    vector<Message*> messages;
    get_messages(node_id, messages, 1);
    if (messages.empty())
        return NULL;
    else
        return messages[0];
}

void
Transport::get_messages(uint32_t node_id, vector<Message*>& messages, int32_t limit)
{
    assert(node_id != g_node_id);

    int       socket = _local_socks[node_id];
    char    * recv_buffer = _recv_buffer[node_id];
    uint32_t& recv_buffer_lower = _recv_buffer_lower[node_id];
    uint32_t& recv_buffer_upper = _recv_buffer_upper[node_id];

    uint32_t max_size = RECV_BUFFER_SIZE - recv_buffer_upper;
    ssize_t bytes = recv(socket, recv_buffer + recv_buffer_upper, max_size, MSG_DONTWAIT);
    assert(bytes >= 0 || errno == EAGAIN);
    if (bytes > 0)
        recv_buffer_upper += bytes;

    while (recv_buffer_upper - recv_buffer_lower >= sizeof(Message)) {
        Message * msg = (Message *) (recv_buffer + recv_buffer_lower);
        assert(msg->get_packet_len() < MAX_MESSAGE_SIZE);
        if (recv_buffer_upper - recv_buffer_lower >= msg->get_packet_len()) {
            msg = new Message(recv_buffer + recv_buffer_lower);
            recv_buffer_lower += msg->get_packet_len();
            messages.push_back(msg);
            if (limit > 0 && messages.size() >= (uint32_t)limit)
                break;
        } else 
            break;
    }

    if (recv_buffer_upper == recv_buffer_lower) {
        recv_buffer_upper = 0;
        recv_buffer_lower = 0;
    }

    assert(recv_buffer_upper >= recv_buffer_lower);
    if (recv_buffer_upper != recv_buffer_lower && recv_buffer_lower != 0) {
        memmove(recv_buffer, recv_buffer + recv_buffer_lower, recv_buffer_upper - recv_buffer_lower);
        recv_buffer_upper -= recv_buffer_lower;
        recv_buffer_lower = 0;
    }
}

void
Transport::test_connect()
{
    set<uint32_t> test_nodes;
    for (uint32_t i = 0; i < g_num_nodes; i++) {
        if (i == g_node_id) continue;
        uint64_t tt = get_sys_clock();
        Message msg(Message::PING, i, 0, sizeof(tt), (char*)&tt);
        send_msg(&msg);
        assert(seal_current_seg(i));
        while (send_one_segment(i) == SEND_FINISHED)
            usleep(10);
        test_nodes.insert(i);
    }

    while (!test_nodes.empty()) {
        for (auto it = test_nodes.begin(); it != test_nodes.end(); it++) {
            uint32_t node_id = *it;
            Message *msg = NULL;
            msg = get_one_message(node_id);
            if (msg == NULL) continue;
            assert(msg->get_type() == Message::PONG || msg->get_type() == Message::PING);
            if (msg->get_type() == Message::PING) {
                uint64_t start = *(uint64_t*) msg->get_data();
                Message resp(Message::PONG, msg->get_src_node_id(), 0, sizeof(start), (char*)&start);
                send_msg(&resp);
                assert(seal_current_seg(resp.get_dest_id()));
                while (send_one_segment(resp.get_dest_id()) == SEND_FINISHED)
                    usleep(10);
            } else {
                uint64_t end_time = get_sys_clock();
                uint64_t start_time = *(uint64_t*) msg->get_data();
                _ping_latencies[node_id] = end_time - start_time;
                test_nodes.erase(node_id);
            }
            delete msg;
        }
    }

    // Measure network latency precisely
    for (uint32_t node_id = 0; node_id < g_num_nodes; node_id++) {
        string cmd = "ping -c 1 " + _urls[node_id];
        FILE * pipe = popen(cmd.c_str(), "r");
        if (!pipe) {
            continue;
        }

        constexpr int buffersize = 128;
        char buffer[buffersize];
        string output;
        while (fgets(buffer, buffersize, pipe) != nullptr)
            output += buffer;

        pclose(pipe);

        size_t start, end;
        start = output.find("time=");
        if (start == string::npos) {
            continue;
        }

        start += 5; // Skip "time="
        end = output.find(" ms", start);
        if (end == string::npos) {
            continue;
        }

        double latency = stod(output.substr(start, end - start));
        _ping_latencies[node_id] = uint64_t(latency * 1000000);
    }

    for (uint32_t part_id = 0; part_id < g_num_parts; part_id++) {
        uint64_t min_lat = -1;
        uint32_t min_lat_id = -1;
        for (uint32_t node_id : _partitions[part_id]) {
            if (_ping_latencies[node_id] < min_lat) {
                min_lat = _ping_latencies[node_id];
                min_lat_id = node_id;
            }
        }
        _partition_closest_node.push_back(min_lat_id);
    }

    cout << "Connection Test PASS!" << endl;
}

void
Transport::sync_terminate() {
    for (uint32_t node_id = 0; node_id < g_num_nodes; node_id++) {
        if (node_id == g_node_id) continue;
        Message msg(Message::DUMMY, node_id, 0, 0, NULL);
        transport->send_msg(&msg);
        transport->seal_current_seg(node_id);
        ssize_t bytes = 0;
        do {
            bytes = transport->send_one_segment(node_id);
        } while (bytes > 0);
    }
    for (uint32_t node_id = 0; node_id < g_num_nodes; node_id++) {
        if (node_id == g_node_id) continue;
        Message * msg = NULL;
        do {
            msg = transport->get_one_message(node_id);
            usleep(100);
        } while (msg == NULL || msg->get_type() != Message::DUMMY);
        printf("node:%u, type: %d\n", node_id, (int)msg->get_type());
    }
}

uint32_t
Transport::get_port_num(uint32_t node_id)
{
    return START_PORT + node_id * g_num_input_threads + _transport_id;
}