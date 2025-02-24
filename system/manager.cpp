#include "manager.h"
#include "row.h"
#include "txn.h"
#include "pthread.h"
#include "workload.h"
#include "transport.h"
#include "packetize.h"

__thread drand48_data Manager::_buffer;
__thread uint64_t Manager::_thread_id;
__thread uint64_t Manager::_max_cts = 1;
uint64_t Manager::max_sub_txn_clock = 0;

void
Manager::init() {
    timestamp = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
    *timestamp = 1;
    for (uint32_t i = 0; i < BUCKET_CNT; i++)
        pthread_mutex_init( &mutexes[i], NULL );

    _num_finished_worker_threads = 0;
    _num_finished_remote_nodes = 0;

    _vote_local = 1;
    _vote_remote = 1;

#if HOTNESS_ENABLE
    new (&_hotness_group_num_per_table) vector<uint32_t>();
    _local_hotness_table = NULL;
    _remote_hotness_table = NULL;
    _hotness_msg_ts = 0;
    new (&_remote_hotness_msg_ts) vector<int32_t>();
#endif

    _remote_done = (g_num_nodes == 1);

    _prev_physic_time = (uint64_t *) _mm_malloc(sizeof(uint64_t) * g_num_server_threads, 64);
    _prev_logical_time = (uint64_t *) _mm_malloc(sizeof(uint64_t) * g_num_server_threads, 64);
    memset(_prev_logical_time, 0, sizeof(uint64_t) * g_num_server_threads);
    memset(_prev_physic_time, 0, sizeof(uint64_t) * g_num_server_threads);
}

uint64_t
Manager::get_ts(uint64_t thread_id) {
    if (g_ts_batch_alloc)
        assert(g_ts_alloc == TS_CAS);
    uint64_t time;

#if TS_ALLOC == TS_MUTEX
    pthread_mutex_lock( &ts_mutex );
    time = ++(*timestamp);
    pthread_mutex_unlock( &ts_mutex );
#elif TS_ALLOC == TS_CAS
    if (g_ts_batch_alloc)
        time = ATOM_FETCH_ADD((*timestamp), g_ts_batch_num);
    else
        time = ATOM_FETCH_ADD((*timestamp), 1);
#elif TS_ALLOC == TS_CLOCK
    time = (get_sys_clock() * g_num_worker_threads + thread_id) * g_num_parts + g_node_id;
#elif TS_ALLOC == TS_HYBRID
    uint64_t clock_time = get_sys_clock() / TS_SCALE;
    uint64_t &counter = _prev_logical_time[thread_id];
    
    if (clock_time < max_sub_txn_clock)
        clock_time = max_sub_txn_clock;

    if (clock_time > _prev_physic_time[thread_id]) {
        counter = 0;
        _prev_physic_time[thread_id] = clock_time;
    } else {
        counter += 1;
        if (counter >= TS_COUNTER_MAX) {
            _prev_physic_time[thread_id] += 1;
            counter = 0;
        }
    }
    uint64_t tmp;
    assert(__builtin_mul_overflow(_prev_physic_time[thread_id], TS_COUNTER_MAX, &tmp) == 0);
    assert(__builtin_add_overflow(tmp, counter, &time) == 0);
    assert(__builtin_mul_overflow(time, g_num_worker_threads, &tmp) == 0);
    assert(__builtin_add_overflow(tmp, thread_id, &time) == 0);
    assert(__builtin_mul_overflow(time, g_num_nodes, &tmp) == 0);
    assert(__builtin_add_overflow(tmp, g_node_id, &time) == 0);
    
#endif
    return time;
}

uint64_t
Manager::rand_uint64()
{
    int64_t rint64 = 0;
    lrand48_r(&_buffer, &rint64);
    return rint64;
}

uint64_t
Manager::rand_uint64(uint64_t max)
{
    return rand_uint64() % max;
}

uint64_t
Manager::rand_uint64(uint64_t min, uint64_t max)
{
    return min + rand_uint64(max - min + 1);
}

double
Manager::rand_double()
{
    double r = 0;
    drand48_r(&_buffer, &r);
    return r;
}

// Resize the hotness tables to hold statistics for all hotness groups in all tables
void
Manager::resize_hotness_table(uint32_t num_table, vector<uint64_t> num_row)
{
#if ACCESS_METHOD == HISTOGRAM_3D
    if (g_node_id != transport->get_partition_leader(g_part_id))
        return;

    assert(num_table == num_row.size());
    _hotness_group_num_per_table.resize(num_table);
    _local_hotness_table = new atomic<uint64_t> ** [num_table];
    _remote_hotness_table = new atomic<uint64_t> ** [num_table];

    for (uint32_t i = 0; i < num_table; i++) {
        _hotness_group_num_per_table[i] = num_row[i] / g_hotness_group_size;
        _local_hotness_table[i] = new atomic<uint64_t> * [_hotness_group_num_per_table[i]];
        _remote_hotness_table[i] = new atomic<uint64_t> * [_hotness_group_num_per_table[i]];
        for (uint32_t j = 0; j < 100; j++) {
            // Maximum priority level as 100
            _local_hotness_table[i][j] = new atomic<uint64_t> [100];
            fill_n(_local_hotness_table[i][j], 100, 0);
            _remote_hotness_table[i][j] = new atomic<uint64_t> [100];
            fill_n(_remote_hotness_table[i][j], 100, 0);
        }
    }
#else
    // On leaders, _local_hotness_table contains the number of updates for each local hotness group per interval
    // _remote_hotness_table contains the number of updates for each remote hotness group per interval
    // Note that local and remote hotness groups interlap, so we need to separate local and remote hotness tables
    if (g_node_id != transport->get_partition_leader(g_part_id))
        return;

    assert(num_table == num_row.size());
    _hotness_group_num_per_table.resize(num_table);
    _local_hotness_table = new atomic<uint64_t> * [num_table];
    _remote_hotness_table = new atomic<uint64_t> * [num_table];

    for (uint32_t i = 0; i < num_table; i++) {
        _hotness_group_num_per_table[i] = num_row[i] / g_hotness_group_size;
        _local_hotness_table[i] = new atomic<uint64_t> [_hotness_group_num_per_table[i]];
        fill_n(_local_hotness_table[i], _hotness_group_num_per_table[i], 0);
        _remote_hotness_table[i] = new atomic<uint64_t> [_hotness_group_num_per_table[i]];
        fill_n(_remote_hotness_table[i], _hotness_group_num_per_table[i], 0);
    }
#endif
}

// Return a boolean variable indicating whether the record is hot or not
bool
Manager::is_hot(uint32_t table_id, uint64_t key)
{
#if ACCESS_METHOD == HISTOGRAM_3D
    assert(GET_WORKLOAD->key_to_part(key, table_id) != g_part_id);
    if (_hotness_msg_ts == 0) {
        // No hotness stats yet, the default setting is hot
        return true;
    }
    assert(WORKLOAD == YCSB);
    uint64_t row_id = key / g_num_parts;
    assert(row_id / g_hotness_group_size < _hotness_group_num_per_table[table_id]);
    uint64_t hotness = 0;
    for (uint32_t i = 0; i < 100; i++)
        hotness += _remote_hotness_table[table_id][row_id / g_hotness_group_size][i];

    return (hotness >= g_hotness_threshold);
#else
    assert(GET_WORKLOAD->key_to_part(key, table_id) != g_part_id);
    if (_hotness_msg_ts == 0) {
        // No hotness stats yet, the default setting is cold
        return false;
    }
  #if WORKLOAD == YCSB
    uint64_t row_id = key / g_num_parts;
    assert(row_id / g_hotness_group_size < _hotness_group_num_per_table[table_id]);
    uint64_t hotness = _remote_hotness_table[table_id][row_id / g_hotness_group_size];
  #else
    uint64_t hotness = _remote_hotness_table[table_id][key % _hotness_group_num_per_table[table_id]];
  #endif
    return (hotness >= g_hotness_threshold);
#endif
}

#if ACCESS_METHOD == HISTOGRAM_3D

double
Manager::get_update_frequency(uint32_t table_id, uint64_t key, uint64_t prio)
{
    assert(GET_WORKLOAD->key_to_part(key, table_id) != g_part_id);
    if (_hotness_msg_ts == 0) {
        // No hotness stats yet, the default setting is high risk
        return true;
    }
    assert(WORKLOAD == YCSB);
    uint64_t row_id = key / g_num_parts;
    assert(row_id / g_hotness_group_size < _hotness_group_num_per_table[table_id]);
    uint64_t update_count = 0;
    for (uint32_t i = prio; i < 100; i++)
        update_count += _remote_hotness_table[table_id][row_id / g_hotness_group_size][i];

    double update_count_per_record = ((double) update_count) / g_hotness_group_size;

    return update_count_per_record / g_hotness_interval;
}

#else

// Return a boolean variable indicating whether reading the record w/o locking leads to high abort risk
bool
Manager::high_abort_risk(uint32_t table_id, uint64_t key)
{
#if WORKLOAD == TPCC
    if (table_id == 7) {
        // Item table is replicated and read-only
        return false;
    }
#endif

    assert(GET_WORKLOAD->key_to_part(key, table_id) != g_part_id);
    if (_hotness_msg_ts == 0) {
        // No hotness stats yet
    #if WORKLOAD == YCSB
        return true;
    #elif WORKLOAD == TPCC
        return false;
    #endif
    }
#if WORKLOAD == YCSB
    uint64_t row_id = key / g_num_parts;
    assert(row_id / g_hotness_group_size < _hotness_group_num_per_table[table_id]);
    uint64_t update_count = _remote_hotness_table[table_id][row_id / g_hotness_group_size];
#else
    uint64_t update_count = _remote_hotness_table[table_id][key % _hotness_group_num_per_table[table_id]];
#endif

    double update_count_per_record = ((double) update_count) / g_hotness_group_size;
    uint32_t remote_part_id = GET_WORKLOAD->key_to_part(key, table_id);
    double rtt_with_leader = ((double) transport->get_ping_latency(transport->get_partition_leader(remote_part_id))) / BILLION; // RTT in sec

    return ((update_count_per_record / g_hotness_interval * rtt_with_leader) >= g_bonspiel_threshold);
}

#endif

#if ACCESS_METHOD == HISTOGRAM_3D

void
Manager::update_hotness(uint32_t table_id, uint64_t key, uint64_t prio)
{
    assert(g_node_id == transport->get_partition_leader(g_part_id));
    assert(WORKLOAD == YCSB);

    prio = min(prio, (uint64_t) 100);
    uint64_t row_id = key / g_num_parts;
    assert(row_id / g_hotness_group_size < _hotness_group_num_per_table[table_id]);
    _local_hotness_table[table_id][row_id / g_hotness_group_size]++;
}

#else

// This function is called on leaders to update _local_hotness_table
void
Manager::update_hotness(uint32_t table_id, uint64_t key)
{
    assert(g_node_id == transport->get_partition_leader(g_part_id));
#if WORKLOAD == YCSB
    uint64_t row_id = key / g_num_parts;
    assert(row_id / g_hotness_group_size < _hotness_group_num_per_table[table_id]);
    _local_hotness_table[table_id][row_id / g_hotness_group_size]++;
#else
    _local_hotness_table[table_id][key % _hotness_group_num_per_table[table_id]]++;
#endif
}

#endif

// This function serializes _local_hotness_table into buffer
uint32_t
Manager::get_local_hotness_table(UnstructuredBuffer & buffer)
{
#if ACCESS_METHOD == HISTOGRAM_3D
    if (_remote_hotness_msg_ts.empty() && g_num_parts != 1) {
        _remote_hotness_msg_ts.resize(g_num_parts, -1);
    }
    // Construct buffer
    // | timestamp | payload |
    buffer.put(&_hotness_msg_ts);
    _hotness_msg_ts++;
    for (uint32_t table_id = 0; table_id < _hotness_group_num_per_table.size(); table_id++) {
        for (uint32_t hg_id = 0; hg_id < _hotness_group_num_per_table[table_id]; hg_id++) {
            for (uint32_t prio = 0; prio < 100; prio++)
                buffer.put(&_local_hotness_table[table_id][hg_id][prio]);
        }
    }
    // Reset _local_hotness_table
    for (uint32_t table_id = 0; table_id < _hotness_group_num_per_table.size(); table_id++) {
        for (uint32_t hg_id = 0; hg_id < _hotness_group_num_per_table[table_id]; hg_id++) {
            for (uint32_t prio = 0; prio < 100; prio++)
                _local_hotness_table[table_id][hg_id][prio] = 0;
        }
    }
    return buffer.size();
#else
    if (_remote_hotness_msg_ts.empty() && g_num_parts != 1) {
        _remote_hotness_msg_ts.resize(g_num_parts, -1);
    }
    // Construct buffer
    // | timestamp | payload |
    buffer.put(&_hotness_msg_ts);
    _hotness_msg_ts++;
    for (uint32_t table_id = 0; table_id < _hotness_group_num_per_table.size(); table_id++) {
        for (uint32_t hg_id = 0; hg_id < _hotness_group_num_per_table[table_id]; hg_id++) {
            buffer.put(&_local_hotness_table[table_id][hg_id]);
        }
    }
    // Reset _local_hotness_table
    for (uint32_t table_id = 0; table_id < _hotness_group_num_per_table.size(); table_id++) {
        for (uint32_t hg_id = 0; hg_id < _hotness_group_num_per_table[table_id]; hg_id++) {
            _local_hotness_table[table_id][hg_id] = 0;
        }
    }
    return buffer.size();
#endif
}

// This function applies _remote_hotness_table
void
Manager::apply_remote_hotness_table(uint32_t part_id, UnstructuredBuffer buffer)
{
#if ACCESS_METHOD == HISTOGRAM_3D
    if (_remote_hotness_msg_ts.empty() && g_num_parts != 1) {
        _remote_hotness_msg_ts.resize(g_num_parts, -1);
    }
    int32_t ts;
    buffer.get(&ts);
    if (ts > _remote_hotness_msg_ts[part_id]) {
        // Only apply those with higher timestamps
        uint64_t hotness;
        int32_t max_ts = *max_element(_remote_hotness_msg_ts.begin(), _remote_hotness_msg_ts.end());
        if (ts < max_ts) {
            // Ignore old msg
            return;
        }
        for (uint32_t table_id = 0; table_id < _hotness_group_num_per_table.size(); table_id++) {
            for (uint32_t hg_id = 0; hg_id < _hotness_group_num_per_table[table_id]; hg_id++) {
                for (uint32_t prio = 0; prio < 100; prio++) {
                    buffer.get(&hotness);
                    if (ts > max_ts) {
                        // A hotness msg in a new term, reset table
                        _remote_hotness_table[table_id][hg_id][prio] = hotness;
                    } else {
                        // A hotness msg in the current term, increment table
                        _remote_hotness_table[table_id][hg_id][prio] += hotness;
                    }
                }
            }
        }
        _remote_hotness_msg_ts[part_id] = ts;
    }
#else
    if (_remote_hotness_msg_ts.empty() && g_num_parts != 1) {
        _remote_hotness_msg_ts.resize(g_num_parts, -1);
    }
    int32_t ts;
    buffer.get(&ts);
    if (ts > _remote_hotness_msg_ts[part_id]) {
        // Only apply those with higher timestamps
        uint64_t hotness;
        int32_t max_ts = *max_element(_remote_hotness_msg_ts.begin(), _remote_hotness_msg_ts.end());
        if (ts < max_ts) {
            // Ignore old msg
            return;
        }
        for (uint32_t table_id = 0; table_id < _hotness_group_num_per_table.size(); table_id++) {
            for (uint32_t hg_id = 0; hg_id < _hotness_group_num_per_table[table_id]; hg_id++) {
                buffer.get(&hotness);
                if (ts > max_ts) {
                    // A hotness msg in a new term, reset table
                    _remote_hotness_table[table_id][hg_id] = hotness;
                } else {
                    // A hotness msg in the current term, increment table
                    _remote_hotness_table[table_id][hg_id] += hotness;
                }
            }
        }
        _remote_hotness_msg_ts[part_id] = ts;
    }
#endif
}

bool
Manager::read_local()
{
    return (_vote_local / _vote_remote >= g_local_threshold);
}

void
Manager::vote_for_local()
{
    ATOM_ADD(_vote_local, 1);
}

void
Manager::vote_for_remote()
{
    ATOM_ADD(_vote_remote, 1);
}
