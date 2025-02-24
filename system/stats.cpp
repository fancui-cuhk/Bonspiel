#include "global.h"
#include "helper.h"
#include "stats.h"
#include "message.h"
#include "tpcc_helper.h"
#include "tpce_helper.h"
#include "transport.h"
#include <numeric>

Stats_thd::Stats_thd()
{
    _float_stats = (double *) _mm_malloc(sizeof(double) * NUM_FLOAT_STATS, 64);
    _sp_float_stats = (double *) _mm_malloc(sizeof(double) * NUM_SP_FLOAT_STATS, 64);
    _mp_float_stats = (double *) _mm_malloc(sizeof(double) * NUM_MP_FLOAT_STATS, 64);
    _remote_float_stats = (double *) _mm_malloc(sizeof(double) * NUM_REMOTE_FLOAT_STATS, 64);
    _int_stats = (uint64_t *) _mm_malloc(sizeof(uint64_t) * NUM_INT_STATS, 64);

    _msg_count = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);
    _msg_size = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);
    _msg_committed_count = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);
    _msg_committed_size = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);

    clear();
}

void Stats_thd::init(uint64_t thd_id) {
    clear();
}

void Stats_thd::clear() {
    for (uint32_t i = 0; i < NUM_FLOAT_STATS; i++)
        _float_stats[i] = 0;
    for (uint32_t i = 0; i < NUM_SP_FLOAT_STATS; i++)
        _sp_float_stats[i] = 0;
    for (uint32_t i = 0; i < NUM_MP_FLOAT_STATS; i++)
        _mp_float_stats[i] = 0;
    for (uint32_t i = 0; i < NUM_REMOTE_FLOAT_STATS; i++)
        _remote_float_stats[i] = 0;
    for (uint32_t i = 0; i < NUM_INT_STATS; i++)
        _int_stats[i] = 0;

    memset(_msg_count, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
    memset(_msg_size, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
    memset(_msg_committed_count, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
    memset(_msg_committed_size, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);

#if WORKLOAD == TPCC
    memset(_commits_per_txn_type, 0, sizeof(uint64_t) * 5);
    memset(_aborts_per_txn_type, 0, sizeof(uint64_t) * 5);
    memset(_latency_per_txn_type, 0, sizeof(uint64_t) * 5);
#elif WORKLOAD == TPCE
    // SP & MP stats
    memset(_commits_per_txn_type, 0, sizeof(uint64_t) * 10);
    memset(_aborts_per_txn_type, 0, sizeof(uint64_t) * 10);
    memset(_latency_per_txn_type, 0, sizeof(uint64_t) * 10);
    memset(_execution_phase_per_txn_type, 0, sizeof(uint64_t) * 10);
    memset(_prepare_phase_per_txn_type, 0, sizeof(uint64_t) * 10);
    memset(_commit_phase_per_txn_type, 0, sizeof(uint64_t) * 10);
    memset(_abort_time_per_txn_type, 0, sizeof(uint64_t) * 10);

    // MP stats
    memset(_mp_commits_per_txn_type, 0, sizeof(uint64_t) * 10);
    memset(_mp_aborts_per_txn_type, 0, sizeof(uint64_t) * 10);
    memset(_mp_latency_per_txn_type, 0, sizeof(uint64_t) * 10);
    memset(_mp_execution_phase_per_txn_type, 0, sizeof(uint64_t) * 10);
    memset(_mp_prepare_phase_per_txn_type, 0, sizeof(uint64_t) * 10);
    memset(_mp_commit_phase_per_txn_type, 0, sizeof(uint64_t) * 10);
    memset(_mp_abort_time_per_txn_type, 0, sizeof(uint64_t) * 10);
#endif
}

void
Stats_thd::copy_from(Stats_thd * stats_thd)
{
    memcpy(_float_stats, stats_thd->_float_stats, sizeof(double) * NUM_FLOAT_STATS);
    memcpy(_sp_float_stats, stats_thd->_sp_float_stats, sizeof(double) * NUM_SP_FLOAT_STATS);
    memcpy(_mp_float_stats, stats_thd->_mp_float_stats, sizeof(double) * NUM_MP_FLOAT_STATS);
    memcpy(_remote_float_stats, stats_thd->_remote_float_stats, sizeof(double) * NUM_REMOTE_FLOAT_STATS);
    memcpy(_int_stats, stats_thd->_int_stats, sizeof(double) * NUM_INT_STATS);
    memcpy(_msg_count, stats_thd->_msg_count, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
    memcpy(_msg_size, stats_thd->_msg_size, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
    memcpy(_msg_committed_count, stats_thd->_msg_committed_count, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
    memcpy(_msg_committed_size, stats_thd->_msg_committed_size, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
}

////////////////////////////////////////////////
// class Stats
////////////////////////////////////////////////
Stats::Stats()
{
    _num_cp = 0;
    _stats = new Stats_thd * [g_total_num_threads];
    for (uint32_t i = 0; i < g_total_num_threads; i++) {
        _stats[i] = (Stats_thd *) _mm_malloc(sizeof(Stats_thd), 64);
        new(_stats[i]) Stats_thd();
    }
}

void Stats::init() {
    if (!STATS_ENABLE)
        return;
    _stats = (Stats_thd**) _mm_malloc(sizeof(Stats_thd*) * g_total_num_threads, 64);
}

void Stats::init(uint64_t thread_id) {
    if (!STATS_ENABLE)
        return;
    _stats[thread_id] = (Stats_thd *)
        _mm_malloc(sizeof(Stats_thd), 64);

    _stats[thread_id]->init(thread_id);
}

void Stats::clear(uint64_t tid) {
    if (STATS_ENABLE) {
        _stats[tid]->clear();

        dl_detect_time = 0;
        dl_wait_time = 0;
        cycle_detect = 0;
        deadlock = 0;
    }
}

void Stats::output(std::ostream * os)
{
    std::ostream &out = *os;

    if (g_warmup_time > 0) {
        // subtract the stats in the warmup period
        uint32_t cp = int(1000 * g_warmup_time / STATS_CP_INTERVAL) - 1;
        Stats * base = _checkpoints[cp];
        for (uint32_t i = 0; i < g_total_num_threads; i++) {
            for (uint32_t n = 0; n < NUM_FLOAT_STATS; n++)
                _stats[i]->_float_stats[n] -= base->_stats[i]->_float_stats[n];
            for (uint32_t n = 0; n < NUM_SP_FLOAT_STATS; n++)
                _stats[i]->_sp_float_stats[n] -= base->_stats[i]->_sp_float_stats[n];
            for (uint32_t n = 0; n < NUM_MP_FLOAT_STATS; n++)
                _stats[i]->_mp_float_stats[n] -= base->_stats[i]->_mp_float_stats[n];
            if (i < g_num_worker_threads)
                _stats[i]->_float_stats[STAT_run_time] = g_run_time * BILLION;
            for (uint32_t n = 0; n < NUM_INT_STATS; n++)
                _stats[i]->_int_stats[n] -= base->_stats[i]->_int_stats[n];

            for (uint32_t n = 0; n < Message::NUM_MSG_TYPES; n++) {
                _stats[i]->_msg_count[n] -= base->_stats[i]->_msg_count[n];
                _stats[i]->_msg_size[n] -= base->_stats[i]->_msg_size[n];
                _stats[i]->_msg_committed_count[n] -= base->_stats[i]->_msg_committed_count[n];
                _stats[i]->_msg_committed_size[n] -= base->_stats[i]->_msg_committed_size[n];
            }
        }
    }

    uint64_t total_num_commits = 0;
    uint64_t total_num_sp_commits = 0;
    uint64_t total_num_mp_commits = 0;
    uint64_t total_num_remote_commits = 0;
    double total_run_time = 0;
    for (uint32_t tid = 0; tid < g_total_num_threads; tid++) {
        total_num_commits += _stats[tid]->_int_stats[STAT_num_commits];
        total_run_time += _stats[tid]->_float_stats[STAT_run_time];
        total_num_sp_commits += _stats[tid]->_int_stats[STAT_num_home_sp_commits];
        total_num_mp_commits += _stats[tid]->_int_stats[STAT_num_home_mp_commits];
        total_num_remote_commits += _stats[tid]->_int_stats[STAT_num_remote_commits];
    }

    if (total_num_commits == 0) total_num_commits = 1;
    out << "=Worker Thread=" << endl;
    out << "    " << setw(33) << left << "Throughput:"
        << BILLION * total_num_commits / total_run_time * g_num_server_threads << endl;
    // print floating point stats
    for (uint32_t i = 0; i < NUM_FLOAT_STATS; i++) {
        if (i == STAT_txn_latency)
            continue;
        double total = 0;
        for (uint32_t tid = 0; tid < g_total_num_threads; tid++)
            total += _stats[tid]->_float_stats[i];
        string suffix = "";
        if (i >= STAT_execute_phase/* && i <= STAT_network*/) {
            total = total / total_num_commits * 1000000; // in us.
            suffix = " (in us) ";
        }
        out << "    " << setw(33) << left << statsFloatName[i] + suffix + ':' << total / BILLION;
        out << " (";
        for (uint32_t tid = 0; tid < g_total_num_threads; tid++)
            out << _stats[tid]->_float_stats[i] / BILLION << ',';
        out << ')' << endl;
    }

    out << endl;

#if COLLECT_LATENCY
    if (g_role == ROLE_LEADER) {
        uint64_t latency_size = _agg_latency.size();
        uint64_t sp_latency_size = _agg_sp_latency.size();
        uint64_t mp_latency_size = _agg_mp_latency.size();
        LatencyEntry entry;

        // All txns
        out << "    " << setw(20) << left << "Latency of all txns (ms):" << endl;
        // p50
        entry = _agg_latency[(uint64_t)(latency_size * 0.50)];
        out << "    " << setw(17) << left << "p50_latency:"
                    << setw(10) << left << entry.latency / MILLION
                    << setw(13) << left << "abort count:"
                    << setw(5)  << left << entry.abort_count
                    << "(exec, prep, commit, abort, net): ("
                    << entry.execution_latency / MILLION << ", "
                    << entry.prepare_latency / MILLION << ", "
                    << entry.commit_latency / MILLION << ", "
                    << entry.abort / MILLION << ", "
                    << entry.network / MILLION << ")"
                    << endl;
        // p90
        entry = _agg_latency[(uint64_t)(latency_size * 0.90)];
        out << "    " << setw(17) << left << "p90_latency:"
                    << setw(10) << left << entry.latency / MILLION
                    << setw(13) << left << "abort count:"
                    << setw(5)  << left << entry.abort_count
                    << "(exec, prep, commit, abort, net): ("
                    << entry.execution_latency / MILLION << ", "
                    << entry.prepare_latency / MILLION << ", "
                    << entry.commit_latency / MILLION << ", "
                    << entry.abort / MILLION << ", "
                    << entry.network / MILLION << ")"
                    << endl;
        // p95
        entry = _agg_latency[(uint64_t)(latency_size * 0.95)];
        out << "    " << setw(17) << left << "p95_latency:"
                    << setw(10) << left << entry.latency / MILLION
                    << setw(13) << left << "abort count:"
                    << setw(5)  << left << entry.abort_count
                    << "(exec, prep, commit, abort, net): ("
                    << entry.execution_latency / MILLION << ", "
                    << entry.prepare_latency / MILLION << ", "
                    << entry.commit_latency / MILLION << ", "
                    << entry.abort / MILLION << ", "
                    << entry.network / MILLION << ")"
                    << endl;
        // p99
        entry = _agg_latency[(uint64_t)(latency_size * 0.99)];
        out << "    " << setw(17) << left << "p99_latency:"
                    << setw(10) << left << entry.latency / MILLION
                    << setw(13) << left << "abort count:"
                    << setw(5)  << left << entry.abort_count
                    << "(exec, prep, commit, abort, net): ("
                    << entry.execution_latency / MILLION << ", "
                    << entry.prepare_latency / MILLION << ", "
                    << entry.commit_latency / MILLION << ", "
                    << entry.abort / MILLION << ", "
                    << entry.network / MILLION << ")"
                    << endl;
        // p999
        entry = _agg_latency[(uint64_t)(latency_size * 0.999)];
        out << "    " << setw(17) << left << "p999_latency:"
                    << setw(10) << left << entry.latency / MILLION
                    << setw(13) << left << "abort count:"
                    << setw(5)  << left << entry.abort_count
                    << "(exec, prep, commit, abort, net): ("
                    << entry.execution_latency / MILLION << ", "
                    << entry.prepare_latency / MILLION << ", "
                    << entry.commit_latency / MILLION << ", "
                    << entry.abort / MILLION << ", "
                    << entry.network / MILLION << ")"
                    << endl;
        // p9999
        entry = _agg_latency[(uint64_t)(latency_size * 0.9999)];
        out << "    " << setw(17) << left << "p9999_latency:"
                    << setw(10) << left << entry.latency / MILLION
                    << setw(13) << left << "abort count:"
                    << setw(5)  << left << entry.abort_count
                    << "(exec, prep, commit, abort, net): ("
                    << entry.execution_latency / MILLION << ", "
                    << entry.prepare_latency / MILLION << ", "
                    << entry.commit_latency / MILLION << ", "
                    << entry.abort / MILLION << ", "
                    << entry.network / MILLION << ")"
                    << endl;
        // max
        // entry = _agg_latency[latency_size - 1];
        // out << "    " << setw(17) << left << "max_latency:"
        //             << setw(10) << left << entry.latency / MILLION
        //             << setw(13) << left << "abort count:"
        //             << setw(5)  << left << entry.abort_count
        //             << "(exec, prep, commit, abort, net): ("
        //             << entry.execution_latency / MILLION << ", "
        //             << entry.prepare_latency / MILLION << ", "
        //             << entry.commit_latency / MILLION << ", "
        //             << entry.abort / MILLION << ", "
        //             << entry.network / MILLION << ")"
        //             << endl;
        out << endl;

        // Single-region txns
        if (_agg_sp_latency.size() > 0) {
            out << "    " << setw(20) << left << "Latency of local txns (ms):" << endl;
            // p50
            entry = _agg_sp_latency[(uint64_t)(sp_latency_size * 0.50)];
            out << "    " << setw(17) << left << "sp_p50_latency:"
                        << setw(10) << left << entry.latency / MILLION
                        << setw(13) << left << "abort count:"
                        << setw(5)  << left << entry.abort_count
                        << "(exec, prep, commit, abort, net): ("
                        << entry.execution_latency / MILLION << ", "
                        << entry.prepare_latency / MILLION << ", "
                        << entry.commit_latency / MILLION << ", "
                        << entry.abort / MILLION << ", "
                        << entry.network / MILLION << ")"
                        << endl;
            // p90
            entry = _agg_sp_latency[(uint64_t)(sp_latency_size * 0.90)];
            out << "    " << setw(17) << left << "sp_p90_latency:"
                        << setw(10) << left << entry.latency / MILLION
                        << setw(13) << left << "abort count:"
                        << setw(5)  << left << entry.abort_count
                        << "(exec, prep, commit, abort, net): ("
                        << entry.execution_latency / MILLION << ", "
                        << entry.prepare_latency / MILLION << ", "
                        << entry.commit_latency / MILLION << ", "
                        << entry.abort / MILLION << ", "
                        << entry.network / MILLION << ")"
                        << endl;
            // p95
            entry = _agg_sp_latency[(uint64_t)(sp_latency_size * 0.95)];
            out << "    " << setw(17) << left << "sp_p95_latency:"
                        << setw(10) << left << entry.latency / MILLION
                        << setw(13) << left << "abort count:"
                        << setw(5)  << left << entry.abort_count
                        << "(exec, prep, commit, abort, net): ("
                        << entry.execution_latency / MILLION << ", "
                        << entry.prepare_latency / MILLION << ", "
                        << entry.commit_latency / MILLION << ", "
                        << entry.abort / MILLION << ", "
                        << entry.network / MILLION << ")"
                        << endl;
            // p99
            entry = _agg_sp_latency[(uint64_t)(sp_latency_size * 0.99)];
            out << "    " << setw(17) << left << "sp_p99_latency:"
                        << setw(10) << left << entry.latency / MILLION
                        << setw(13) << left << "abort count:"
                        << setw(5)  << left << entry.abort_count
                        << "(exec, prep, commit, abort, net): ("
                        << entry.execution_latency / MILLION << ", "
                        << entry.prepare_latency / MILLION << ", "
                        << entry.commit_latency / MILLION << ", "
                        << entry.abort / MILLION << ", "
                        << entry.network / MILLION << ")"
                        << endl;
            // p999
            entry = _agg_sp_latency[(uint64_t)(sp_latency_size * 0.999)];
            out << "    " << setw(17) << left << "sp_p999_latency:"
                        << setw(10) << left << entry.latency / MILLION
                        << setw(13) << left << "abort count:"
                        << setw(5)  << left << entry.abort_count
                        << "(exec, prep, commit, abort, net): ("
                        << entry.execution_latency / MILLION << ", "
                        << entry.prepare_latency / MILLION << ", "
                        << entry.commit_latency / MILLION << ", "
                        << entry.abort / MILLION << ", "
                        << entry.network / MILLION << ")"
                        << endl;
            // p9999
            entry = _agg_sp_latency[(uint64_t)(sp_latency_size * 0.9999)];
            out << "    " << setw(17) << left << "sp_p9999_latency:"
                        << setw(10) << left << entry.latency / MILLION
                        << setw(13) << left << "abort count:"
                        << setw(5)  << left << entry.abort_count
                        << "(exec, prep, commit, abort, net): ("
                        << entry.execution_latency / MILLION << ", "
                        << entry.prepare_latency / MILLION << ", "
                        << entry.commit_latency / MILLION << ", "
                        << entry.abort / MILLION << ", "
                        << entry.network / MILLION << ")"
                        << endl;
            // max
            // entry = _agg_sp_latency[sp_latency_size - 1];
            // out << "    " << setw(17) << left << "sp_max_latency:"
            //             << setw(10) << left << entry.latency / MILLION
            //             << setw(13) << left << "abort count:"
            //             << setw(5)  << left << entry.abort_count
            //             << "(exec, prep, commit, abort, net): ("
            //             << entry.execution_latency / MILLION << ", "
            //             << entry.prepare_latency / MILLION << ", "
            //             << entry.commit_latency / MILLION << ", "
            //             << entry.abort / MILLION << ", "
            //             << entry.network / MILLION << ")"
            //             << endl;
            out << endl;
        }

        // Multi-region txns
        if (_agg_mp_latency.size() > 0) {
            out << "    " << setw(20) << left << "Latency of distributed txns (ms):" << endl;
            // p50
            entry = _agg_mp_latency[(uint64_t)(mp_latency_size * 0.50)];
            out << "    " << setw(17) << left << "mp_p50_latency:"
                        << setw(10) << left << entry.latency / MILLION
                        << setw(13) << left << "abort count:"
                        << setw(5)  << left << entry.abort_count
                        << "(exec, prep, commit, abort, net): ("
                        << entry.execution_latency / MILLION << ", "
                        << entry.prepare_latency / MILLION << ", "
                        << entry.commit_latency / MILLION << ", "
                        << entry.abort / MILLION << ", "
                        << entry.network / MILLION << ")"
                        << endl;
            // p90
            entry = _agg_mp_latency[(uint64_t)(mp_latency_size * 0.90)];
            out << "    " << setw(17) << left << "mp_p90_latency:"
                        << setw(10) << left << entry.latency / MILLION
                        << setw(13) << left << "abort count:"
                        << setw(5)  << left << entry.abort_count
                        << "(exec, prep, commit, abort, net): ("
                        << entry.execution_latency / MILLION << ", "
                        << entry.prepare_latency / MILLION << ", "
                        << entry.commit_latency / MILLION << ", "
                        << entry.abort / MILLION << ", "
                        << entry.network / MILLION << ")"
                        << endl;
            // p95
            entry = _agg_mp_latency[(uint64_t)(mp_latency_size * 0.95)];
            out << "    " << setw(17) << left << "mp_p95_latency:"
                        << setw(10) << left << entry.latency / MILLION
                        << setw(13) << left << "abort count:"
                        << setw(5)  << left << entry.abort_count
                        << "(exec, prep, commit, abort, net): ("
                        << entry.execution_latency / MILLION << ", "
                        << entry.prepare_latency / MILLION << ", "
                        << entry.commit_latency / MILLION << ", "
                        << entry.abort / MILLION << ", "
                        << entry.network / MILLION << ")"
                        << endl;
            // p99
            entry = _agg_mp_latency[(uint64_t)(mp_latency_size * 0.99)];
            out << "    " << setw(17) << left << "mp_p99_latency:"
                        << setw(10) << left << entry.latency / MILLION
                        << setw(13) << left << "abort count:"
                        << setw(5)  << left << entry.abort_count
                        << "(exec, prep, commit, abort, net): ("
                        << entry.execution_latency / MILLION << ", "
                        << entry.prepare_latency / MILLION << ", "
                        << entry.commit_latency / MILLION << ", "
                        << entry.abort / MILLION << ", "
                        << entry.network / MILLION << ")"
                        << endl;
            // p999
            entry = _agg_mp_latency[(uint64_t)(mp_latency_size * 0.999)];
            out << "    " << setw(17) << left << "mp_p999_latency:"
                        << setw(10) << left << entry.latency / MILLION
                        << setw(13) << left << "abort count:"
                        << setw(5)  << left << entry.abort_count
                        << "(exec, prep, commit, abort, net): ("
                        << entry.execution_latency / MILLION << ", "
                        << entry.prepare_latency / MILLION << ", "
                        << entry.commit_latency / MILLION << ", "
                        << entry.abort / MILLION << ", "
                        << entry.network / MILLION << ")"
                        << endl;
            // p9999
            entry = _agg_mp_latency[(uint64_t)(mp_latency_size * 0.9999)];
            out << "    " << setw(17) << left << "mp_p9999_latency:"
                        << setw(10) << left << entry.latency / MILLION
                        << setw(13) << left << "abort count:"
                        << setw(5)  << left << entry.abort_count
                        << "(exec, prep, commit, abort, net): ("
                        << entry.execution_latency / MILLION << ", "
                        << entry.prepare_latency / MILLION << ", "
                        << entry.commit_latency / MILLION << ", "
                        << entry.abort / MILLION << ", "
                        << entry.network / MILLION << ")"
                        << endl;
            // max
            // entry = _agg_mp_latency[mp_latency_size - 1];
            // out << "    " << setw(17) << left << "mp_max_latency:"
            //             << setw(10) << left << entry.latency / MILLION
            //             << setw(13) << left << "abort count:"
            //             << setw(5)  << left << entry.abort_count
            //             << "(exec, prep, commit, abort, net): ("
            //             << entry.execution_latency / MILLION << ", "
            //             << entry.prepare_latency / MILLION << ", "
            //             << entry.commit_latency / MILLION << ", "
            //             << entry.abort / MILLION << ", "
            //             << entry.network / MILLION << ")"
            //             << endl;
            out << endl;
        }
    }
#endif

    // print integer stats
    int64_t num_commits = 0;
    int64_t num_aborts = 0;
    int64_t num_mp_txn = 0;
    int64_t num_sp_txn = 0;
    for (uint32_t i = 0; i < NUM_INT_STATS; i++) {
        double total = 0;
        for (uint32_t tid = 0; tid < g_total_num_threads; tid++) {
            total += _stats[tid]->_int_stats[i];
        }

        out << "    " << setw(33) << left << statsIntName[i] + ':'<< total;
        out << " (";
        for (uint32_t tid = 0; tid < g_total_num_threads; tid++)
            out << _stats[tid]->_int_stats[i] << ',';
        out << ')' << endl;

        if (i == STAT_num_commits)
            num_commits = total;
        if (i == STAT_num_aborts) {
            num_aborts = total;
            out << "    " << setw(33) << left << "abort_rate:" << (double)num_aborts / (num_commits + num_aborts) << endl;
        }
        if (i == STAT_num_home_sp)
            num_sp_txn = total;
        if (i == STAT_num_home_mp)
            num_mp_txn = total;
        if (i == STAT_num_home_sp_commits)
            out << "    " << setw(33) << left << "sp_abort_rate:" << (double)(num_sp_txn - total) / num_sp_txn << endl;
        if (i == STAT_num_home_mp_commits)
            out << "    " << setw(33) << left << "mp_abort_rate:" << (double)(num_mp_txn - total) / num_mp_txn << endl;
    }

    out << endl;
    out << "=Single Partition Stats=" << endl;
    for (uint32_t i = 0; i < NUM_SP_FLOAT_STATS; i++) {
        double total = 0;
        for (uint32_t tid = 0; tid < g_total_num_threads; tid++)
            total += _stats[tid]->_sp_float_stats[i];

        if (i >= STAT_sp_retry) {
            total = total / total_num_sp_commits;
            out << "    " << setw(33) << left << statsFloatSPName[i] + ':' << total;
            out << " (";
            for (uint32_t tid = 0; tid < g_total_num_threads; tid++)
                out << _stats[tid]->_sp_float_stats[i] << ',';
            out << ')' << endl;
            continue;
        }
        total = total / total_num_sp_commits * 1000000; // in us.
        string suffix = " (in us) ";

        out << "    " << setw(33) << left << statsFloatSPName[i] + suffix + ':' << total / BILLION;
        out << " (";
        for (uint32_t tid = 0; tid < g_total_num_threads; tid++)
            out << _stats[tid]->_sp_float_stats[i] / BILLION << ',';
        out << ')' << endl;
    } 

    if (total_num_mp_commits) {
        out << endl;
        out << "=Multiple Partition Stats=" << endl;
        for (uint32_t i = 0; i < NUM_MP_FLOAT_STATS; i++) {
            double total = 0;
            for (uint32_t tid = 0; tid < g_total_num_threads; tid++)
                total += _stats[tid]->_mp_float_stats[i];

            if (i >= STAT_mp_retry) {
                total = total / total_num_mp_commits;
                out << "    " << setw(33) << left << statsFloatMPName[i] + ':' << total;
                out << " (";
                for (uint32_t tid = 0; tid < g_total_num_threads; tid++)
                    out << _stats[tid]->_mp_float_stats[i] << ',';
                out << ')' << endl;
                continue;
            }
            total = total / total_num_mp_commits * 1000000; // in us.
            string suffix = " (in us) ";
            out << "    " << setw(33) << left << statsFloatMPName[i] + suffix + ':' << total / BILLION;
            out << " (";
            for (uint32_t tid = 0; tid < g_total_num_threads; tid++)
                out << _stats[tid]->_mp_float_stats[i] / BILLION << ',';
            out << ')' << endl;
        }
        out << endl;
    }

    if (total_num_remote_commits) {
        out << "=Remote Stats=" << endl;
        for (uint32_t i = 0; i < NUM_REMOTE_FLOAT_STATS; i++) {
            double total = 0;
            for (uint32_t tid = 0; tid < g_total_num_threads; tid++)
                total += _stats[tid]->_remote_float_stats[i];

            total = total / total_num_remote_commits * 1000000; // in us.
            string suffix = " (in us) ";
            out << "    " << setw(33) << left << statsFloatRemoteName[i] + suffix + ':' << total / BILLION;
            out << " (";
            for (uint32_t tid = 0; tid < g_total_num_threads; tid++)
                out << _stats[tid]->_remote_float_stats[i] / BILLION << ',';
            out << ')' << endl;
        }
        out << endl;
    }

#if WORKLOAD == TPCC
    out << "TPCC Per Txn Type Stats" << endl;
    out << "    " << setw(18) << left << "Txn Name"
        << setw(12) << left << "Commits"
        << setw(12) << left << "Aborts"
        << setw(12) << left << "Latency"
        << endl;
    for (uint32_t i = 0; i < 5; i ++) {
        uint64_t commits = 0, aborts = 0;
        double time = 0;
        for (uint32_t tid = 0; tid < g_num_worker_threads; tid++) {
            commits += _stats[tid]->_commits_per_txn_type[i];
            aborts += _stats[tid]->_aborts_per_txn_type[i];
            time += 1.0 * _stats[tid]->_latency_per_txn_type[i] / MILLION;
        }
        out << "    " << setw(18) << left << TPCCHelper::get_txn_name(i)
            << setw(12) << left << commits
            << setw(12) << left << aborts
            << setw(12) << left << time / commits
            << endl;
    }
    out << endl;
#elif WORKLOAD == TPCE
    out << "TPCE Per Txn Type Stats" << endl;
    out << "    " << setw(18) << left << "Txn Name"
        << setw(12) << left << "Commits"
        << setw(12) << left << "Aborts"
        << setw(12) << left << "Latency"
        << setw(12) << left << "Execution"
        << setw(12) << left << "Prepare"
        << setw(12) << left << "Commit"
        << setw(12) << left << "Abort Time"
        << endl;
    for (uint32_t i = 0; i < 10; i ++) {
        uint64_t commits = 0, aborts = 0;
        double latency = 0, execution_phase = 0, prepare_phase = 0, commit_phase = 0, abort_time = 0;
        for (uint32_t tid = 0; tid < g_num_worker_threads; tid++) {
            commits         += _stats[tid]->_commits_per_txn_type[i];
            aborts          += _stats[tid]->_aborts_per_txn_type[i];
            latency         += 1.0 * _stats[tid]->_latency_per_txn_type[i]         / MILLION;
            execution_phase += 1.0 * _stats[tid]->_execution_phase_per_txn_type[i] / MILLION;
            prepare_phase   += 1.0 * _stats[tid]->_prepare_phase_per_txn_type[i]   / MILLION;
            commit_phase    += 1.0 * _stats[tid]->_commit_phase_per_txn_type[i]    / MILLION;
            abort_time      += 1.0 * _stats[tid]->_abort_time_per_txn_type[i]      / MILLION;
        }
        out << "    " << setw(18) << left << TPCEHelper::get_txn_name(i)
            << setw(12) << left << commits
            << setw(12) << left << aborts
            << setw(12) << left << latency         / commits
            << setw(12) << left << execution_phase / commits
            << setw(12) << left << prepare_phase   / commits
            << setw(12) << left << commit_phase    / commits
            << setw(12) << left << abort_time      / commits
            << endl;
    }
    out << endl;

    out << "TPCE Per Txn Type Stats [MP Txn]" << endl;
    out << "    " << setw(18) << left << "Txn Name"
        << setw(12) << left << "Commits"
        << setw(12) << left << "Aborts"
        << setw(12) << left << "Latency"
        << setw(12) << left << "Execution"
        << setw(12) << left << "Prepare"
        << setw(12) << left << "Commit"
        << setw(12) << left << "Abort Time"
        << endl;
    for (uint32_t i = 0; i < 10; i ++) {
        uint64_t commits = 0, aborts = 0;
        double latency = 0, execution_phase = 0, prepare_phase = 0, commit_phase = 0, abort_time = 0;
        for (uint32_t tid = 0; tid < g_num_worker_threads; tid++) {
            commits         += _stats[tid]->_mp_commits_per_txn_type[i];
            aborts          += _stats[tid]->_mp_aborts_per_txn_type[i];
            latency         += 1.0 * _stats[tid]->_mp_latency_per_txn_type[i]         / MILLION;
            execution_phase += 1.0 * _stats[tid]->_mp_execution_phase_per_txn_type[i] / MILLION;
            prepare_phase   += 1.0 * _stats[tid]->_mp_prepare_phase_per_txn_type[i]   / MILLION;
            commit_phase    += 1.0 * _stats[tid]->_mp_commit_phase_per_txn_type[i]    / MILLION;
            abort_time      += 1.0 * _stats[tid]->_mp_abort_time_per_txn_type[i]      / MILLION;
        }
        out << "    " << setw(18) << left << TPCEHelper::get_txn_name(i)
            << setw(12) << left << commits
            << setw(12) << left << aborts
            << setw(12) << left << latency         / commits
            << setw(12) << left << execution_phase / commits
            << setw(12) << left << prepare_phase   / commits
            << setw(12) << left << commit_phase    / commits
            << setw(12) << left << abort_time      / commits
            << endl;
    }
    out << endl;
#endif

    // print stats for input thread
    out << "=Input/Output Thread=" << endl;
    out << "    " << setw(25) << left
        << "Message Types"
        << setw(12) << left << "#recv"
        << setw(12) << left << "bytesRecv"
        << setw(12) << left << "#sent"
        << setw(12) << left << "bytesSent"
        << setw(12) << left << "#committed"
        << setw(12) << left << "bytesCommitted" << endl;

    for (uint32_t i = 0; i < Message::NUM_MSG_TYPES; i++) {
        string msg_name = Message::get_name( (Message::Type)i );
        out << "    " << setw(25) << left << msg_name
             << setw(12) << left << _stats[g_total_num_threads - 2]->_msg_count[i]
             << setw(12) << left << _stats[g_total_num_threads - 2]->_msg_size[i]
             << setw(12) << left << _stats[g_total_num_threads - 1]->_msg_count[i]
             << setw(12) << left << _stats[g_total_num_threads - 1]->_msg_size[i];
        uint64_t committed_count = 0;
        uint64_t committed_size = 0;
        for (uint32_t j = 0; j < g_num_worker_threads; j ++) {
            committed_count += _stats[j]->_msg_committed_count[i];
            committed_size += _stats[j]->_msg_committed_size[i];
        }
        out << setw(12) << left << committed_count
            << setw(12) << left << committed_size
            << endl;
    }
    
    if (g_num_nodes > 1) {
        // print ping latency
        out << "\n==Ping Latencies==\n";
        for (uint32_t i = 0; i < g_num_nodes; i++) {
            out << i << ": " << transport->get_ping_latency(i) / 1000 << endl;
        }
    } 

    // print the checkpoints
    if (_checkpoints.size() > 1) {
        out << "\n=Check Points=\n" << endl;
        out << "Metrics:\tthr,";
        for    (uint32_t i = 0; i < NUM_INT_STATS; i++)
            out << statsIntName[i] << ',';
        for    (uint32_t i = 0; i < NUM_FLOAT_STATS; i++)
            out << statsFloatName[i] << ',';
        for (uint32_t i = 0; i < Message::NUM_MSG_TYPES; i++)
            out << Message::get_name( (Message::Type)i ) << ',';
        out << endl;
    }

    for (uint32_t i = 1; i < _checkpoints.size(); i ++)
    {
        uint64_t num_commits = 0;
        for (uint32_t tid = 0; tid < g_total_num_threads; tid++) {
            num_commits += _checkpoints[i]->_stats[tid]->_int_stats[STAT_num_commits];
            num_commits -= _checkpoints[i - 1]->_stats[tid]->_int_stats[STAT_num_commits];
        }
        double thr = 1.0 * num_commits / STATS_CP_INTERVAL * 1000;
        out << "CP" << i << ':';
        out << "\t" << thr << ',';
        for (uint32_t n = 0; n < NUM_INT_STATS; n++) {
            uint64_t value = 0;
            for (uint32_t tid = 0; tid < g_total_num_threads; tid++) {
                value += _checkpoints[i]->_stats[tid]->_int_stats[n];
                value -= _checkpoints[i - 1]->_stats[tid]->_int_stats[n];
            }
            out << value << ',';
        }
        for (uint32_t n = 0; n < NUM_FLOAT_STATS; n++) {
            double value = 0;
            for (uint32_t tid = 0; tid < g_total_num_threads; tid++) {
                value += _checkpoints[i]->_stats[tid]->_float_stats[n];
                value -= _checkpoints[i - 1]->_stats[tid]->_float_stats[n];
            }
            out << value / BILLION << ',';
        }
        for (uint32_t n = 0; n < Message::NUM_MSG_TYPES; n++) {
            uint64_t value = 0;
            // for input thread
            value += _checkpoints[i]->_stats[g_total_num_threads - 2]->_msg_count[n];
            value -= _checkpoints[i - 1]->_stats[g_total_num_threads - 2]->_msg_count[n];
            out << value << ',';
        }
        out << endl;
    }
}

void Stats::print()
{
    ofstream file;
    bool write_to_file = false;
    if (output_file != NULL) {
        write_to_file = true;
        file.open(output_file);
    }
    // compute the latency distribution
#if COLLECT_LATENCY
    for (uint32_t tid = 0; tid < g_total_num_threads; tid++) {
        assert(_stats[tid]->all_latency.size() == _stats[tid]->_int_stats[STAT_num_commits]);
        assert(_stats[tid]->sp_latency.size() == _stats[tid]->_int_stats[STAT_num_home_sp_commits]);
        assert(_stats[tid]->mp_latency.size() == _stats[tid]->_int_stats[STAT_num_home_mp_commits]);

        // TODO. should exclude txns during the warmup
        _agg_latency.insert(_agg_latency.end(), _stats[tid]->all_latency.begin(), _stats[tid]->all_latency.end());
        _agg_sp_latency.insert(_agg_sp_latency.end(), _stats[tid]->sp_latency.begin(), _stats[tid]->sp_latency.end());
        _agg_mp_latency.insert(_agg_mp_latency.end(), _stats[tid]->mp_latency.begin(), _stats[tid]->mp_latency.end());
    }
    std::sort(_agg_latency.begin(), _agg_latency.end());
    std::sort(_agg_sp_latency.begin(), _agg_sp_latency.end());
    std::sort(_agg_mp_latency.begin(), _agg_mp_latency.end());
#endif
    output(&cout);
    if (write_to_file) {
        std::ofstream fout (output_file);
        g_warmup_time = 0;
        output(&fout);
        fout.close();
    }

    return;
}

void Stats::print_lat_distr() {}

void
Stats::checkpoint()
{
    Stats * stats = new Stats();
    stats->copy_from(this);
    if (_checkpoints.size() > 0)
        for (uint32_t i = 0; i < NUM_INT_STATS; i++)
            assert(stats->_stats[0]->_int_stats[i] >= _checkpoints.back()->_stats[0]->_int_stats[i]);
    _checkpoints.push_back(stats);
    COMPILER_BARRIER
    _num_cp ++;
}

void
Stats::copy_from(Stats * stats)
{
    // TODO. this checkpoint may be slightly inconsistent. But it should be fine.
    for (uint32_t i = 0; i < g_total_num_threads; i ++)
        _stats[i]->copy_from(stats->_stats[i]);
}

double
Stats::last_cp_bytes_sent(double &dummy_bytes)
{
    uint32_t num_cp = _num_cp;
    if (num_cp > 0) {
        if (num_cp == 1) {
            Stats * cp = _checkpoints[num_cp - 1];
            dummy_bytes = cp->_stats[g_total_num_threads - 1]->_float_stats[STAT_dummy_bytes_sent];
            return cp->_stats[g_total_num_threads - 1]->_float_stats[STAT_bytes_sent];
        } else {
            Stats * cp1 = _checkpoints[num_cp - 1];
            Stats * cp2 = _checkpoints[num_cp - 2];
            dummy_bytes = cp1->_stats[g_total_num_threads - 1]->_float_stats[STAT_dummy_bytes_sent]
                        - cp2->_stats[g_total_num_threads - 1]->_float_stats[STAT_dummy_bytes_sent];
            return cp1->_stats[g_total_num_threads - 1]->_float_stats[STAT_bytes_sent]
                 - cp2->_stats[g_total_num_threads - 1]->_float_stats[STAT_bytes_sent];
        }
    } else {
        dummy_bytes = 0;
        return 0;
    }
}
