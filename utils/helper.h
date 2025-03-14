#pragma once

#include <cstdlib>
#include <iostream>
#include <stdint.h>
#include <chrono>
#include "global.h"
#define BILLION 1000000000UL
#define MILLION 1000000UL

//////////////////////////////////////////////////
// atomic operations
//////////////////////////////////////////////////
#define ATOM_ADD(dest, value) \
    __sync_fetch_and_add(&(dest), value)
#define ATOM_SUB(dest, value) \
    __sync_fetch_and_sub(&(dest), value)
// returns true if cas is successful
#define ATOM_CAS(dest, oldval, newval) \
    __sync_bool_compare_and_swap(&(dest), oldval, newval)
#define ATOM_ADD_FETCH(dest, value) \
    __sync_add_and_fetch(&(dest), value)
#define ATOM_FETCH_ADD(dest, value) \
    __sync_fetch_and_add(&(dest), value)
#define ATOM_SUB_FETCH(dest, value) \
    __sync_sub_and_fetch(&(dest), value)
#define ATOM_COUT(msg) \
    stringstream sstream; sstream << msg; cout << sstream.str();

#define COMPILER_BARRIER asm volatile("" ::: "memory");
// on draco, each pause takes approximately 3.7 ns.
#define PAUSE __asm__ ( "pause;" );
#define PAUSE10 \
    PAUSE PAUSE PAUSE PAUSE PAUSE \
    PAUSE PAUSE PAUSE PAUSE PAUSE

// about 370 ns.
#define PAUSE100 \
    PAUSE10    PAUSE10    PAUSE10    PAUSE10    PAUSE10 \
    PAUSE10    PAUSE10    PAUSE10    PAUSE10    PAUSE10

#define NANOSLEEP(t) { \
    timespec time {0, t}; \
    nanosleep(&time, NULL); }
//////////////////////////////////////////////////
// DEBUG print
//////////////////////////////////////////////////
#define DEBUG_PRINT(...) \
    if (true) \
        printf(__VA_ARGS__);

//////////////////////////////////////////////////
// ASSERT Helper
//////////////////////////////////////////////////
#define M_ASSERT(cond, ...) \
    if (!(cond)) {\
        printf("ASSERTION FAILURE [%s : %d] ", __FILE__, __LINE__); \
        printf(__VA_ARGS__);\
        fprintf(stderr, "ASSERTION FAILURE [%s : %d] ", \
        __FILE__, __LINE__); \
        fprintf(stderr, __VA_ARGS__);\
        assert(false);\
    }

#define ASSERT(cond) assert(cond)

//////////////////////////////////////////////////
// Global Data Structure
//////////////////////////////////////////////////
#define GET_WORKLOAD glob_manager->get_workload()
#define GLOBAL_NODE_ID g_node_id
#define GET_THD_ID glob_manager->get_thd_id()


//////////////////////////////////////////////////
// STACK helper (push & pop)
//////////////////////////////////////////////////
#define STACK_POP(stack, top) { \
    if (stack == NULL) top = NULL; \
    else {    top = stack;     stack=stack->next; } }
#define STACK_PUSH(stack, entry) {\
    entry->next = stack; stack = entry; }

//////////////////////////////////////////////////
// LIST helper (read from head & write to tail)
//////////////////////////////////////////////////
#define LIST_GET_HEAD(lhead, ltail, en) {\
    en = lhead; \
    lhead = lhead->next; \
    if (lhead) lhead->prev = NULL; \
    else ltail = NULL; \
    en->next = NULL; }
#define LIST_PUT_TAIL(lhead, ltail, en) {\
    en->next = NULL; \
    en->prev = NULL; \
    if (ltail) { en->prev = ltail; ltail->next = en; ltail = en; } \
    else { lhead = en; ltail = en; }}
#define LIST_INSERT_BEFORE(entry, newentry) { \
    newentry->next = entry; \
    newentry->prev = entry->prev; \
    if (entry->prev) entry->prev->next = newentry; \
    entry->prev = newentry; }
#define LIST_REMOVE(entry) { \
    if (entry->next) entry->next->prev = entry->prev; \
    if (entry->prev) entry->prev->next = entry->next; }
#define LIST_REMOVE_HT(entry, head, tail) { \
    if (entry->next) entry->next->prev = entry->prev; \
    else { assert(entry == tail); tail = entry->prev; } \
    if (entry->prev) entry->prev->next = entry->next; \
    else { assert(entry == head); head = entry->next; } \
}

//////////////////////////////////////////////////
// STATS helper
//////////////////////////////////////////////////
#define INC_STATS(tid, name, value) \
    ;

#define INC_TMP_STATS(tid, name, value) \
    ;

#define INC_GLOB_STATS(name, value) \
    if (STATS_ENABLE) \
        stats->name += value;

#define INC_SP_FLOAT_STATS(name, value) { \
    if (STATS_ENABLE) \
        stats->_stats[GET_THD_ID]->_sp_float_stats[STAT_sp_##name] += value; }

#define INC_MP_FLOAT_STATS(name, value) { \
    if (STATS_ENABLE) \
        stats->_stats[GET_THD_ID]->_mp_float_stats[STAT_mp_##name] += value; }

#define INC_REMOTE_FLOAT_STATS(name, value) { \
    if (STATS_ENABLE) \
        stats->_stats[GET_THD_ID]->_remote_float_stats[STAT_remote_##name] += value; }

#define INC_FLOAT_STATS(name, value) { \
    if (STATS_ENABLE) \
        stats->_stats[GET_THD_ID]->_float_stats[STAT_##name] += value; }

#define INC_INT_STATS(name, value) {{ \
    if (STATS_ENABLE) \
        stats->_stats[GET_THD_ID]->_int_stats[STAT_##name] += value; }}

#define TIME_STATS(dim1, dim2, value) { \
    if (STATS_ENABLE) \
        stats->_stats[GET_THD_ID]->_time_breakdown[TIME_##dim1][TIME_##dim2] += value; }

#define INC_MSG_SIZE(type, size) { \
    if (STATS_ENABLE) \
        stats->_stats[GET_THD_ID]->_msg_size[type] += size; }

#define INC_MSG_COUNT(type, count) { \
    if (STATS_ENABLE) \
        stats->_stats[GET_THD_ID]->_msg_count[type] += count; }

#define INC_MSG_STATS(type, size) { \
    if (STATS_ENABLE) { \
        stats->_stats[GET_THD_ID]->_msg_size[type] += size; \
        stats->_stats[GET_THD_ID]->_msg_count[type] += 1;}}

//////////////////////////////////////////////////
// Malloc helper
//////////////////////////////////////////////////

#define ALIGNED(x) __attribute__ ((aligned(x)))
#define TXN_STAGE_BEGIN stage_begin:
#define TXN_STAGE(x) if (_txn_state == x)
#define TXN_TO_STAGE(x) {_txn_state = x; goto stage_begin;}

#if TS_ALLOC == TS_CLOCK || TS_ALLOC == TS_HYBRID
extern uint64_t _init_clock;
extern uint64_t _init_tsc;
#endif

int get_thdid_from_txnid(uint64_t txnid);

// key_to_part() is only for ycsb
uint64_t key_to_part(uint64_t key);
uint64_t get_part_id(void * addr);

uint64_t txn_id_to_node_id(uint64_t txn_id);
uint64_t txn_id_to_thread_id(uint64_t txn_id);
void init_tsc();

extern timespec * res;
inline uint64_t get_server_clock() {
#if defined(__i386__)
    uint64_t ret;
    __asm__ __volatile__("rdtsc" : "=A" (ret));

#elif defined(__x86_64__)
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    uint64_t ret = ( (uint64_t)lo)|( ((uint64_t)hi)<<32 );
    ret = (uint64_t) ((double)ret / CPU_FREQ);
    return ret - _init_tsc + _init_clock;

#else
    timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t ret = tp->tv_sec * 1000000000 + tp->tv_nsec;
#endif
    return ret;
}

inline uint64_t get_relative_clock() {
    timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    uint64_t ret = tp.tv_sec * 1000000000 + tp.tv_nsec;
    return ret;
}

inline uint64_t get_sys_clock() {
#if TIME_ENABLE
    return get_server_clock();
#else
    return 0;
#endif
}

uint32_t get_cpus_per_node();
bool pin_thd_to_numa_node(pthread_t thd, uint32_t node_id);
bool pin_thread_by_id(pthread_t thd, uint32_t thd_id);
