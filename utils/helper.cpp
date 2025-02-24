#include "global.h"
#include "helper.h"
#include "time.h"
#include <unistd.h>

//int get_thdid_from_txnid(uint64_t txnid) {
//    return txnid % g_num_worker_threads;
//}

//uint64_t get_part_id(void * addr) {
//    return ((uint64_t)addr / PAGE_SIZE) % g_part_cnt;
//}

//uint64_t key_to_part(uint64_t key) {
//    return 0;
//}
//
//uint64_t txn_id_to_node_id(uint64_t txn_id)
//{
//    return txn_id % g_num_nodes;
//}
//
//uint64_t txn_id_to_thread_id(uint64_t txn_id)
//{
//    return (txn_id / g_num_server_nodes) % g_num_server_threads;
//}

#if TS_ALLOC == TS_CLOCK || TS_ALLOC == TS_HYBRID
uint64_t _init_clock;
uint64_t _init_tsc;

void init_tsc()
{
	timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    _init_clock = tp->tv_sec * 1000000000 + tp->tv_nsec;
#if defined(__x86_64__)
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    _init_tsc = ( (uint64_t)lo)|( ((uint64_t)hi)<<32 );
    _init_tsc = (uint64_t) ((double)_init_tsc / CPU_FREQ);
#endif
}

inline uint32_t get_num_numa_nodes()
{
    if (numa_available() < 0)
        return 1;
    return numa_max_node() + 1;
}

uint32_t get_cpus_per_node()
{
    if (numa_available() < 0) {
        return sysconf(_SC_NPROCESSORS_ONLN);
    } else {
        struct bitmask *cpuset = numa_allocate_cpumask();
        numa_bitmask_clearall(cpuset);
        assert(numa_node_to_cpus(0, cpuset) != -1);
        uint32_t ret = CPU_COUNT_S(cpuset->size / 8, (cpu_set_t*)cpuset->maskp);
        numa_free_cpumask(cpuset);
        return ret;
    }
}

bool pin_thd_to_numa_node(pthread_t thd, uint32_t node_id)
{
    if (numa_available() < 0) {
        if (node_id == 0)
            return true;
        return false;
    }
    if (get_num_numa_nodes() <= node_id)
        return false;

    struct bitmask *cpuset = numa_allocate_cpumask();
    numa_bitmask_clearall(cpuset);   

    assert(numa_node_to_cpus(node_id, cpuset) != -1);
    assert(pthread_setaffinity_np(thd, cpuset->size / 8, (cpu_set_t*)cpuset->maskp) == 0);

    numa_free_cpumask(cpuset); 
    return true;
}

bool pin_thread_by_id(pthread_t thd, uint32_t thd_id)
{

    long num_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    if (numa_available() < 0) {
        if (thd_id >= num_cpus)
            return false;
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(thd_id, &cpuset);
        assert(pthread_setaffinity_np(thd, sizeof(cpuset), &cpuset) == 0);
    } else {
        struct bitmask *cpuset = numa_allocate_cpumask();
        uint32_t node_id;
        for (node_id = 0; node_id < get_num_numa_nodes(); node_id ++) {
            numa_bitmask_clearall(cpuset);   
            assert(numa_node_to_cpus(node_id, cpuset) != -1);
            uint32_t core_count = CPU_COUNT_S(cpuset->size / 8, (cpu_set_t*)cpuset->maskp) / 2;
            for (uint32_t cpu_id = 0; cpu_id < num_cpus && core_count > 0; cpu_id ++) {
                if (numa_bitmask_isbitset(cpuset, cpu_id)) {
                    if (thd_id == 0) {
                        numa_bitmask_clearall(cpuset);   
                        numa_bitmask_setbit(cpuset, cpu_id);
                        assert(pthread_setaffinity_np(thd, cpuset->size / 8, (cpu_set_t*)cpuset->maskp) == 0);
                        return true;
                    }
                    thd_id -= 1;
                    core_count -= 1;
                }
            }
        }
    }
    return false;
}

#endif