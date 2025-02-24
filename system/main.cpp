#include "global.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "tpcc.h"
#include "tpce.h"
#include "bank.h"
#include "server_thread.h"
#include "manager.h"
#include "query.h"
#include "transport.h"
#include "txn_table.h"
#include "input_thread.h"
#include "output_thread.h"
#include "log.h"
#include "raft_thread.h"
#include "hotness_thread.h"
#include "calvin_thread.h"
#include "calvin_worker_thread.h"

void * start_thread(void *);

InputThread ** input_threads;
OutputThread ** output_threads;
RaftThread ** raft_threads;
HotnessThread ** hotness_threads;
CalvinThread ** calvin_threads;
CalvinWorkerThread ** calvin_worker_threads;

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
    pin_thd_to_numa_node(pthread_self(), 0);
    parser(argc, argv);
    init_tsc();
    M_ASSERT(INDEX_STRUCT != IDX_BTREE, "btree is not supported yet\n");
    transport = new Transport (0, ifconfig_file);

#if CC_ALG == CALVIN

    g_num_worker_threads = 1;
    g_sequencer_threads = 1;
    g_total_num_threads = g_num_worker_threads + g_sequencer_threads + g_num_input_threads +
                          g_num_output_threads + g_num_raft_instances;

    input_queues = new InOutQueue * [g_total_num_threads];
    raft_log_queues = new LogQueue * [g_num_raft_instances];
    raft_msg_queues = new InOutQueue * [g_num_raft_instances];
    log_apply_queues = new LogApplyQueue * [g_sequencer_threads];

    for (uint32_t i = 0; i < g_sequencer_threads; i++) {
        log_apply_queues[i] = (LogApplyQueue *) _mm_malloc(sizeof(LogApplyQueue), 64);
        new (log_apply_queues[i]) LogApplyQueue(1024);
    }
    
    for (uint32_t i = 0; i < g_num_raft_instances; i++) {
        raft_log_queues[i] = (LogQueue *) _mm_malloc(sizeof(LogQueue), 64);
        new (raft_log_queues[i]) LogQueue(1024);
        raft_msg_queues[i] = (InOutQueue *) _mm_malloc(sizeof(InOutQueue), 64);
        new (raft_msg_queues[i]) InOutQueue(1024);
    }

    for (uint32_t i = 0; i < g_total_num_threads; i++) {
        input_queues[i] = (InOutQueue *) _mm_malloc(sizeof(InOutQueue), 64);
        new (input_queues[i]) InOutQueue(1024);
    }

#else

    g_num_worker_threads = g_num_server_threads;
    g_total_num_threads  = g_num_worker_threads + g_num_input_threads + g_num_output_threads +
                           g_num_raft_instances + g_num_hotness_threads;

  #if DEBUG_MSG_WAIT_INQUEUE
    msg_arrival_times = (MsgArrivalQueue *) _mm_malloc(sizeof(MsgArrivalQueue), 64);
    new (msg_arrival_times) MsgArrivalQueue(1024);
  #endif
    input_queues = new InOutQueue * [g_num_worker_threads];
    ready_queues = new WaitQueue * [g_num_worker_threads];
    hotness_msg_queues = new InOutQueue * [g_num_hotness_threads];
    raft_log_queues = new LogQueue * [g_num_raft_instances];
    raft_msg_queues = new InOutQueue * [g_num_raft_instances];
    log_apply_queues = new LogApplyQueue * [g_num_worker_threads];

    for (uint32_t i = 0; i < g_num_worker_threads; i++) {
        input_queues[i] = (InOutQueue *) _mm_malloc(sizeof(InOutQueue), 64);
        new (input_queues[i]) InOutQueue(1024);
        ready_queues[i] = (WaitQueue *) _mm_malloc(sizeof(WaitQueue), 64);
        new (ready_queues[i]) WaitQueue(1024);
        log_apply_queues[i] = (LogApplyQueue *) _mm_malloc(sizeof(LogApplyQueue), 64);
        new (log_apply_queues[i]) LogApplyQueue(1024);
    }

    for (uint32_t i = 0; i < g_num_raft_instances; i++) {
        raft_log_queues[i] = (LogQueue *) _mm_malloc(sizeof(LogQueue), 64);
        new (raft_log_queues[i]) LogQueue(1024);
        raft_msg_queues[i] = (InOutQueue *) _mm_malloc(sizeof(InOutQueue), 64);
        new (raft_msg_queues[i]) InOutQueue(1024);
    }

    for (uint32_t i = 0; i < g_num_hotness_threads; i++) {
        hotness_msg_queues[i] = (InOutQueue *) _mm_malloc(sizeof(InOutQueue), 64);
        new (hotness_msg_queues[i]) InOutQueue(1024);
    }

#endif

    stats = (Stats *) _mm_malloc(sizeof(Stats), 64);
    new(stats) Stats();

    glob_manager = (Manager *) _mm_malloc(sizeof(Manager), 64);
    glob_manager->init();

    printf("mem_allocator initialized!\n");
    workload * m_wl;
    switch (WORKLOAD) {
        case YCSB :
            m_wl = new WorkloadYCSB;
            QueryYCSB::calculateDenom();
            break;
        case TPCC :
            m_wl = new WorkloadTPCC;
            break;
        case TPCE :
            m_wl = new WorkloadTPCE;
            break;
        case BANK :
            m_wl = new WorkloadBank;
            break;
        default:
            assert(false);
    }

    glob_manager->set_workload(m_wl);
    m_wl->init();

#if WORKLOAD == BANK && DEBUG_TXN_RESULT
    for (uint32_t i = 0; i < g_num_server_threads; i++) {
        balances[i] = new int64_t[BANK_NUM_KEYS * g_num_parts];
        memset(balances[i], 0, BANK_NUM_KEYS * g_num_parts * sizeof(int64_t));
    }
#endif
    printf("workload initialized!\n");
    if (g_num_nodes > 1)
        transport->test_connect();

    cout << "Warmup time: " << g_warmup_time << "s. Run time: " << g_run_time << "s." << endl;

#if CC_ALG == CALVIN

    calvin_threads = new CalvinThread * [g_sequencer_threads];
    calvin_worker_threads = new CalvinWorkerThread * [g_num_worker_threads];
    input_threads = new InputThread * [g_num_input_threads];
    output_threads = new OutputThread * [g_num_output_threads];
    raft_threads = new RaftThread * [g_num_raft_instances];
    for (uint64_t i = 0; i < g_sequencer_threads; i++)
        calvin_threads[i] = new CalvinThread(i);
    for (uint64_t i = 0; i < g_num_worker_threads; i++)
        calvin_worker_threads[i] = new CalvinWorkerThread(i + g_sequencer_threads);
    for (uint64_t i = 0; i < g_num_raft_instances; i++)
        raft_threads[i] = new RaftThread(i + g_sequencer_threads + g_num_worker_threads, i);
    for (uint64_t i = 0; i < g_num_input_threads; i++)
        input_threads[i] = new InputThread(i + g_sequencer_threads + g_num_worker_threads + g_num_raft_instances, transport);
    for (uint64_t i = 0; i < g_num_output_threads; i++)
        output_threads[i] = new OutputThread(i + g_sequencer_threads + g_num_worker_threads + g_num_raft_instances + g_num_input_threads, transport); 

    pthread_barrier_init(&global_barrier, NULL, g_total_num_threads);
    pthread_mutex_init(&global_lock, NULL);

    warmup_finish = true;

    pthread_t pthreads[g_total_num_threads];
    // spawn and run txns
    timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t start_t = tp->tv_sec * 1000000000 + tp->tv_nsec;

    int64_t starttime = get_server_clock();
    for (uint64_t i = 0; i < g_num_worker_threads; i++)
        pthread_create(&pthreads[i], NULL, start_thread, (void *)calvin_worker_threads[i]);
    for (uint64_t i = 0; i < g_num_input_threads; i++)
        pthread_create(&pthreads[g_num_worker_threads + i], NULL, start_thread, (void *)input_threads[i]);
    for (uint64_t i = 0; i < g_num_output_threads; i++)
        pthread_create(&pthreads[g_num_worker_threads + g_num_input_threads + i], NULL, start_thread, (void *)output_threads[i]);
    for (uint64_t i = 0; i < g_num_raft_instances; i++)
        pthread_create(&pthreads[g_num_worker_threads + g_num_input_threads + g_num_output_threads + i], NULL, start_thread, (void *)raft_threads[i]);

    start_thread((void *) (calvin_threads[0]));

    for (uint32_t i = 0; i < g_num_worker_threads; i++)
        pthread_join(pthreads[i], NULL);
    for (uint32_t i = 0; i < g_num_raft_instances; i++)
        raft_threads[i]->signal_to_stop();
    for (uint32_t i = 0; i < g_num_input_threads; i++)
        input_threads[i]->signal_to_stop();
    for (uint32_t i = 0; i < g_num_output_threads; i++)
        output_threads[i]->signal_to_stop();

#else

    Thread ** worker_threads;
    server_threads = new ServerThread * [g_num_worker_threads];
    for (uint32_t i = 0; i < g_num_worker_threads; i++)
        server_threads[i] = new ServerThread(i);
    worker_threads = (Thread **) server_threads;
    input_threads = new InputThread * [g_num_input_threads];
    output_threads = new OutputThread * [g_num_output_threads];
    raft_threads = new RaftThread * [g_num_raft_instances];
    hotness_threads = new HotnessThread * [g_num_hotness_threads];
    for (uint64_t i = 0; i < g_num_raft_instances; i++)
        raft_threads[i] = new RaftThread(i + g_num_worker_threads, i);
    for (uint64_t i = 0; i < g_num_input_threads; i++)
        input_threads[i] = new InputThread(i + g_num_worker_threads + g_num_raft_instances, transport);
    for (uint64_t i = 0; i < g_num_output_threads; i++)
        output_threads[i] = new OutputThread(i + g_num_worker_threads + g_num_raft_instances + g_num_input_threads, transport); 
    for (uint64_t i = 0; i < g_num_hotness_threads; i++)
        hotness_threads[i] = new HotnessThread(i + g_num_worker_threads + g_num_raft_instances + g_num_input_threads + g_num_output_threads, i);

    pthread_barrier_init(&global_barrier, NULL, g_total_num_threads);
    pthread_mutex_init(&global_lock, NULL);

    warmup_finish = true;

    pthread_t pthreads[g_total_num_threads];
    // spawn and run txns
    timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t start_t = tp->tv_sec * 1000000000 + tp->tv_nsec;

    int64_t starttime = get_server_clock();
    for (uint64_t i = 0; i < g_num_worker_threads - 1; i++)
        pthread_create(&pthreads[i], NULL, start_thread, (void *)worker_threads[i]);
    for (uint64_t i = 0; i < g_num_input_threads; i++)
        pthread_create(&pthreads[g_num_worker_threads + i], NULL, start_thread, (void *)input_threads[i]);
    for (uint64_t i = 0; i < g_num_output_threads; i++)
        pthread_create(&pthreads[g_num_worker_threads + g_num_input_threads + i], NULL, start_thread, (void *)output_threads[i]);
    for (uint64_t i = 0; i < g_num_raft_instances; i++)
        pthread_create(&pthreads[g_num_worker_threads + g_num_input_threads + g_num_output_threads + i], NULL, start_thread, (void *)raft_threads[i]);
    for (uint64_t i = 0; i < g_num_hotness_threads; i++)
        pthread_create(&pthreads[g_num_worker_threads + g_num_input_threads + g_num_output_threads + g_num_raft_instances + i], NULL, start_thread, (void *)hotness_threads[i]);

    start_thread((void *) (worker_threads[g_num_worker_threads - 1]));

    for (uint32_t i = 0; i < g_num_worker_threads - 1; i++)
        pthread_join(pthreads[i], NULL);
    for (uint32_t i = 0; i < g_num_hotness_threads; i++)
        hotness_threads[i]->signal_to_stop();
    for (uint32_t i = 0; i < g_num_raft_instances; i++)
        raft_threads[i]->signal_to_stop();
    for (uint32_t i = 0; i < g_num_input_threads; i++)
        input_threads[i]->signal_to_stop();
    for (uint32_t i = 0; i < g_num_output_threads; i++)
        output_threads[i]->signal_to_stop();
    for (uint64_t i = 0; i < g_num_input_threads + g_num_output_threads + g_num_raft_instances + g_num_hotness_threads; i++)
        pthread_join(pthreads[g_num_worker_threads + i], NULL);

#endif

    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t end_t = tp->tv_sec * 1000000000 + tp->tv_nsec;

    int64_t endtime = get_server_clock();
    int64_t runtime = end_t - start_t;

    if (abs(1.0 * runtime / (endtime - starttime) - 1) > 0.01)
        M_ASSERT(false, "the CPU_FREQ is inaccurate! correct value should be %f\n",
            1.0 * (endtime - starttime) * CPU_FREQ / runtime);
    printf("PASS! SimTime = %ld\n", endtime - starttime);

    if (STATS_ENABLE)
        stats->print();

    if (WORKLOAD == BANK) {
        WorkloadBank *workload_bank = (WorkloadBank *) m_wl;
        workload_bank->verify_integrity();
    }
    return 0;
}

void * start_thread(void * thread) {
    Thread * thd = (Thread *) thread;
    thd->run();
    return NULL;
}
