import os
from experiments import *
from simulate_region_latency import network_topology

partitions_dict = {
    # Two partitions, two replicas
    2: [
        ["ssd4.dbg.private", "ssd6.dbg.private"],
        ["ssd7.dbg.private", "ssd5.dbg.private"]
    ],
    # Three partitions, three replicas
    3: [
        ["ssd4.dbg.private", "ssd7.dbg.private", "ssd10.dbg.private"],
        ["ssd8.dbg.private", "ssd5.dbg.private", "ssd11.dbg.private"],
        ["ssd12.dbg.private", "ssd6.dbg.private", "ssd9.dbg.private"]
    ],
    # Five partitions, five replicas (ssd15 is down)
    5: [
        ["ssd4.dbg.private", "ssd9.dbg.private", "ssd14.dbg.private", "ssd19.dbg.private", "ssd27.dbg.private"],
        ["ssd10.dbg.private", "ssd5.dbg.private", "ssd2.dbg.private", "ssd20.dbg.private", "ssd28.dbg.private"],
        ["ssd16.dbg.private", "ssd6.dbg.private", "ssd11.dbg.private", "ssd24.dbg.private", "ssd29.dbg.private"],
        ["ssd25.dbg.private", "ssd7.dbg.private", "ssd12.dbg.private", "ssd17.dbg.private", "ssd30.dbg.private"],
        ["ssd31.dbg.private", "ssd8.dbg.private", "ssd13.dbg.private", "ssd18.dbg.private", "ssd26.dbg.private"]
    ]
}

# Values in latency matrices represent the round-trip time between two nodes (two-way communication latency)
latency_matrix_list = [
    # Uniform topology with 10ms latency
    [
        [0, 10, 10],
        [10, 0, 10],
        [10, 10, 0]
    ],
    # Uniform topology with 30ms latency
    [
        [0, 30, 30],
        [30, 0, 30],
        [30, 30, 0]
    ],
    # Uniform topology with 100ms latency
    [
        [0, 100, 100],
        [100, 0, 100],
        [100, 100, 0]
    ],
    # Non-uniform topology with two regions close to each other
    [
        [0, 30, 30],
        [30, 0, 10],
        [30, 10, 0]
    ],
    # Two regions, three datacenters
    [
        [0, 0.2, 100],
        [0.2, 0, 100],
        [100, 100, 0]
    ],
    # Uniform topology with 30ms latency
    [
        [0, 30, 30, 30, 30],
        [30, 0, 30, 30, 30],
        [30, 30, 0, 30, 30],
        [30, 30, 30, 0, 30],
        [30, 30, 30, 30, 0]
    ],
    # Uniform topology with 100ms latency
    [
        [0, 100, 100, 100, 100],
        [100, 0, 100, 100, 100],
        [100, 100, 0, 100, 100],
        [100, 100, 100, 0, 100],
        [100, 100, 100, 100, 0]
    ],
    # Three regions, five datacenters
    [
        [0, 0.2, 60, 60, 50],
        [0.2, 0, 60, 60, 50],
        [60, 60, 0, 0.2, 10],
        [60, 60, 0.2, 0, 10],
        [50, 50, 10, 10, 0]
    ],
    # Beijing, Chengdu, Hangzhou, Shanghai, Hong Kong
    [
        [0, 30, 30, 30, 60],
        [30, 0, 30, 40, 30],
        [30, 30, 0, 10, 30],
        [30, 40, 10, 0, 30],
        [60, 30, 30, 30, 0]
    ],
    # Setup used in Natto: VA, WA, PR, NSW, SG
    [
        [0, 67, 80, 196, 214],
        [67, 0, 136, 175, 163],
        [80, 136, 0, 234, 149],
        [196, 175, 234, 0, 87],
        [214, 163, 149, 87, 0]
    ]
]

# Experiment settings
topology_list = [9]

# YCSB
ycsb_num_txn_list = [167, 333, 500, 667, 833, 1000]
theta_list = [0.2, 0.8]
# ycsb_num_txn_list = [500]
# theta_list = [0.8]
perc_dist_txn_list = [0.1]
# perc_dist_txn_list = [0, 0.1, 0.25, 0.5, 0.75, 1]
read_perc_list = [0.5]
# read_perc_list = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]

# TPCC
tpcc_num_txn_list = [33, 50, 67, 83, 100]
tpcc_num_wh_list = [50, 300]
# tpcc_num_txn_list = [50]
# tpcc_num_wh_list = [50]

# Bonspiel
heuristic_perc_list = [0.2]
bonspiel_threshold_list = [0.001]

for i, latency_matrix in reversed(list(enumerate(latency_matrix_list))):
    # Run a set of topology settings
    if i not in topology_list:
        continue

    num_partition = len(latency_matrix)
    partitions = partitions_dict[num_partition]

    # Set up network topology according to the latency matrix
    network_topology(num_partition, num_partition, latency_matrix)

    # YCSB experiments
    jobs.clear()
    for c in range(1):
        for theta in theta_list:
            for num_txn in ycsb_num_txn_list:
                for perc_dist_txn in perc_dist_txn_list:
                    for read_perc in read_perc_list:
                        # Bonspiel
                        for bonspiel_threshold in bonspiel_threshold_list:
                            insert_job(partitions, "YCSB", "DOCC", "RAFT_2PC", "BONSPIEL", zipf_theta=theta, num_native_txns=num_txn, bonspiel_threshold=bonspiel_threshold, early_write_install=1, multiple_priority_levels=1, perc_dist_txn=perc_dist_txn, read_perc=read_perc)
                            # insert_job(partitions, "YCSB", "DOCC", "RAFT_2PC", "BONSPIEL", zipf_theta=theta, num_native_txns=num_txn, bonspiel_threshold=bonspiel_threshold, early_write_install=1, multiple_priority_levels=1, perc_dist_txn=perc_dist_txn, read_perc=read_perc, bonspiel_no_reser=1)
                            # insert_job(partitions, "YCSB", "DOCC", "RAFT_2PC", "BONSPIEL", zipf_theta=theta, num_native_txns=num_txn, bonspiel_threshold=bonspiel_threshold, early_write_install=1, multiple_priority_levels=1, perc_dist_txn=perc_dist_txn, read_perc=read_perc, bonspiel_no_ams=1)
                        # Spanner
                        insert_job(partitions, "YCSB", "NO_WAIT",    "RAFT_2PC", "READ_LEADER",         zipf_theta=theta, num_native_txns=num_txn, opt_commit=0, opt_coord=0, opt_leader=0, perc_dist_txn=perc_dist_txn, read_perc=read_perc)
                        # CRDB
                        insert_job(partitions, "YCSB", "DOCC",       "RAFT_2PC", "READ_LEADER",         zipf_theta=theta, num_native_txns=num_txn, opt_commit=0, opt_coord=1, opt_leader=0, perc_dist_txn=perc_dist_txn, read_perc=read_perc)
                        # TAPIR
                        insert_job(partitions, "YCSB", "DOCC",       "TAPIR",    "READ_LEADER",         zipf_theta=theta, num_native_txns=num_txn, perc_dist_txn=perc_dist_txn, read_perc=read_perc)
                        # GPAC
                        insert_job(partitions, "YCSB", "DOCC",       "GPAC",     "READ_LEADER",         zipf_theta=theta, num_native_txns=num_txn, perc_dist_txn=perc_dist_txn, read_perc=read_perc)
                        # R4
                        insert_job(partitions, "YCSB", "DOCC",       "RAFT_2PC", "READ_LEADER",         zipf_theta=theta, num_native_txns=num_txn, perc_dist_txn=perc_dist_txn, read_perc=read_perc)
                        # Sundial
                        insert_job(partitions, "YCSB", "SUNDIAL",    "RAFT_2PC", "READ_LEADER",         zipf_theta=theta, num_native_txns=num_txn, perc_dist_txn=perc_dist_txn, read_perc=read_perc)
                        # CC + R4
                        insert_job(partitions, "YCSB", "WOUND_WAIT", "RAFT_2PC", "READ_LEADER",         zipf_theta=theta, num_native_txns=num_txn, perc_dist_txn=perc_dist_txn, read_perc=read_perc)
                        insert_job(partitions, "YCSB", "WAIT_DIE",   "RAFT_2PC", "READ_LEADER",         zipf_theta=theta, num_native_txns=num_txn, perc_dist_txn=perc_dist_txn, read_perc=read_perc)
                        insert_job(partitions, "YCSB", "NO_WAIT",    "RAFT_2PC", "READ_LEADER",         zipf_theta=theta, num_native_txns=num_txn, perc_dist_txn=perc_dist_txn, read_perc=read_perc)
                        insert_job(partitions, "YCSB", "DOCC",       "RAFT_2PC", "READ_NEAREST",        zipf_theta=theta, num_native_txns=num_txn, perc_dist_txn=perc_dist_txn, read_perc=read_perc)
                        # Polaris
                        insert_job(partitions, "YCSB", "DOCC",       "RAFT_2PC", "READ_RESERVE_LEADER", zipf_theta=theta, num_native_txns=num_txn, perc_dist_txn=perc_dist_txn, read_perc=read_perc)
    run_all_test(jobs)
    # Rename the result file
    os.system(f'mv result.csv csv/ycsb-a-topology-{i}-new.csv')

    # # TPCC experiments
    # jobs.clear()
    # for c in range(1):
    #     for tpcc_num_wh in tpcc_num_wh_list:
    #         for num_txn in tpcc_num_txn_list:
    #             # Bonspiel
    #             for bonspiel_threshold in bonspiel_threshold_list:
    #                 insert_job(partitions, "TPCC", "DOCC", "RAFT_2PC", "BONSPIEL", num_wh=tpcc_num_wh, num_native_txns=num_txn, bonspiel_threshold=bonspiel_threshold, early_write_install=1, multiple_priority_levels=1, hotness_group_size=50)
    #                 # insert_job(partitions, "TPCC", "DOCC", "RAFT_2PC", "BONSPIEL", num_wh=tpcc_num_wh, num_native_txns=num_txn, bonspiel_threshold=bonspiel_threshold, early_write_install=1, multiple_priority_levels=1, bonspiel_no_reser=1, hotness_group_size=50)
    #                 # insert_job(partitions, "TPCC", "DOCC", "RAFT_2PC", "BONSPIEL", num_wh=tpcc_num_wh, num_native_txns=num_txn, bonspiel_threshold=bonspiel_threshold, early_write_install=1, multiple_priority_levels=1, bonspiel_no_ams=1, hotness_group_size=50)
    #             # Spanner
    #             insert_job(partitions, "TPCC", "NO_WAIT", "RAFT_2PC", "READ_LEADER", num_wh=tpcc_num_wh, num_native_txns=num_txn, opt_commit=0, opt_coord=0, opt_leader=0)
    #             # CRDB
    #             insert_job(partitions, "TPCC", "DOCC", "RAFT_2PC", "READ_LEADER", num_wh=tpcc_num_wh, num_native_txns=num_txn, opt_commit=0, opt_coord=1, opt_leader=0)
    #             # TAPIR
    #             insert_job(partitions, "TPCC", "DOCC", "TAPIR", "READ_LEADER", num_wh=tpcc_num_wh, num_native_txns=num_txn)
    #             # GPAC
    #             insert_job(partitions, "TPCC", "DOCC", "GPAC", "READ_LEADER", num_wh=tpcc_num_wh, num_native_txns=num_txn)
    #             # R4
    #             insert_job(partitions, "TPCC", "DOCC", "RAFT_2PC", "READ_LEADER", num_wh=tpcc_num_wh, num_native_txns=num_txn)
    #             # Sundial
    #             insert_job(partitions, "TPCC", "SUNDIAL", "RAFT_2PC", "READ_LEADER", num_wh=tpcc_num_wh, num_native_txns=num_txn)
    #             # CC + R4
    #             insert_job(partitions, "TPCC", "WOUND_WAIT", "RAFT_2PC", "READ_LEADER", num_wh=tpcc_num_wh, num_native_txns=num_txn)
    #             insert_job(partitions, "TPCC", "WAIT_DIE", "RAFT_2PC", "READ_LEADER", num_wh=tpcc_num_wh, num_native_txns=num_txn)
    #             insert_job(partitions, "TPCC", "NO_WAIT", "RAFT_2PC", "READ_LEADER", num_wh=tpcc_num_wh, num_native_txns=num_txn)
    #             insert_job(partitions, "TPCC", "DOCC", "RAFT_2PC", "READ_NEAREST", num_wh=tpcc_num_wh, num_native_txns=num_txn)
    #             # Polaris
    #             insert_job(partitions, "TPCC", "DOCC", "RAFT_2PC", "READ_RESERVE_LEADER", num_wh=tpcc_num_wh, num_native_txns=num_txn)
    # run_all_test(jobs)
    # # Rename the result file
    # os.system(f'mv result.csv csv/tpcc-topology-{i}-new.csv')
