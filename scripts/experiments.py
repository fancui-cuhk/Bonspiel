import numpy as np
import os, re, os.path, csv
import subprocess, datetime, time


def replace(filename, pattern, replacement):
	f = open(filename)
	s = f.read()
	f.close()
	s = re.sub(pattern, replacement, s)
	f = open(filename, 'w')
	f.write(s)
	f.close()


jobs = []
test_folder = "/home/fan.cui/geodb"
dev_folder = "/opt/nfs_dcc/fcui/GeoDBx1000"
dbms_cfg = [dev_folder + "/config-std.h", dev_folder + "/config.h"]
test_server = "ssd3.dbg.private"


def insert_job(partitions, workload, cc_alg, commit_protocol="RAFT_2PC", access_method="READ_LEADER", early_write_install=0,
			   hotness_threshold=100, bonspiel_threshold=0, bonspiel_no_reser=0, bonspiel_no_ams=0, heuristic_perc=0.2,
               multiple_priority_levels=0, abort_after_write_reservation_fail=0, priority_lock=0, zipf_theta=0.6, num_wh=32,
               perc_dist_txn=0.1, perc_remote_dist_txn=1, tpce_remote=10, core_cnt=12, read_perc=0.5, num_native_txns=1000,
               opt_commit=1, opt_coord=1, opt_leader=1, hotness_group_size=1000):
	jobs.append({
		"PARTITIONS"							: partitions,
		"WORKLOAD"								: workload,
		"CC_ALG"								: cc_alg,
		"COMMIT_PROTOCOL"						: commit_protocol,
		"ACCESS_METHOD"							: access_method,
		"EARLY_WRITE_INSTALL"					: early_write_install,
		"HOTNESS_THRESHOLD"						: hotness_threshold,
		"BONSPIEL_THRESHOLD"					: bonspiel_threshold,
        "BONSPIEL_NO_RESER"                     : bonspiel_no_reser,
        "BONSPIEL_NO_AMS"                       : bonspiel_no_ams,
		"HEURISTIC_PERC"						: heuristic_perc,
        "MULTIPLE_PRIORITY_LEVELS"              : multiple_priority_levels,
		"ABORT_AFTER_WRITE_RESERVATION_FAIL"	: abort_after_write_reservation_fail,
		"PRIORITY_LOCK"							: priority_lock,
		"ZIPF_THETA"							: zipf_theta,				# YCSB
		"NUM_WH"								: num_wh,					# TPCC
		"PERC_DIST_TXN"							: perc_dist_txn,
		"PERC_REMOTE_DIST_TXN"					: perc_remote_dist_txn,
		"TPCE_PERC_REMOTE"						: tpce_remote,
		"NUM_SERVER_THREADS"					: core_cnt,
		"READ_PERC"								: read_perc,
		"MAX_NUM_NATIVE_TXNS"					: num_native_txns,
		"BYPASS_COMMIT_CONSENSUS"				: opt_commit,
		"BYPASS_COORD_CONSENSUS"				: opt_coord,
		"BYPASS_LEADER_DECIDE"					: opt_leader,
		"HOTNESS_GROUP_SIZE"					: hotness_group_size,
	})


def test_compile(job):
	os.system("cp " + dbms_cfg[0] + ' ' + dbms_cfg[1])
	for (param, value) in job.items():
		pattern = r"\#define\s+" + re.escape(param) + r'\s+.*'
		replacement = "#define " + param + ' ' + str(value)
		replace(dbms_cfg[1], pattern, replacement)
	os.system("make clean > temp.out 2>&1")
	ret = os.system("make -j > temp.out 2>&1")
	if ret != 0:
		print("ERROR in compiling job=")
		print(job)
		exit(0)
	print(f"PASS Compile\tCC={job['CC_ALG']}\tworkload={job['WORKLOAD']}")


def test_run(job, log_dir):
	print(job)
	partitions = job["PARTITIONS"]
	all_nodes = [n for p in partitions for n in p]

	print("creating ifconfig.txt...")
	while True:
		try:
			with open("ifconfig.txt", 'w') as ifconfig_file:
				for partition in partitions:
					ifconfig = "=P\n" + "\n".join(partition) + "\n"
					ifconfig_file.write(ifconfig)
			break
		except BlockingIOError:
			print("ifconfig.txt busy")
			time.sleep(1)

	log_file_names = [f"{job['CC_ALG']}_{job['COMMIT_PROTOCOL']}_{job['WORKLOAD']}_{datetime.datetime.now().strftime('%m_%d_%H_%M_%S')}_{i}.log" for i in range(len(all_nodes))]
	log_paths = [os.path.join(log_dir, log_file_name) for log_file_name in log_file_names]
	leader_log_paths = []
	idx = 0
	for partition in partitions:
		leader_log_paths.append(log_paths[idx])
		idx += len(partition)

	print("log files:\n" + "\n".join(log_paths))

	print("clearing process...")
	clear_cmds = [f"ssh {node} 'pkill -9 rundb; pkill -f rundb'" for node in all_nodes]
	processes = [subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True) for cmd in clear_cmds]

	for process in processes:
		while process.poll() is None:
			time.sleep(1)

	print("copying file...")
	cmds = [f"scp {dev_folder}/rundb {node}:{test_folder}; scp {dev_folder}/ifconfig.txt {node}:{test_folder}" for node in all_nodes if node != test_server]
	processes = [subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True) for cmd in cmds]

	for process in processes:
		while process.poll() is None:
			time.sleep(1)

	# Only leaders write logs
	cmds = [f"ssh {node} 'cd {test_folder}; ./rundb &> {log_path}'" for node, log_path in zip([partition[0] for partition in partitions], leader_log_paths)]
	cmds += [f"ssh {node} 'cd {test_folder}; ./rundb &> /dev/null'" for node in set(all_nodes).difference(set([partition[0] for partition in partitions]))]

	# All nodes write logs
	# cmds = [f"ssh {node} 'cd {test_folder}; ./rundb &> {log_path}'" for node, log_path in zip(all_nodes, log_paths)]

	print("cmds:\n" + "\n".join(cmds))
	processes = [subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True) for cmd in cmds]
	start = datetime.datetime.now()

	timeout = 500

	for process in processes:
		while process.poll() is None:
			time.sleep(1)
			now = datetime.datetime.now()
			if (now - start).seconds > timeout:
				os.waitpid(-1, os.WNOHANG)
				print("ERROR. Timeout")
				return False

	results = []
	for log_path in leader_log_paths:
		with open(log_path) as log_file:
			process_stdout = log_file.read()
			if "PASS" not in process_stdout:
				print(f"FAILED execution. {job}")
				print(f"log file: {log_path}")
				return False
			else:
				results.append(parse_result(process_stdout))

	merged_result = merge_multi_node_results(results)
	write_result("result.csv", job, merged_result)
	return True


metrics = ["Throughput", "num_commits", "num_aborts", "num_home_sp_commits", "num_home_mp_commits",
		   "sp_execute_phase", "sp_prepare_phase", "sp_commit_phase", "sp_average_latency",
		   "mp_execute_phase", "mp_prepare_phase", "mp_commit_phase", "mp_average_latency",
		   "sp_abort_rate", "mp_abort_rate", "p50_latency", "p90_latency", "p95_latency",
		   "p99_latency", "p999_latency", "p9999_latency", "sp_p50_latency", "sp_p90_latency",
		   "sp_p95_latency", "sp_p99_latency", "sp_p999_latency", "sp_p9999_latency", "mp_p50_latency",
		   "mp_p90_latency", "mp_p95_latency", "mp_p99_latency", "mp_p999_latency", "mp_p9999_latency",
		   "dist_txn_local_abort", "dist_txn_2pc_abort", "remote_txn_lock_write_set_abort",
		   "remote_txn_validation_abort", "remote_txn_val_rd_abort", "remote_txn_val_wr_abort",
		   "dist_txn_abort_local", "dist_txn_abort_dist", "local_txn_abort_local", "local_txn_abort_dist",
		   "nearest_access", "leader_access", "leader_reserve", "p999_exec_latency", "p999_commit_latency",
		   "p999_abort_latency", "p999_net_latency"]


def parse_result(process_stdout):
	result = {}
	for metric in metrics:
		if metric == "p999_exec_latency" or metric == "p999_commit_latency" or metric == "p999_abort_latency" or metric == "p999_net_latency":
			continue
		match = re.search(f"\s*{metric}\s*:\s*(\d+.?\d*e?[\+-]?\d*)(\s|\n)", process_stdout)
		if match:
			result[metric] = float(match.group(1))
		else:
			match = re.search(f"\s*{metric} \(in us\)\s*:\s*(\d+.?\d*e?[\+-]?\d*)(\s|\n)", process_stdout)
			if match:
				result[metric] = float(match.group(1))
			else:
				# Not found in result
				if 'sp_' in metric or 'mp_' in metric:
					result[metric] = float(0)
	latency_match = re.search(
        r"p999_latency:\s*([\d.]+)\s*abort count:\s*\d+\s*\(exec, prep, commit, abort, net\): \(([\d.]+),\s*([\d.]+),\s*([\d.]+),\s*([\d.]+),\s*([\d.]+)\)",
        process_stdout
    )
	if latency_match:
		result["p999_exec_latency"] = float(latency_match.group(2))
		result["p999_commit_latency"] = float(latency_match.group(3)) + float(latency_match.group(4))
		result["p999_abort_latency"] = float(latency_match.group(5))
		result["p999_net_latency"] = float(latency_match.group(6))
	return result


def merge_multi_node_results(results):
	metrics = [("Throughput", np.sum), ("num_commits", np.sum), ("num_aborts", np.sum), ("num_home_sp_commits", np.sum), ("num_home_mp_commits", np.sum),
	    	   ("sp_execute_phase", np.mean), ("sp_prepare_phase", np.mean), ("sp_commit_phase", np.mean), ("sp_average_latency", np.mean),
			   ("mp_execute_phase", np.mean), ("mp_prepare_phase", np.mean), ("mp_commit_phase", np.mean), ("mp_average_latency", np.mean),
			   ("sp_abort_rate", np.mean), ("mp_abort_rate", np.mean), ("p50_latency", np.mean), ("p90_latency", np.mean), ("p95_latency", np.mean),
			   ("p99_latency", np.mean), ("p999_latency", np.mean), ("p9999_latency", np.mean), ("sp_p50_latency", np.mean), ("sp_p90_latency", np.mean),
			   ("sp_p95_latency", np.mean), ("sp_p99_latency", np.mean), ("sp_p999_latency", np.mean), ("sp_p9999_latency", np.mean),
			   ("mp_p50_latency", np.mean), ("mp_p90_latency", np.mean), ("mp_p95_latency", np.mean), ("mp_p99_latency", np.mean), ("mp_p999_latency", np.mean),
			   ("mp_p9999_latency", np.mean), ("dist_txn_local_abort", np.sum), ("dist_txn_2pc_abort", np.sum), ("remote_txn_lock_write_set_abort", np.sum),
			   ("remote_txn_validation_abort", np.sum), ("remote_txn_val_rd_abort", np.sum), ("remote_txn_val_wr_abort", np.sum),
			   ("dist_txn_abort_local", np.sum), ("dist_txn_abort_dist", np.sum), ("local_txn_abort_local", np.sum), ("local_txn_abort_dist", np.sum),
			   ("nearest_access", np.sum), ("leader_access", np.sum), ("leader_reserve", np.sum), ("p999_exec_latency", np.mean), ("p999_commit_latency", np.mean),
			   ("p999_abort_latency", np.mean), ("p999_net_latency", np.mean)]
	merged_result = {}
	for metric, func in metrics:
		metric_results = [result[metric] for result in results]
		merged_result[metric] = func(metric_results)
	return merged_result


def write_result(file_name, job, result):
	fields = ["COMMIT_PROTOCOL", "BYPASS_COMMIT_CONSENSUS", "BYPASS_COORD_CONSENSUS", "BYPASS_LEADER_DECIDE", # commit protocol setting
			  "CC_ALG", "ACCESS_METHOD", "EARLY_WRITE_INSTALL", "PRIORITY_LOCK", "MULTIPLE_PRIORITY_LEVELS",  # CC setting
			#   "ABORT_AFTER_WRITE_RESERVATION_FAIL",														  # CC setting
			  "HOTNESS_THRESHOLD", "BONSPIEL_THRESHOLD", "BONSPIEL_NO_RESER", "BONSPIEL_NO_AMS",              # CC setting
              "HEURISTIC_PERC", 									                                          # CC setting
		   	  "WORKLOAD", "NUM_PARTS", "NUM_SERVER_THREADS", "MAX_NUM_NATIVE_TXNS",							  # system setting
			  "NUM_WH",																						  # TPCC setting
		   	#   "TPCE_PERC_REMOTE",																			  # TPCE setting
		      "ZIPF_THETA", "READ_PERC", "PERC_DIST_TXN", "PERC_REMOTE_DIST_TXN",							  # YCSB setting
		   	  ] + metrics
	file_exists = os.path.isfile(file_name)
	csv_result = {**job, **result}
	csv_result["NUM_PARTS"] = len(csv_result["PARTITIONS"])
	with open(file_name, "a") as result_file:
		writer = csv.DictWriter(result_file, delimiter=',', lineterminator='\n', fieldnames=fields, extrasaction='ignore')
		if not file_exists:
			writer.writeheader()
		writer.writerow(csv_result)


def run_all_test(jobs):
	for i, job in enumerate(jobs):
		print(f"\nJob {i+1}/{len(jobs)}")
		num_retry = 0
		test_compile(job)
		while num_retry < 10:
			if num_retry != 0:
				print(f"\nRetrying...")
			num_retry += 1
			try:
				if test_run(job, '/opt/nfs_dcc/fcui/GeoDBx1000/log'):
					break
			except Exception as ex:
				print(ex)
		if num_retry >= 10:
			print("ERROR!!")
			return
	jobs = []
