import subprocess

# Network topology for experiments (For now we only consider full-replication cases)
# n_regions: number of replicas
# n_nodes_per_region: number of shards
# latency_matrix: network latencies between different replicas/regions
def network_topology(n_regions, n_nodes_per_region, latency_matrix):
	# Nodes used in the experiments start from ssd4
	base_node=4
	wan_bandwidth = 1
	region_nodes = []
	# Starting from base_node, construct region_nodes
	for region in range(n_regions):
		# ssd21 - ssd23 are used as proxies
		nodes = [f"172.20.10.{base_node + i}" if base_node + i <= 20 else f"172.20.10.{base_node + i + 3}"
	   			 for i in range(n_nodes_per_region * region, n_nodes_per_region * (region + 1))]
		# ssd15 is down
		nodes = ["172.20.10.2" if n == "172.20.10.15" else n for n in nodes]
		region_nodes.append(nodes)

	print(region_nodes)

	for src_region in range(n_regions):
		src_nodes = region_nodes[src_region]
		for src in src_nodes:
			print(src)
			subprocess.run(f"ssh {src} 'sudo tc qdisc del dev vmbr0 root'", shell=True)
			# Performance tuning for the TCP socket on the source node
			cmd = 'sudo sysctl -w net.ipv4.tcp_rmem="4096 32768 629145600";'
			cmd += 'sudo sysctl -w net.ipv4.tcp_wmem="4096 32768 629145600";'
			cmd += 'sudo sysctl -w net.ipv4.tcp_adv_win_scale=-2;'
			cmd += 'sudo sysctl -w net.ipv4.tcp_notsent_lowat=32768;'

			cmd += f"sudo tc qdisc add dev vmbr0 root handle 1: prio bands {n_regions} priomap " + f"{src_region} " * 16 + ";"
			for dest_region in range(n_regions):
				if dest_region == src_region:
					continue
				# Divide the latency by 2 to get the one-way latency between two replicas
				cmd += f"sudo tc qdisc add dev vmbr0 parent 1:{dest_region+1} handle {dest_region+1}0: netem delay {latency_matrix[src_region][dest_region]/2}ms rate {wan_bandwidth}Gbit;"
				dest_nodes = region_nodes[dest_region]
				for dest in dest_nodes:
					cmd += f"sudo tc filter add dev vmbr0 protocol ip parent 1:0 prio 1 u32 match ip dst {dest} classid 1:{dest_region+1}; "
			subprocess.run(f"ssh {src} '{cmd}'", shell=True)
