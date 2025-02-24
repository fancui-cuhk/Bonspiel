#pragma once

#include "global.h"
#include "thread.h"
#include "message.h"
#include "log.h"

extern "C" {
	#include "raft.h"
}
void raft_append(LogRecord * record);

class RaftThread : public Thread
{
public:
	RaftThread(uint64_t thd_id, uint64_t ins_id);
	RC run();
	void signal_to_stop();

private:
	bool 		   _stop_signal;
	raft_server_t* _raft_server;
	uint64_t	   _id_counter;
	uint64_t	   _ins_id;

	static int cb_send_requestvote(raft_server_t *raft, void *user_data, raft_node_t *node, msg_requestvote_t *msg);
	static int cb_send_appendentries(raft_server_t *raft, void *user_data, raft_node_t *node, msg_appendentries_t *msg);
	static int cb_apply_log(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t idx);
	static int cb_persist_vote(raft_server_t *raft, void *user_data, const int voted_for);
	static int cb_persist_term(raft_server_t *raft, void *user_data, raft_term_t current_term, raft_node_id_t vote);
	static int cb_log_offer(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx);
	static int cb_log_poll(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx);
	static int cb_log_pop(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx);
	static int cb_log_clear(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx);
	static int cb_log_get_node_id(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx);

	static int cb_node_has_sufficient_logs(raft_server_t *raft, void *user_data, raft_node_t *node);
	static void cb_log(raft_server_t *raft, raft_node_t *node, void *user_data, const char *buf);

	bool append_config_change(raft_logtype_e type, uint32_t node_id);
	void cleanup_msg(Message *msg);
};