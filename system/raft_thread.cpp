#include "raft_thread.h"
#include "helper.h"
#include "manager.h"
#include "transport.h"
#include "message.h"
#include "packetize.h"
#include "cc_manager.h"

void
raft_append(LogRecord *record)
{
	uint32_t queue_id = glob_manager->txnid_to_server_thread(record->txn_id) % NUM_RAFT_INSTANCES;
	raft_log_queues[queue_id]->push(record);
}

RaftThread::RaftThread(uint64_t thd_id, uint64_t ins_id) : Thread(thd_id, RAFT_THREAD)
{
	_stop_signal = false;
	_ins_id = ins_id;
	raft_cbs_t raft_callbacks = {
		.send_requestvote = cb_send_requestvote,
		.send_appendentries = cb_send_appendentries,
		.applylog = cb_apply_log,
		.persist_vote = cb_persist_vote,
		.persist_term = cb_persist_term,
		.log_offer = cb_log_offer,
		.log_poll = cb_log_poll,
		.log_pop = cb_log_pop,
		.log_clear = cb_log_clear,
		.log_get_node_id = cb_log_get_node_id,
		.node_has_sufficient_logs = cb_node_has_sufficient_logs,
		.log = NULL,
	};

	_raft_server = raft_new();
	raft_set_callbacks(_raft_server, &raft_callbacks, this);
	srand(thd_id);
	raft_add_node(_raft_server, this, g_node_id, true);
	if (g_role == ROLE_LEADER)
		raft_become_leader(_raft_server);
	vector<uint32_t> &cluster_nodes = transport->get_partition_nodes(g_part_id);
	for (uint32_t node_id : cluster_nodes) {
		if (node_id == g_node_id) continue;
		raft_add_node(_raft_server, this, node_id, false);
	}
}

void
RaftThread::signal_to_stop()
{
	_stop_signal = true;
}

RC
RaftThread::run()
{
    glob_manager->init_rand( get_thd_id() );
    glob_manager->set_thd_id( get_thd_id() );
    assert( glob_manager->get_thd_id() == get_thd_id() );
#if SET_AFFINITY
    pin_thread_by_id(pthread_self(), _thd_id);
#endif
    pthread_barrier_wait( &global_barrier );

	if (!LOG_ENABLE)
		return RCOK;

	//todo: determine we are recovering or starting, only have starting logic here
	uint32_t	 record_per_loop = RAFT_WORK_PER_LOOP;
	uint32_t     msg_per_loop = record_per_loop * (transport->get_partition_nodes(g_part_id).size() - 1);
	uint64_t 	 last_periodic = get_sys_clock();
	LogQueue *   log_queue = raft_log_queues[_ins_id];
	InOutQueue * msg_queue = raft_msg_queues[_ins_id];

	Message * msg = NULL;
	LogRecord * record = NULL;
	while (!_stop_signal) {
		uint64_t t0 = get_sys_clock();
		uint64_t periodic_elapsed = (t0 - last_periodic) / MILLION;
		if (periodic_elapsed > RAFT_PERIODIC) {
			raft_periodic(_raft_server, periodic_elapsed);
			last_periodic = t0;
		}
		uint64_t t1 = get_sys_clock();
		INC_FLOAT_STATS(raft_time_periodic, t1 - t0);

		raft_apply_all(_raft_server);
		uint64_t t2 = get_sys_clock();
		INC_FLOAT_STATS(raft_time_apply, t2 - t1);

		int err = 0;

		uint32_t log_count = 0;
		while (log_count < record_per_loop && log_queue->pop(record)) {
			log_count += 1;
			msg_entry_t entry;
			entry.id = _id_counter++;
			entry.data.len = record->size;
			entry.data.buf = record;
			entry.type = RAFT_LOGTYPE_NORMAL;
			msg_entry_response_t resp;
			err = raft_recv_entry(_raft_server, &entry, &resp);
			assert(err == 0);  //todo: error handling
		}
		uint64_t t3 = get_sys_clock();
		INC_FLOAT_STATS(raft_time_append, t3 - t2);

		uint32_t msg_count = 0;
		while (msg_count < msg_per_loop && msg_queue->pop(msg)) {
			// todo: node leave logic
			// todo: optimize get node from idx
			msg_count += 1;
			raft_node_t *raft_src_node = raft_get_node(_raft_server, msg->get_src_node_id());
			if (msg->get_type() == Message::RAFT_REQUESTVOTE) {
				assert(msg->get_data_size() == sizeof(msg_requestvote_t));
				msg_requestvote_t *raft_msg = (msg_requestvote_t *)msg->get_data();
				msg_requestvote_response_t raft_resp;
				err = raft_recv_requestvote(_raft_server, raft_src_node, raft_msg, &raft_resp);
				assert(err == 0); // todo: error handling
				Message resp_msg(Message::RAFT_REQUESTVOTE_RESP, msg->get_src_node_id(), _ins_id, sizeof(raft_resp), (char*)&raft_resp);	
				transport->send_msg(&resp_msg);
			} else if (msg->get_type() == Message::RAFT_REQUESTVOTE_RESP) {
				assert(msg->get_data_size() == sizeof(msg_requestvote_response_t));
				msg_requestvote_response_t *raft_msg = (msg_requestvote_response_t *)msg->get_data();
				err = raft_recv_requestvote_response(_raft_server, raft_src_node, raft_msg);
				assert(err == 0); // todo: error handling
			} else if (msg->get_type() == Message::RAFT_APPEND_ENTRIES) {
				// format: | term | prev_log_idx | prev_log_term | leader_commit | n_entries | entries...
				// entries: | id | type | size | data
				UnstructuredBuffer buffer(msg->get_data());
				msg_appendentries_t raft_msg;
				buffer.get(&raft_msg.term);
				buffer.get(&raft_msg.prev_log_idx);
				buffer.get(&raft_msg.prev_log_term);
				buffer.get(&raft_msg.leader_commit);
				buffer.get(&raft_msg.n_entries);
				raft_msg.entries = new msg_entry_t[raft_msg.n_entries];
				for (int i = 0; i < raft_msg.n_entries; i++) {
					msg_entry_t &entry = raft_msg.entries[i];
					raft_msg.entries[i].term = raft_msg.term;
					buffer.get(&entry.id);
					buffer.get(&entry.type);
					buffer.get(&entry.data.len);
					entry.data.buf = new unsigned int[entry.data.len];
					char * data = NULL;
					buffer.get(data, entry.data.len);
					memcpy(entry.data.buf, data, entry.data.len);
				}
				msg_appendentries_response_t raft_resp;
				err = raft_recv_appendentries(_raft_server, raft_src_node, &raft_msg, &raft_resp);
				Message resp_msg(Message::RAFT_APPEND_ENTRIES_RESP, msg->get_src_node_id(), _ins_id, sizeof(raft_resp), (char*)&raft_resp);
				transport->send_msg(&resp_msg);
			#if COMMIT_PROTOCOL == RAFT_2PC && BYPASS_LEADER_DECIDE && CC_ALG != CALVIN
				for (int i = 0; i < raft_msg.n_entries; i++) {
					msg_entry_t &entry = raft_msg.entries[i];
					if (entry.type == RAFT_LOGTYPE_NORMAL) {
						LogRecord * record = (LogRecord *)entry.data.buf;
						assert(record->size == entry.data.len);
						if (record->type == LOG_PREPARE) {
							UnstructuredBuffer buffer(record->data);
							// parse log to get the coordinator and participants
						#if CC_ALG == DOCC || CC_ALG == SUNDIAL
							// For DOCC, the first entry in the log is _min_commit_ts
							buffer.set_offset(sizeof(uint64_t)); // be careful here
						#endif
							uint32_t coord_part;
							buffer.get(&coord_part);
							if (coord_part != g_part_id) {
								Message bypassed_msg(Message::APPEND_ENTRIES_RESP, transport->get_partition_leader(coord_part), record->txn_id, 0, NULL);
								transport->send_msg(&bypassed_msg);
							}
						#if BYPASS_COORD_CONSENSUS
        					uint32_t num_parts;
        					buffer.get( &num_parts );
        					assert(num_parts >= 1);
        					for (uint32_t i = 0; i < num_parts; i++) {
        					    uint32_t part_id;
        					    buffer.get( &part_id );
								if (part_id != g_part_id && part_id != coord_part) {
									Message bypassed_msg(Message::APPEND_ENTRIES_RESP, transport->get_partition_leader(part_id), record->txn_id, 0, NULL);
									transport->send_msg(&bypassed_msg);
								}
        					}
						#endif
						}
					}
				}
			#endif
				assert(err == 0); // todo: err handling
			} else if (msg->get_type() == Message::RAFT_APPEND_ENTRIES_RESP) {
				assert(msg->get_data_size() == sizeof(msg_appendentries_response_t));
				msg_appendentries_response_t * raft_msg = (msg_appendentries_response_t *) msg->get_data();
				err = raft_recv_appendentries_response(_raft_server, raft_src_node, raft_msg);
			} else {
				assert(false);
			}
			cleanup_msg(msg);
		}
		INC_FLOAT_STATS(raft_time_msg, get_sys_clock() - t3);
	}
	raft_apply_all(_raft_server);

	return RCOK;
}

void
RaftThread::cleanup_msg(Message *msg)
{
	if (msg->get_data_size() > 0)
		delete[] msg->get_data();
	delete msg;
}

bool
RaftThread::append_config_change(raft_logtype_e type, uint32_t node_id) {
	msg_entry_t entry;
	entry.id = _id_counter++;
	entry.data.len = sizeof(uint32_t);
	entry.data.buf = new char[sizeof(uint32_t)];
	*(uint32_t*) entry.data.buf = node_id;
	entry.type = type;
	msg_entry_response_t resp;
	if (raft_recv_entry(_raft_server, &entry, &resp) == 0)
		return true;
	return false;
}

int
RaftThread::cb_send_requestvote(raft_server_t *raft, void *user_data, raft_node_t *node, msg_requestvote_t *raft_msg)
{
	//todo: check connection and reconnect
	RaftThread * me = (RaftThread*) user_data;
	Message msg(Message::RAFT_REQUESTVOTE, raft_node_get_id(node), me->_ins_id, sizeof(*raft_msg), (char*)raft_msg);
	INC_MSG_STATS(Message::RAFT_REQUESTVOTE, sizeof(*raft_msg));
	transport->send_msg(&msg);
	return 0;
}

int
RaftThread::cb_send_appendentries(raft_server_t *raft, void *user_data, raft_node_t *node, msg_appendentries_t *m)
{
	// format: | term | prev_log_idx | prev_log_term | leader_commit | n_entries | entries...
	// entries: | id | type | size | data
	UnstructuredBuffer buffer;
	buffer.put(&m->term);
	buffer.put(&m->prev_log_idx);
	buffer.put(&m->prev_log_term);
	buffer.put(&m->leader_commit);
	buffer.put(&m->n_entries);

	for (int i = 0; i < m->n_entries; i++) {
		msg_entry_t &entry = m->entries[i];
		assert(entry.term == m->term);
		buffer.put(&entry.id);
		buffer.put(&entry.type);
		buffer.put(&entry.data.len);
		buffer.put(entry.data.buf, entry.data.len);
	}

	RaftThread * me = (RaftThread*) user_data;
	Message msg(Message::RAFT_APPEND_ENTRIES, raft_node_get_id(node), me->_ins_id, buffer.size(), buffer.data());
	transport->send_msg(&msg);
	return 0;
}

int
RaftThread::cb_apply_log(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t idx)
{
	if (raft_entry_is_cfg_change(entry)) {
		// todo: handle node leave here
		// uint32_t node_id = *(uint32_t*) entry->data;
		return 0;
	}
	LogRecord * record = (LogRecord *) entry->data.buf;
	assert(record->size == entry->data.len);
#if CC_ALG == CALVIN
	// only one log queue in calvin
	uint32_t queue_id = 0;
#else
	uint32_t queue_id = glob_manager->txnid_to_server_thread(record->txn_id);
#endif
	log_apply_queues[queue_id]->push(record);
	INC_FLOAT_STATS(log_size, entry->data.len);

	return 0;
}

int
RaftThread::cb_persist_vote(raft_server_t *raft, void *user_data, const int voted_for)
{
	// todo: write to log
	return 0;
}

int
RaftThread::cb_persist_term(raft_server_t *raft, void *user_data, raft_term_t current_term, raft_node_id_t vote)
{
	// todo: write to log
	return 0;
}

int
RaftThread::cb_log_offer(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx)
{
	/*
	RaftThread *me = (RaftThread *) user_data;
	if (raft_entry_is_cfg_change(entry)) 
		me->offer_config_change((raft_logtype_e) entry->type, *(uint32_t*)entry->data.buf);
	*/
	// todo: write to log
	return 0;
}

int
RaftThread::cb_log_poll(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx)
{
	// todo: delete entry in log
	assert(false);
	if (entry->data.len > 0)
		delete[] (char*)entry->data.buf;

	return 0;
}

int
RaftThread::cb_log_pop(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx)
{
	// todo: pop the last entry in log
	assert(false);
	if (entry->data.len > 0)
		delete[] (char*)entry->data.buf;
		
	return 0;
}
int
RaftThread::cb_log_clear(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx)
{
	assert(false);
	if (entry->data.len > 0)
		delete[] (char*)entry->data.buf;
	return 0;
}

int
RaftThread::cb_log_get_node_id(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx)
{
	assert(raft_entry_is_cfg_change(entry) && entry->data.len == sizeof(uint32_t));
	return *(uint32_t*) entry->data.buf;
}

int
RaftThread::cb_node_has_sufficient_logs(raft_server_t *raft, void *user_data, raft_node_t *node)
{
	RaftThread * me = (RaftThread*) user_data;
	assert(me->append_config_change(RAFT_LOGTYPE_ADD_NODE, raft_node_get_id(node)));
	return 0;
}

void
RaftThread::cb_log(raft_server_t *raft, raft_node_t *node, void *user_data, const char *buf)
{
	cout << buf << endl;
	//todo: disply debug info
}