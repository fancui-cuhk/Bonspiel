#include <sched.h>
#include "global.h"
#include "helper.h"
#include "workload.h"
#include "server_thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "query.h"

#include "bank.h"
#include "bank_query.h"
#include "bank_store_procedure.h"

#if WORKLOAD == BANK

int WorkloadBank::next_tid;

RC
WorkloadBank::init() {
	workload::init();
    next_tid = 0;
    char * cpath = getenv("GRAPHITE_HOME");
    string path;
    if (cpath == NULL)
        path = "./benchmarks/bank_schema.txt";
    else {
        path = string(cpath);
        path += "/tests/apps/dbms/bank_schema.txt";
    }
    init_schema( path );
    init_table_parallel();
    return RCOK;
}

void
WorkloadBank::init_table_parallel() {
    enable_thread_mem_pool = true;
    pthread_t p_thds[g_init_parallelism - 1];
    for (uint32_t i = 0; i < g_init_parallelism - 1; i++)
        pthread_create(&p_thds[i], NULL, threadInitTable, this);
    threadInitTable(this);

    for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
        int rc = pthread_join(p_thds[i], NULL);
        if (rc) {
            printf("ERROR; return code from pthread_join() is %d\n", rc);
            exit(-1);
        }
    }
    enable_thread_mem_pool = false;
}

void *
WorkloadBank::init_table_slice() {
	uint32_t tid = ATOM_FETCH_ADD(next_tid, 1);
	RC rc;
	assert(tid < g_init_parallelism);
#if SET_AFFINITY
    pin_thd_to_numa_node(pthread_self(), tid % g_num_server_threads / get_cpus_per_node());
#endif
    uint64_t start = tid * BANK_NUM_KEYS / g_init_parallelism;
    uint64_t end = (tid + 1) * BANK_NUM_KEYS / g_init_parallelism;
    for (uint64_t key = start; key < end; key ++)
    {
        row_t *new_row_account1, *new_row_account2;
        int part_id = key_to_part(key);
        rc = tables[0]->get_new_row(new_row_account1, part_id);
        assert(rc == RCOK);
        rc = tables[1]->get_new_row(new_row_account2, part_id);
        assert(rc == RCOK);
        // LSBs of a key indicate the node ID
        uint64_t primary_key = key * g_num_parts + g_part_id;
		uint64_t init_balance = INIT_BALANCE / 2;
        new_row_account1->set_value(0, &primary_key);
        new_row_account1->set_value(1, &init_balance);
        new_row_account2->set_value(0, &primary_key);
        new_row_account2->set_value(1, &init_balance);

        uint64_t idx_key = primary_key;
        rc = indexes[0]->insert(idx_key, new_row_account1);
        assert(rc == RCOK);
        rc = indexes[1]->insert(idx_key, new_row_account2);
        assert(rc == RCOK);
    }
	return NULL;
}

StoreProcedure *
WorkloadBank::create_store_procedure(TxnManager * txn, QueryBase * query)
{
    return new BankStoreProcedure(txn, query);
}

QueryBase *
WorkloadBank::gen_query(int is_mp)
{
    QueryBase * query = new QueryBank(is_mp);
    return query;
}

QueryBase *
WorkloadBank::clone_query(QueryBase * query)
{
    QueryBank * q = (QueryBank *) query;
    QueryBank * new_q = new QueryBank(*q);
    return new_q;
}

QueryBase *
WorkloadBank::deserialize_subquery(char * data)
{
    QueryBank * query = new QueryBank(data);
    return query;
}

void
WorkloadBank::table_to_indexes(uint32_t table_id, set<INDEX *> * indexes)
{
	assert(table_id <= 1);
    indexes->insert(this->indexes[table_id]);
}

uint64_t
WorkloadBank::get_primary_key(row_t * row)
{
    uint64_t key;
    row->get_value(0, &key);
    return key;
}

void
WorkloadBank::verify_integrity()
{
	/* do not acquire any lock here */
	int64_t sum = 0;
	for (int64_t local_key = 0; local_key < BANK_NUM_KEYS; local_key++) {
		int64_t global_key = local_key * g_num_parts + g_part_id;

		set<row_t *> *rows = indexes[0]->read(global_key);
		assert(!rows->empty());
		row_t * curr_row = *rows->begin();
		char * curr_data = curr_row->get_data();
		int64_t bal1 = *(int64_t *)row_t::get_value(tables[0]->get_schema(), 1, curr_data);
		rows = indexes[1]->read(global_key);
		assert(!rows->empty());
		curr_row = *rows->begin();
		curr_data = curr_row->get_data();
		int64_t bal2 = *(int64_t *)row_t::get_value(tables[1]->get_schema(), 1, curr_data);

        printf("%ld: %ld(%ld %ld) \n", global_key, bal1 + bal2, bal1, bal2);
		sum += bal1 + bal2;
	}
    #if DEBUG_TXN_RESULT
	    for (int64_t key = 0; key < BANK_NUM_KEYS * g_num_parts; key++) {
            int64_t expected = 0;
            for (auto sub_bal : balances)
                expected += sub_bal[key];
            printf("%ld expected: %ld\n", key, expected);
        }
    #endif
	printf("sum: %lu\n", sum);
}
#endif