#pragma once

#include "workload.h"
#include "txn.h"
#include "global.h"
#include "helper.h"

class QueryBank;
class SubQueryBank;
class StoreProcedure;

class WorkloadBank : public workload {
public :
    RC init();
    StoreProcedure * create_store_procedure(TxnManager * txn, QueryBase * query);
    QueryBase * gen_query(int is_mp = 0);
    QueryBase * clone_query(QueryBase * query);
    QueryBase * deserialize_subquery(char * data);

    uint64_t     get_primary_key(row_t * row);
    uint64_t     get_index_key(row_t * row, uint32_t index_id)
    { return get_primary_key(row); }
    INDEX *     get_index(uint32_t index_id) { return indexes[index_id]; }
    table_t *     get_table(uint32_t table_id) { return tables[table_id]; };
    void         table_to_indexes(uint32_t table_id, set<INDEX *> * indexes);
    INDEX *     get_primary_index(uint32_t table_id) { return indexes[table_id]; }

    uint32_t     key_to_part(uint64_t key, uint32_t table_id = 0) { return key % g_num_parts; }
	void		 verify_integrity();
private:
    void init_table_parallel();
    void * init_table_slice();
    static void * threadInitTable(void * This) {
        ((WorkloadBank *)This)->init_table_slice();
        return NULL;
    }
    //  For parallel initialization
    static int next_tid;
};

