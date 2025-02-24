#include "bank_store_procedure.h"
#include "bank.h"
#include "bank_query.h"
#include "manager.h"
#include "cc_manager.h"
#include "row.h"
#include "table.h"
#include "catalog.h"
#include "index_base.h"
#include "index_hash.h"

#if WORKLOAD == BANK


BankStoreProcedure::BankStoreProcedure(TxnManager * txn_man, QueryBase * query)
	: StoreProcedure(txn_man, query)
{
}

RC
BankStoreProcedure::execute()
{
	RC rc = RCOK;
	WorkloadBank * wl = (WorkloadBank *) GET_WORKLOAD;
	QueryBank * query = (QueryBank *) _query;
	assert(_query);
	uint32_t wr_table_id = (query->to_key + query->from_key) % 2;
	uint32_t rd_table_id = 1 - wr_table_id;
	Catalog * schema_wr = wl->tables[wr_table_id]->get_schema();
	Catalog * schema_rd = wl->tables[rd_table_id]->get_schema();

	if (_phase == 0) {
		_phase = 1;
		get_cc_manager()->row_request(wr_table_id, wr_table_id, query->to_key, RD);
		get_cc_manager()->row_request(rd_table_id, rd_table_id, query->from_key, RD);
		get_cc_manager()->row_request(wr_table_id, wr_table_id, query->from_key, RD);
		rc = get_cc_manager()->process_requests();
		if (rc != RCOK) return rc;
	}

	if (_phase == 1) {
		_phase = 2;
		_curr_data = get_cc_manager()->buffer_request(rd_table_id, rd_table_id, query->from_key, RD);
		LOAD_VALUE(int64_t, from_account_bal_rd, schema_rd, _curr_data, 1);
		_curr_data = get_cc_manager()->buffer_request(wr_table_id, wr_table_id, query->from_key, WR);
		LOAD_VALUE(int64_t, from_account_bal_wr, schema_wr, _curr_data, 1);

		if (from_account_bal_rd + from_account_bal_wr < query->amount) {
			_self_abort = true;
			return ABORT;
		}

		from_account_bal_wr -= query->amount;
		STORE_VALUE(from_account_bal_wr, schema_wr, _curr_data, 1);

		_curr_data = get_cc_manager()->buffer_request(wr_table_id, wr_table_id, query->to_key, WR);
		LOAD_VALUE(int64_t, to_account_bal_wr, schema_wr, _curr_data, 1);
		to_account_bal_wr += query->amount;
		STORE_VALUE(to_account_bal_wr, schema_wr, _curr_data, 1);

		rc = get_cc_manager()->process_requests();
		if (rc != RCOK) return rc;
	}

	return RCOK;
}

void
BankStoreProcedure::txn_abort()
{
	StoreProcedure::txn_abort();
	_phase = 0;
	_curr_query_id = 0;
}

#endif