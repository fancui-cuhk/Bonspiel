#include "query.h"
#include "bank_query.h"
#include "bank.h"
#include "tpcc_helper.h"
#include "workload.h"
#include "table.h"
#include "manager.h"

#if WORKLOAD == BANK

// return random data from [0, max-1]
static uint64_t RAND(uint64_t max) {
    return glob_manager->rand_uint64() % max;
}

// random number from [x, y]
static uint64_t URand(uint64_t x, uint64_t y) {
    return x + RAND(y - x + 1);
}

QueryBank::QueryBank(int is_mp)
	: QueryBase()
{
	uint32_t to_part_id;

	from_key = RAND(BANK_NUM_KEYS);
	bool remote;
	if (is_mp < 0)
		remote = false;
	else if (is_mp > 0)
		remote = true;
	else
		remote = (g_num_parts > 1)? (glob_manager->rand_double() < BANK_REMOTE_PERC) : false;

	if (remote) {
		to_part_id = (g_part_id + 1 + RAND(g_num_parts - 1)) % g_num_parts;
		to_key = RAND(BANK_NUM_KEYS) * g_num_parts + to_part_id;
		_is_multi_partition = true;
	}
	else {
		while ((to_key = RAND(BANK_NUM_KEYS)) == from_key);
		to_key = to_key * g_num_parts + g_part_id;
	}

	from_key = from_key * g_num_parts + g_part_id;
	assert(from_key != to_key);
	amount = URand(1, MAX_TRANSFER);
}

QueryBank::QueryBank(char * data)
{
	memcpy((char*)this, data, sizeof(*this));
}

uint32_t
QueryBank::serialize(char * &raw_data)
{
	memcpy(raw_data, this, sizeof(*this));
	return sizeof(*this);
}


#endif