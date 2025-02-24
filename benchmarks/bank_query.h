#pragma once

#include "global.h"
#include "helper.h"
#include "query.h"

class workload;

class QueryBank : public QueryBase {
public:
    QueryBank() : QueryBank(0) {};
    QueryBank(int is_mp);
    QueryBank(char * raw_data);

    uint32_t serialize(char * &raw_data);

    int64_t from_key;
    int64_t to_key;
	int64_t amount;
};
