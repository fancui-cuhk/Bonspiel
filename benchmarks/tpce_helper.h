#pragma once
#include "global.h"
#include "helper.h"

#if WORKLOAD == TPCE

uint64_t stringHash(string str);

uint64_t accountKey(uint64_t a_id);
uint64_t customerKey(uint64_t c_id);
uint64_t brokerKey(uint64_t b_id);
uint64_t tradeKey(uint64_t t_id);
uint64_t tradeTypeKey(string tt_id);
uint64_t securityKey(string s_symb);
uint64_t watchListKey(uint64_t wl_id);
uint64_t exchangeKey();
uint64_t companyKey(uint64_t co_id);
uint64_t industryKey(string in_id);
uint64_t newsKey(uint64_t ni_id);
uint64_t sectorKey(string sc_id);
uint64_t addressKey(uint64_t ad_id);
uint64_t statusTypeKey(string st_id);
uint64_t taxrateKey();
uint64_t zipCodeKey();
uint64_t chargeKey(uint64_t tier, string tt_id);
uint64_t holdingSummaryKey(uint64_t ca_id, string s_symb);
uint64_t commissionRateKey(uint64_t c_tier, string tt_id, string ex_id, uint64_t qty);
uint64_t dailyMarketKey(string date, string s_symb);
uint64_t financialKey(uint64_t co_id, uint64_t year, uint64_t qtr);

// Return random data from [0, max-1]
uint64_t RAND(uint64_t max);
// Random number from [x, y]
uint64_t URand(uint64_t x, uint64_t y);
// Non-uniform random number
uint64_t NURand(uint64_t A, uint64_t x, uint64_t y);
// Random string with random length beteen min and max.
uint64_t MakeAlphaString(int min, int max, char* str);
uint64_t MakeNumberString(int min, int max, char* str);

class TPCEHelper {
public:
    static const char * get_txn_name(uint32_t txn_type_id);
};

#endif