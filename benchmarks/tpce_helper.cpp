#include "tpce_helper.h"
#include "manager.h"
#include "tpce_const.h"
#include <functional>

#if WORKLOAD == TPCE

uint64_t stringHash(string str) {
    hash<string> hasher;
    return hasher(str);
}

uint64_t accountKey(uint64_t a_id) {
    return a_id;
}

uint64_t customerKey(uint64_t c_id) {
    return c_id;
}

uint64_t brokerKey(uint64_t b_id) {
    return b_id;
}

uint64_t tradeKey(uint64_t t_id) {
    return t_id;
}

// Use 3 bits to represent trade type
uint64_t tradeTypeKey(string tt_id) {
    assert(tt_id == "TMB" || tt_id == "TMS" || tt_id == "TSL" || tt_id == "TLS" || tt_id == "TLB");

    if (tt_id == "TMB") {
        return TMB;
    } else if (tt_id == "TMS") {
        return TMS;
    } else if (tt_id == "TSL") {
        return TSL;
    } else if (tt_id == "TLS") {
        return TLS;
    } else if (tt_id == "TLB") {
        return TLB;
    } else {
        return INVALID_STATUS;
    }
}

uint64_t securityKey(string s_symb) {
    return stoi(s_symb);
}

uint64_t watchListKey(uint64_t wl_id) {
    return wl_id;
}

uint64_t exchangeKey() {
    // Only one exchange : "NASDAQ"
    return 1;
}

uint64_t companyKey(uint64_t co_id) {
    return co_id;
}

uint64_t industryKey(string in_id) {
    return stoi(in_id);
}

uint64_t newsKey(uint64_t ni_id) {
    return ni_id;
}

uint64_t sectorKey(string sc_id) {
    return stoi(sc_id);
}

uint64_t addressKey(uint64_t ad_id) {
    return ad_id;
}

// Use 3 bits to represent status type
uint64_t statusTypeKey(string st_id) {
    assert(st_id == "ACTV" || st_id == "CMPT" || st_id == "CNCL" || st_id == "PNDG" || st_id == "SBMT");

    if (st_id == "ACTV") {
        return ACTV;
    } else if (st_id == "CMPT") {
        return CMPT;
    } else if (st_id == "CNCL") {
        return CNCL;
    } else if (st_id == "PNDG") {
        return PNDG;
    } else if (st_id == "SBMT") {
        return SBMT;
    } else {
        return INVALID_STATUS;
    }
}

uint64_t taxrateKey() {
    // Only one taxrate : "US1"
    return 1;
}

uint64_t zipCodeKey() {
    // Only one zip code: "95014"
    return 1;
}

uint64_t chargeKey(uint64_t tier, string tt_id) {
    // tt_id - 32 bits; tier - 32 bits
    return (tradeTypeKey(tt_id) << 32) | tier;
}

uint64_t holdingSummaryKey(uint64_t ca_id, string s_symb) {
    // ca_id - 32 bits; s_symb - 32 bits
    return (ca_id << 32) | stoi(s_symb);
}

uint64_t commissionRateKey(uint64_t c_tier, string tt_id, string ex_id, uint64_t qty) {
    // c_tier - 2 bits; tradeTypeKey(tt_id) - 3 bits; ex_id - 10 bits; qty - 16 bits
    return (c_tier << 29) | (tradeTypeKey(tt_id) << 26) | (exchangeKey() << 16) | qty;
}

// This function is currently not used
uint64_t dailyMarketKey(string date, string s_symb) {
    return stringHash(date + s_symb);
}

// This function is currently not used
uint64_t financialKey(uint64_t co_id, uint64_t year, uint64_t qtr) {
    // co_id - 30 bits; year - 14 bits; qtr - 16 bits
    return (co_id << 30) | (year << 16) | qtr;
}

// Generate a random integer in [0, max)
uint64_t RAND(uint64_t max) {
    return glob_manager->rand_uint64() % max;
}

// Generate a random integer in [x, y]
uint64_t URand(uint64_t x, uint64_t y) {
    return x + RAND(y - x + 1);
}

uint64_t NURand(uint64_t A, uint64_t x, uint64_t y) {
    static bool C_255_init = false;
    static bool C_1023_init = false;
    static bool C_8191_init = false;
    static uint64_t C_255, C_1023, C_8191;
    int C = 0;
    switch (A) {
    case 255:
        if (!C_255_init) {
            C_255 = (uint64_t) URand(0,255);
            C_255_init = true;
        }
        C = C_255;
        break;
    case 1023:
        if (!C_1023_init) {
            C_1023 = (uint64_t) URand(0,1023);
            C_1023_init = true;
        }
        C = C_1023;
        break;
    case 8191:
        if (!C_8191_init) {
            C_8191 = (uint64_t) URand(0,8191);
            C_8191_init = true;
        }
        C = C_8191;
        break;
    default:
        M_ASSERT(false, "Error! NURand\n");
        exit(-1);
    }
    return (((URand(0,A) | URand(x,y))+C)%(y-x+1))+x;
}

uint64_t MakeAlphaString(int min, int max, char* str) {
    char char_list[] = {'1','2','3','4','5','6','7','8','9','a','b','c',
                        'd','e','f','g','h','i','j','k','l','m','n','o',
                        'p','q','r','s','t','u','v','w','x','y','z','A',
                        'B','C','D','E','F','G','H','I','J','K','L','M',
                        'N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};
    uint64_t cnt = URand(min, max - 1);
    for (uint32_t i = 0; i < cnt; i++)
        str[i] = char_list[URand(0L, 60L)];
    for (int i = cnt; i < max; i++)
        str[i] = '\0';

    return cnt;
}

uint64_t MakeNumberString(int min, int max, char* str) {
    uint64_t cnt = URand(min, max - 1);
    for (UInt32 i = 0; i < cnt; i++) {
        uint64_t r = URand(0L, 9L);
        str[i] = '0' + r;
    }
    for (int i = cnt; i < max; i++)
        str[i] = '\0';

    return cnt;
}

const char * TPCEHelper::get_txn_name(uint32_t txn_type_id)
{
    switch(txn_type_id) {
    case TPCE_BROKER_VOLUME:
        return "Broker Volume";
    case TPCE_CUSTOMER_POSITION:
        return "Customer Position";
    case TPCE_MARKET_FEED:
        return "Market Feed";
    case TPCE_MARKET_WATCH:
        return "Market Watch";
    case TPCE_SECURITY_DETAIL:
        return "Security Detail";
    case TPCE_TRADE_LOOKUP:
        return "Trade Lookup";
    case TPCE_TRADE_ORDER:
        return "Trade Order";
    case TPCE_TRADE_RESULT:
        return "Trade Result";
    case TPCE_TRADE_STATUS:
        return "Trade Status";
    case TPCE_TRADE_UPDATE:
        return "Trade Update";
    default:
        assert(false);
    }
}

#endif