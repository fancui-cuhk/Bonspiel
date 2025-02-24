#include "query.h"
#include "tpce_query.h"
#include "tpce.h"
#include "tpce_const.h"
#include "tpce_helper.h"
#include "workload.h"
#include "table.h"
#include "manager.h"

#if WORKLOAD == TPCE

QueryTPCE::QueryTPCE() : QueryBase() { }

QueryTPCE::QueryTPCE(QueryTPCE * query)
{
    type = query->type;
    _is_multi_partition = query->_is_multi_partition;
}

///////////////////////////////////////////
// Broker Volume
///////////////////////////////////////////
QueryBrokerVolumeTPCE::QueryBrokerVolumeTPCE() : QueryTPCE()
{
    type = TPCE_BROKER_VOLUME;
    uint32_t x = URand(1, 100);

    broker_num = URand(20, 40);
    uint64_t b_id;
    unordered_set<uint64_t> b_id_set;

    for (uint64_t i = 0; i < broker_num; i++) {
        if (x >= g_tpce_perc_remote) {
            // Local broker
            do {
                b_id = URand(1, g_broker_num);
            } while (b_id % g_num_parts != g_part_id ||
                     b_id_set.find(b_id) != b_id_set.end());
            b_id_set.insert(b_id);
        } else {
            // Remote broker
            do {
                b_id = URand(1, g_broker_num);
            } while (b_id_set.find(b_id) != b_id_set.end());
            if (b_id % g_num_parts != g_part_id) {
                _is_multi_partition = true;
            }
            b_id_set.insert(b_id);
        }
    }
    b_id_list.insert(b_id_list.end(), b_id_set.begin(), b_id_set.end());
}

///////////////////////////////////////////
// Customer Position
///////////////////////////////////////////
QueryCustomerPositionTPCE::QueryCustomerPositionTPCE() : QueryTPCE()
{
    type = TPCE_CUSTOMER_POSITION;
    uint32_t x = URand(1, 100);

    if (x >= g_tpce_perc_remote) {
        // Local customer
        do {
            c_id = URand(1, g_customer_num);
        } while (c_id % g_num_parts != g_part_id);
    } else {
        // Remote customer
        do {
            c_id = URand(1, g_customer_num);
        } while (c_id % g_num_parts == g_part_id);
        _is_multi_partition = true;
    }
}

///////////////////////////////////////////
// Market Feed
///////////////////////////////////////////
QueryMarketFeedTPCE::QueryMarketFeedTPCE() : QueryTPCE()
{
    type = TPCE_MARKET_FEED;

    symbol_num = 20;
    uint64_t s_symb_int;
    unordered_set<string> symbol_set;

    for (uint64_t i = 0; i < symbol_num; i++) {
        // All securities are local
        do {
            s_symb_int = URand(1, g_security_num);
        } while (s_symb_int % g_num_parts != g_part_id ||
                symbol_set.find(to_string(s_symb_int)) != symbol_set.end());
        symbol_set.insert(to_string(s_symb_int));
        price_list.emplace_back((double) (RAND(100) + 1));
    }
    symbol_list.insert(symbol_list.end(), symbol_set.begin(), symbol_set.end());
}

///////////////////////////////////////////
// Market Watch
///////////////////////////////////////////
QueryMarketWatchTPCE::QueryMarketWatchTPCE() : QueryTPCE()
{
    type = TPCE_MARKET_WATCH;
    uint32_t x = URand(1, 100);

    if (x >= g_tpce_perc_remote) {
        // Local customer account
        do {
            ca_id = URand(1, g_account_num);
        } while (ca_id % g_num_parts != g_part_id);
    } else {
        // Remote customer account
        do {
            ca_id = URand(1, g_account_num);
        } while (ca_id % g_num_parts == g_part_id);
        _is_multi_partition = true;
    }
}

///////////////////////////////////////////
// Security Detail
///////////////////////////////////////////
QuerySecurityDetailTPCE::QuerySecurityDetailTPCE() : QueryTPCE()
{
    type = TPCE_SECURITY_DETAIL;
    uint32_t x = URand(1, 100);

    uint64_t s_symb_int;

    if (x >= g_tpce_perc_remote) {
        // Local security
        do {
            s_symb_int = URand(1, g_security_num);
        } while (s_symb_int % g_num_parts != g_part_id);
    } else {
        // Remote security
        do {
            s_symb_int = URand(1, g_security_num);
        } while (s_symb_int % g_num_parts == g_part_id);
        _is_multi_partition = true;
    }
    strcpy(s_symb, to_string(s_symb_int).c_str());
}

///////////////////////////////////////////
// Trade Lookup
///////////////////////////////////////////
QueryTradeLookupTPCE::QueryTradeLookupTPCE() : QueryTPCE()
{
    type = TPCE_TRADE_LOOKUP;
    uint32_t x = URand(1, 100);

    frame_to_exec = URand(1, 3);
    trade_num = URand(10, 20);
    uint64_t t_id, s_symb_int;
    unordered_set<uint64_t> t_id_set;

    if (frame_to_exec == 1) {
        // First frame: lookup via a list of trade ids
        for (uint64_t i = 0; i < trade_num; i++) {
            if (x >= g_tpce_perc_remote) {
                // Local trade
                do {
                    t_id = URand(1, g_init_trade_num);
                } while (t_id % g_num_parts != g_part_id ||
                         t_id_set.find(t_id) != t_id_set.end());
                t_id_set.insert(t_id);
            } else {
                // Remote trade
                do {
                    t_id = URand(1, g_init_trade_num);
                } while (t_id_set.find(t_id) != t_id_set.end());
                if (t_id % g_num_parts != g_part_id) {
                    _is_multi_partition = true;
                }
                t_id_set.insert(t_id);
            }
        }
        t_id_list.insert(t_id_list.end(), t_id_set.begin(), t_id_set.end());
    } else if (frame_to_exec == 2) {
        // Second frame: lookup via a customer account id
        if (x >= g_tpce_perc_remote) {
            // Local customer account
            do {
                ca_id = URand(1, g_account_num);
            } while (ca_id % g_num_parts != g_part_id);
        } else {
            // Remote customer account
            do {
                ca_id = URand(1, g_account_num);
            } while (ca_id % g_num_parts == g_part_id);
            _is_multi_partition = true;
        }
    } else if (frame_to_exec == 3) {
        // Third frame: lookup via a security symbol
        if (x >= g_tpce_perc_remote) {
            // Local security
            do {
                s_symb_int = URand(1, g_security_num);
            } while (s_symb_int % g_num_parts != g_part_id);
        } else {
            // Remote security
            do {
                s_symb_int = URand(1, g_security_num);
            } while (s_symb_int % g_num_parts == g_part_id);
            _is_multi_partition = true;
        }
        strcpy(s_symb, to_string(s_symb_int).c_str());
    } else {
        assert(false);
    }
}

///////////////////////////////////////////
// Trade Order
///////////////////////////////////////////
// 90% market-buy & 10% limit-buy
QueryTradeOrderTPCE::QueryTradeOrderTPCE() : QueryTPCE()
{
    type = TPCE_TRADE_ORDER;
    uint32_t x = URand(1, 100);

    qty = URand(1, 1000);
    is_margin = (RAND(10) == 9) ? true : false;
    bid_price = (double) (RAND(100) + 1);
    uint64_t s_symb_int;

    // The customer account is always local
    do {
        ca_id = URand(1, g_account_num);
    } while (ca_id % g_num_parts != g_part_id);

    // Because g_init_trade_num > g_security num, every security
    // has corresponding rows in trade and last_trade tables
    if (x >= g_tpce_perc_remote) {
        // Local security
        do {
            s_symb_int = URand(1, g_security_num);
        } while (s_symb_int % g_num_parts != g_part_id);
    } else {
        // Remote security
        do {
            s_symb_int = URand(1, g_security_num);
        } while (s_symb_int % g_num_parts == g_part_id);
        _is_multi_partition = true;
    }
    strcpy(s_symb, to_string(s_symb_int).c_str());
}

///////////////////////////////////////////
// Trade Result
///////////////////////////////////////////
QueryTradeResultTPCE::QueryTradeResultTPCE() : QueryTPCE()
{
    type = TPCE_TRADE_RESULT;

    // t_id will be generated in the function logic
    // All trades are local
    t_id = 0;
}

///////////////////////////////////////////
// Trade Status
///////////////////////////////////////////
QueryTradeStatusTPCE::QueryTradeStatusTPCE() : QueryTPCE()
{
    type = TPCE_TRADE_STATUS;
    uint32_t x = URand(1, 100);

    trade_num = 50;

    if (x >= g_tpce_perc_remote) {
        // Local customer account
        do {
            ca_id = URand(1, g_account_num);
        } while (ca_id % g_num_parts != g_part_id);
    } else {
        // Remote customer account
        do {
            ca_id = URand(1, g_account_num);
        } while (ca_id % g_num_parts == g_part_id);
        _is_multi_partition = true;
    }
}

///////////////////////////////////////////
// Trade Update
///////////////////////////////////////////
QueryTradeUpdateTPCE::QueryTradeUpdateTPCE() : QueryTPCE()
{
    type = TPCE_TRADE_UPDATE;
    uint32_t x = URand(1, 100);

    // TODO: currently frame 3 is excluded since it is unstable
    frame_to_exec = URand(1, 2);
    trade_num = URand(10, 20);
    uint64_t t_id, s_symb_int;
    unordered_set<uint64_t> t_id_set;

    if (frame_to_exec == 1) {
        // First frame: update trades according to trade ids
        // Executor's names are modified
        for (uint64_t i = 0; i < trade_num; i++) {
            if (x >= g_tpce_perc_remote) {
                // Local trade
                do {
                    t_id = URand(1, g_init_trade_num);
                } while (t_id % g_num_parts != g_part_id ||
                         t_id_set.find(t_id) != t_id_set.end());
                t_id_set.insert(t_id);
            } else {
                // Remote trade
                do {
                    t_id = URand(1, g_init_trade_num);
                } while (t_id_set.find(t_id) != t_id_set.end());
                if (t_id % g_num_parts != g_part_id) {
                    _is_multi_partition = true;
                }
                t_id_set.insert(t_id);
            }
        }
        t_id_list.insert(t_id_list.end(), t_id_set.begin(), t_id_set.end());
    } else if (frame_to_exec == 2) {
        // Second frame: update trades of a customer account id
        // The settlement cash type is modified
        if (x >= g_tpce_perc_remote) {
            // Local customer account
            do {
                ca_id = URand(1, g_account_num);
            } while (ca_id % g_num_parts != g_part_id);
        } else {
            // Remote customer account
            do {
                ca_id = URand(1, g_account_num);
            } while (ca_id % g_num_parts == g_part_id);
            _is_multi_partition = true;
        }
    } else if (frame_to_exec == 3) {
        // Third frame: update trades of a seucurity symbol
        // The descriptions of cash trades are modified
        if (x >= g_tpce_perc_remote) {
            // Local security
            do {
                s_symb_int = URand(1, g_security_num);
            } while (s_symb_int % g_num_parts != g_part_id);
        } else {
            // Remote security
            do {
                s_symb_int = URand(1, g_security_num);
            } while (s_symb_int % g_num_parts == g_part_id);
            _is_multi_partition = true;
        }
        strcpy(s_symb, to_string(s_symb_int).c_str());
    } else {
        assert(false);
    }
}

#endif