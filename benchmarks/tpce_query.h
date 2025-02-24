#pragma once

#include "global.h"
#include "helper.h"
#include "query.h"

class QueryTPCE : public QueryBase {
public:
    QueryTPCE();
    QueryTPCE(QueryTPCE * query);
    virtual ~QueryTPCE() {};

    uint32_t type;
};

// Read-only transaction
class QueryBrokerVolumeTPCE : public QueryTPCE {
public:
    QueryBrokerVolumeTPCE();
    QueryBrokerVolumeTPCE(QueryBrokerVolumeTPCE * query)
        : QueryTPCE(query)
    {
        broker_num = query->broker_num;
        b_id_list = vector<uint64_t>(query->b_id_list.begin(),
                                     query->b_id_list.end());
    };
    // QueryBrokerVolumeTPCE(char * data);
    // ~QueryBrokerVolumeTPCE();

    uint64_t broker_num;
    vector<uint64_t> b_id_list;
};

// Read-only transaction
class QueryCustomerPositionTPCE : public QueryTPCE {
public:
    QueryCustomerPositionTPCE();
    QueryCustomerPositionTPCE(QueryCustomerPositionTPCE * query)
        : QueryTPCE(query)
    {
        c_id = query->c_id;
    };
    // QueryCustomerPositionTPCE(char * data);
    // ~QueryCustomerPositionTPCE();

    uint64_t c_id;
};

class QueryMarketFeedTPCE : public QueryTPCE {
public:
    QueryMarketFeedTPCE();
    QueryMarketFeedTPCE(QueryMarketFeedTPCE * query)
        : QueryTPCE(query)
    {
        symbol_num = query->symbol_num;
        price_list = vector<double>(query->price_list.begin(),
                                    query->price_list.end());
        symbol_list = vector<string>(query->symbol_list.begin(),
                                     query->symbol_list.end());
    };
    // QueryMarketFeedTPCE(char * data);
    // ~QueryMarketFeedTPCE();

    uint32_t symbol_num;
    vector<double> price_list;
    vector<string> symbol_list;
};

// Read-only transaction
// TODO : Currently only portfolio-watch is supported
class QueryMarketWatchTPCE : public QueryTPCE {
public:
    QueryMarketWatchTPCE();
    QueryMarketWatchTPCE(QueryMarketWatchTPCE * query)
        : QueryTPCE(query)
    {
        ca_id = query->ca_id;
    };
    // QueryMarketWatchTPCE(char * data);
    // ~QueryMarketWatchTPCE();

    uint64_t ca_id;
};

// Read-only transaction
class QuerySecurityDetailTPCE : public QueryTPCE {
public:
    QuerySecurityDetailTPCE();
    QuerySecurityDetailTPCE(QuerySecurityDetailTPCE * query)
        : QueryTPCE(query)
    {
        strcpy(s_symb, query->s_symb);
    };
    // QuerySecurityDetailTPCE(char * data);
    // ~QuerySecurityDetailTPCE();

    char s_symb[15];
};

// Read-only transaction
class QueryTradeLookupTPCE : public QueryTPCE {
public:
    QueryTradeLookupTPCE();
    QueryTradeLookupTPCE(QueryTradeLookupTPCE * query)
        : QueryTPCE(query)
    {
        frame_to_exec = query->frame_to_exec;
        trade_num = query->trade_num;
        t_id_list = vector<uint64_t>(query->t_id_list.begin(),
                                     query->t_id_list.end());
        strcpy(s_symb, query->s_symb);
        ca_id = query->ca_id;
    };
    // QueryTradeLookupTPCE(char * data);
    // ~QueryTradeLookupTPCE();

    uint32_t frame_to_exec;
    uint64_t trade_num;
    vector<uint64_t> t_id_list;
    char s_symb[15];
    uint64_t ca_id;
};

class QueryTradeOrderTPCE : public QueryTPCE {
public:
    QueryTradeOrderTPCE();
    QueryTradeOrderTPCE(QueryTradeOrderTPCE * query)
        : QueryTPCE(query)
    {
        ca_id = query->ca_id;
        strcpy(s_symb, query->s_symb);
        qty = query->qty;
    };
    // QueryTradeOrderTPCE(char * data);
    // ~QueryTradeOrderTPCE();

    uint64_t ca_id;
    char s_symb[15];
    uint64_t qty;
    bool is_margin;
    double bid_price;
};

class QueryTradeResultTPCE : public QueryTPCE {
public:
    QueryTradeResultTPCE();
    QueryTradeResultTPCE(QueryTradeResultTPCE * query)
        : QueryTPCE(query)
    {
        t_id = query->t_id;
    };
    // QueryTradeResultTPCE(char * data);
    // ~QueryTradeResultTPCE();

    uint64_t t_id;
};

// Read-only transaction
class QueryTradeStatusTPCE : public QueryTPCE {
public:
    QueryTradeStatusTPCE();
    QueryTradeStatusTPCE(QueryTradeStatusTPCE * query)
        : QueryTPCE(query)
    {
        ca_id = query->ca_id;
        trade_num = query->trade_num;
    };
    // QueryTradeStatusTPCE(char * data);
    // ~QueryTradeStatusTPCE();

    uint64_t ca_id;
    uint64_t trade_num;
};

class QueryTradeUpdateTPCE : public QueryTPCE {
public:
    QueryTradeUpdateTPCE();
    QueryTradeUpdateTPCE(QueryTradeUpdateTPCE * query)
        : QueryTPCE(query)
    {
        frame_to_exec = query->frame_to_exec;
        trade_num = query->trade_num;
        t_id_list = vector<uint64_t>(query->t_id_list.begin(),
                                     query->t_id_list.end());
        strcpy(s_symb, query->s_symb);
        ca_id = query->ca_id;
    };
    // QueryTradeUpdateTPCE(char * data);
    // ~QueryTradeUpdateTPCE();

    uint32_t frame_to_exec;
    uint64_t trade_num;
    vector<uint64_t> t_id_list;
    char s_symb[15];
    uint64_t ca_id;
};