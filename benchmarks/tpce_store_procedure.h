#pragma once

#include "store_procedure.h"

class TPCEStoreProcedure : public StoreProcedure {
public:
    TPCEStoreProcedure(TxnManager * txn_man, QueryBase * query);
    ~TPCEStoreProcedure();

    RC       execute();
    void     txn_abort();

private:
    void     init();

    RC       execute_broker_volume();
    RC       execute_customer_position();
    RC       execute_market_feed();
    RC       execute_market_watch();
    RC       execute_security_detail();
    RC       execute_trade_lookup();
    RC       execute_trade_order();
    RC       execute_trade_result();
    RC       execute_trade_status();
    RC       execute_trade_update();

    uint32_t get_txn_type();

    // For customer_position
    int64_t          _ca_id;

    // For market_feed
    vector<uint64_t> _trade_request_list;

    // For trade_result
    bool             _is_cash;
    uint64_t         _qty;
    char             _s_symb[15];
    uint64_t         _b_id;

    // For trade_update
    vector<uint64_t> _t_id_list;
};