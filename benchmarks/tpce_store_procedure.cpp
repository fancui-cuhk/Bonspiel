#include "tpce.h"
#include "tpce_query.h"
#include "tpce_helper.h"
#include "tpce_const.h"
#include "tpce_store_procedure.h"
#include "manager.h"
#include "cc_manager.h"
#include "row.h"
#include "table.h"
#include "index_base.h"
#include "catalog.h"

#if WORKLOAD == TPCE

TPCEStoreProcedure::TPCEStoreProcedure(TxnManager * txn_man, QueryBase * query)
    : StoreProcedure(txn_man, query)
{
    init();
}

TPCEStoreProcedure::~TPCEStoreProcedure() { }

void TPCEStoreProcedure::init()
{
    StoreProcedure::init();
    _ca_id = -1;
    _trade_request_list.clear();
    _is_cash = false;
    _qty = 0;
    memset(_s_symb, 0, 15);
    _b_id = 0;
    _t_id_list.clear();
}

uint32_t TPCEStoreProcedure::get_txn_type()
{
    return ((QueryTPCE *)_query)->type;
}

RC TPCEStoreProcedure::execute()
{
    QueryTPCE * query = (QueryTPCE *) _query;
    assert(_txn->is_coord());
    switch (query->type) {
        case TPCE_BROKER_VOLUME:     return execute_broker_volume();
        case TPCE_CUSTOMER_POSITION: return execute_customer_position();
        case TPCE_MARKET_FEED:       return execute_market_feed();
        case TPCE_MARKET_WATCH:      return execute_market_watch();
        case TPCE_SECURITY_DETAIL:   return execute_security_detail();
        case TPCE_TRADE_LOOKUP:      return execute_trade_lookup();
        case TPCE_TRADE_ORDER:       return execute_trade_order();
        case TPCE_TRADE_RESULT:      return execute_trade_result();
        case TPCE_TRADE_STATUS:      return execute_trade_status();
        case TPCE_TRADE_UPDATE:      return execute_trade_update();
        default:                     assert(false);
    }
}

RC TPCEStoreProcedure::execute_broker_volume()
{
    RC rc = RCOK;
    uint64_t key;
    char * _curr_data;
    Catalog * schema = NULL;

    QueryBrokerVolumeTPCE * query = (QueryBrokerVolumeTPCE *) _query;
    WorkloadTPCE * wl = (WorkloadTPCE *) GET_WORKLOAD;

    if (_phase == 0) {
        for (uint64_t i = 0; i < query->broker_num; i++) {
            uint64_t b_id = query->b_id_list[i];
            key = brokerKey(b_id);
            get_cc_manager()->row_request(TAB_BROKER, IDX_BROKER, key, RD);
            get_cc_manager()->row_request(TAB_TRADE_REQUEST, IDX_TRADE_REQUEST_B_ID, key, RD, 0, 50);
        }

        _phase = 1;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 1) {
        for (uint64_t i = 0; i < query->broker_num; i++) {
            uint64_t b_id = query->b_id_list[i];
            key = brokerKey(b_id);

            schema = wl->t_broker->get_schema();
            _curr_data = get_cc_manager()->buffer_request(TAB_BROKER, IDX_BROKER, key, RD);

            __attribute__((unused)) char name[50];
            memcpy(name, row_t::get_value(schema, B_NAME, _curr_data), schema->get_field_size(B_NAME));

            schema = wl->t_trade_request->get_schema();
            vector<char *> rows;
            get_cc_manager()->buffer_request(rows, TAB_TRADE_REQUEST, IDX_TRADE_REQUEST_B_ID, key, RD, 0, 50);

            for (auto it = rows.begin(); it != rows.end(); it++) {
                _curr_data = *it;
                __attribute__((unused)) LOAD_VALUE(double, price, schema, _curr_data, TR_BID_PRICE);
                __attribute__((unused)) LOAD_VALUE(uint64_t, qty, schema, _curr_data, TR_QTY);
            }

            _phase = 2;
            rc = get_cc_manager()->process_requests();
            if (rc != RCOK) return rc;
        }
    }

    return RCOK;
}

RC TPCEStoreProcedure::execute_customer_position()
{
    RC rc = RCOK;
    uint64_t key;
    char * _curr_data;
    Catalog * schema = NULL;

    QueryCustomerPositionTPCE * query = (QueryCustomerPositionTPCE *) _query;
    WorkloadTPCE * wl = (WorkloadTPCE *) GET_WORKLOAD;

    if (_phase == 0) {
        key = customerKey(query->c_id);
        get_cc_manager()->row_request(TAB_CUSTOMER_ACCOUNT, IDX_CUSTOMER_ACCOUNT_C_ID, key, RD, 0, 5);

        _phase = 1;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 1) {
        key = customerKey(query->c_id);
        schema = wl->t_customer_account->get_schema();
        vector<char *> rows;
        get_cc_manager()->buffer_request(rows, TAB_CUSTOMER_ACCOUNT, IDX_CUSTOMER_ACCOUNT_C_ID, key, RD, 0, 5);

        for (auto it = rows.begin(); it != rows.end(); it++) {
            _curr_data = *it;
            __attribute__((unused)) LOAD_VALUE(double, bal, schema, _curr_data, CA_BAL);
            LOAD_VALUE(uint64_t, ca_id, schema, _curr_data, CA_ID);
            _ca_id = (int64_t) ca_id;
        }

        if (_ca_id == -1) {
            // No specified account ID
            return RCOK;
        }

        key = accountKey((uint64_t) _ca_id);
        get_cc_manager()->row_request(TAB_TRADE, IDX_TRADE_CA_ID, key, RD, 0, 20);

        _phase = 2;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 2) {
        key = accountKey((uint64_t) _ca_id);
        schema = wl->t_trade->get_schema();
        vector<char *> rows;
        get_cc_manager()->buffer_request(rows, TAB_TRADE, IDX_TRADE_CA_ID, key, RD, 0, 5);

        for (auto it = rows.begin(); it != rows.end(); it++) {
            _curr_data = *it;
            __attribute__((unused)) LOAD_VALUE(uint64_t, t_id, schema, _curr_data, T_ID);
            __attribute__((unused)) LOAD_VALUE(double, qty, schema, _curr_data, T_QTY);

            _phase = 3;
            rc = get_cc_manager()->process_requests();
            if (rc != RCOK) return rc;
        }
    }

    return RCOK;
}

RC TPCEStoreProcedure::execute_market_feed()
{
    RC rc = RCOK;
    uint64_t key;
    char * _curr_data;
    Catalog * schema = NULL;

    QueryMarketFeedTPCE * query = (QueryMarketFeedTPCE *) _query;
    WorkloadTPCE * wl = (WorkloadTPCE *) GET_WORKLOAD;

    if (_phase == 0) {
        for (uint32_t i = 0; i < query->symbol_num; i++) {
            key = securityKey(query->symbol_list[i]);
            get_cc_manager()->row_request(TAB_LAST_TRADE, IDX_LAST_TRADE, key, WR);
            get_cc_manager()->row_request(TAB_TRADE_REQUEST, IDX_TRADE_REQUEST_S_SYMB, key, RD, 0, 20);
        }

        _phase = 1;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 1) {
        for (uint32_t i = 0; i < query->symbol_num; i++) {
            key = securityKey(query->symbol_list[i]);

            schema = wl->t_last_trade->get_schema();
            _curr_data = get_cc_manager()->buffer_request(TAB_LAST_TRADE, IDX_LAST_TRADE, key, WR);
            if (_curr_data == NULL) {
                // No trade for this security yet
                continue;
            }

            uint64_t dts = 20240301;
            STORE_VALUE(dts, schema, _curr_data, LT_DTS);
            STORE_VALUE(query->price_list[i], schema, _curr_data, LT_PRICE);
            LOAD_VALUE(uint64_t, vol, schema, _curr_data, LT_VOL);
            vol += 10;
            STORE_VALUE(vol, schema, _curr_data, LT_VOL);

            schema = wl->t_trade_request->get_schema();
            vector<char *> rows;
            get_cc_manager()->buffer_request(rows, TAB_TRADE_REQUEST, IDX_TRADE_REQUEST_S_SYMB, key, RD, 0, 20);

            for (auto it = rows.begin(); it != rows.end(); it++) {
                _curr_data = *it;
                LOAD_VALUE(double, bid_price, schema, _curr_data, TR_BID_PRICE);
                __attribute__((unused)) LOAD_VALUE(uint64_t, qty, schema, _curr_data, TR_QTY);
                LOAD_VALUE(uint64_t, t_id, schema, _curr_data, TR_T_ID);

                if (bid_price >= query->price_list[i]) {
                    // All limit orders are limit buys
                    // Now the price is lower than the bid price
                    _trade_request_list.emplace_back(t_id);
                    key = tradeKey(t_id);
                    get_cc_manager()->row_request(TAB_TRADE, IDX_TRADE, key, WR);

                    // row_t * row = new row_t(wl->t_trade_history);
                    // char st_id[5] = "CMPT";
                    // row->set_value(TH_T_ID, &t_id);
                    // row->set_value(TH_DTS, &dts);
                    // row->set_value(TH_ST_ID, st_id);
                    // get_cc_manager()->row_request(row, TAB_TRADE_HISTORY, INS);
                }
            }
        }

        _phase = 2;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 2) {
        schema = wl->t_trade->get_schema();

        for (auto it = _trade_request_list.begin(); it != _trade_request_list.end(); it++) {
            uint64_t t_id = *it;
            key = tradeKey(t_id);
            _curr_data = get_cc_manager()->buffer_request(TAB_TRADE, IDX_TRADE, key, WR);

            uint64_t dts = 20240301;
            char st_id[5] = "CMPT";
            STORE_VALUE(dts, schema, _curr_data, T_DTS);
            row_t::set_value(schema, T_ST_ID, _curr_data, st_id);
        }

        _phase = 3;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    return RCOK;
}

RC TPCEStoreProcedure::execute_market_watch()
{
    RC rc = RCOK;
    uint64_t key;
    char * _curr_data;
    Catalog * schema = NULL;

    QueryMarketWatchTPCE * query = (QueryMarketWatchTPCE *) _query;
    WorkloadTPCE * wl = (WorkloadTPCE *) GET_WORKLOAD;

    if (_phase == 0) {
        key = accountKey(query->ca_id);
        get_cc_manager()->row_request(TAB_HOLDING, IDX_HOLDING_CA_ID, key, RD, 0, 50);

        _phase = 1;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 1) {
        schema = wl->t_holding->get_schema();
        key = accountKey(query->ca_id);
        vector<char *> rows;
        get_cc_manager()->buffer_request(rows, TAB_HOLDING, IDX_HOLDING_CA_ID, key, RD, 0, 50);

        for (auto it = rows.begin(); it != rows.end(); it++) {
            _curr_data = *it;
            char s_symb[15];
            memcpy(s_symb, row_t::get_value(schema, H_S_SYMB, _curr_data), schema->get_field_size(H_S_SYMB));
            string s_symb_str(s_symb);
            key = securityKey(s_symb_str);
            get_cc_manager()->row_request(TAB_SECURITY, IDX_SECURITY, key, RD);
        }

        _phase = 2;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    return RCOK;
}

RC TPCEStoreProcedure::execute_security_detail()
{
    RC rc = RCOK;
    uint64_t key;

    QuerySecurityDetailTPCE * query = (QuerySecurityDetailTPCE *) _query;

    if (_phase == 0) {
        string s_symb_str(query->s_symb);
        key = securityKey(s_symb_str);
        get_cc_manager()->row_request(TAB_SECURITY, IDX_SECURITY, key, RD);
        get_cc_manager()->row_request(TAB_DAILY_MARKET, IDX_DAILY_MARKET, key, RD);

        _phase = 1;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    return RCOK;
}

RC TPCEStoreProcedure::execute_trade_lookup()
{
    RC rc = RCOK;
    uint64_t key;

    QueryTradeLookupTPCE * query = (QueryTradeLookupTPCE *) _query;

    if (query->frame_to_exec == 1) {
        // First frame: lookup via a list of trade ids
        if (_phase == 0) {
            for (uint64_t i = 0; i < query->trade_num; i++) {
                uint64_t t_id = query->t_id_list[i];
                key = tradeKey(t_id);
                get_cc_manager()->row_request(TAB_TRADE, IDX_TRADE, key, RD);
            }

            _phase = 1;
            rc = get_cc_manager()->process_requests();
            if (rc != RCOK) return rc;
        }
    } else if (query->frame_to_exec == 2) {
        // Second frame: lookup via a customer account id
        if (_phase == 0) {
            key = accountKey(query->ca_id);
            get_cc_manager()->row_request(TAB_TRADE, IDX_TRADE_CA_ID, key, RD, 0, 1);

            _phase = 1;
            rc = get_cc_manager()->process_requests();
            if (rc != RCOK) return rc;
        }
    } else if (query->frame_to_exec == 3) {
        // Third frame: lookup via a security symbol
        if (_phase == 0) {
            string s_symb_str(query->s_symb);
            key = securityKey(s_symb_str);
            get_cc_manager()->row_request(TAB_TRADE, IDX_TRADE_S_SYMB, key, RD, 0, 1);

            _phase = 1;
            rc = get_cc_manager()->process_requests();
            if (rc != RCOK) return rc;
        }
    } else {
        assert(false);
    }

    return RCOK;
}

RC TPCEStoreProcedure::execute_trade_order()
{
    RC rc = RCOK;
    uint64_t key;
    char * _curr_data;
    Catalog * schema = NULL;

    QueryTradeOrderTPCE * query = (QueryTradeOrderTPCE *) _query;
    WorkloadTPCE * wl = (WorkloadTPCE *) GET_WORKLOAD;

    if (_phase == 0) {
        key = accountKey(query->ca_id);
        get_cc_manager()->row_request(TAB_CUSTOMER_ACCOUNT, IDX_CUSTOMER_ACCOUNT, key, RD);

        _phase = 1;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    // The permission checking process is omitted here
    if (_phase == 1) {
        key = accountKey(query->ca_id);
        schema = wl->t_customer_account->get_schema();
        _curr_data = get_cc_manager()->buffer_request(TAB_CUSTOMER_ACCOUNT, IDX_CUSTOMER_ACCOUNT, key, RD);

        __attribute__((unused)) LOAD_VALUE(uint64_t, b_id, schema, _curr_data, CA_B_ID);
        __attribute__((unused)) LOAD_VALUE(uint64_t, c_id, schema, _curr_data, CA_C_ID);

        string s_symb_str(query->s_symb);
        key = securityKey(s_symb_str);
        get_cc_manager()->row_request(TAB_LAST_TRADE, IDX_LAST_TRADE, key, RD);

        _phase = 2;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 2) {
        string s_symb_str(query->s_symb);
        key = securityKey(s_symb_str);

        schema = wl->t_last_trade->get_schema();
        _curr_data = get_cc_manager()->buffer_request(TAB_LAST_TRADE, IDX_LAST_TRADE, key, RD);

        LOAD_VALUE(double, price, schema, _curr_data, LT_PRICE);
        double expected = (query->is_margin ? query->bid_price : price) * query->qty;

        uint64_t t_id = wl->next_t_id();
        // In new trades, brokers are always local
        uint64_t b_id = URand(1, g_broker_num);
        while (b_id % g_num_parts != g_part_id) {
            b_id = URand(1, g_broker_num);
        }
        uint64_t dts = 20240301;
        char st_id[5];
        char tt_id[4];
        if (query->is_margin) {
            strcpy(st_id, "PNDG");
            strcpy(tt_id, "TLB");
        } else {
            strcpy(st_id, "CMPT");
            strcpy(tt_id, "TMB");
        }
        char exec_name[50];
        MakeAlphaString(40, 50, exec_name);
        double charge = RAND(100) + 1;
        double commission = RAND(1000);
        double tax = 0.01 * expected;
        bool lifo = true;
        bool is_cash = (RAND(10) < 9) ? true : false;

        // trade
        row_t * row = new row_t(wl->t_trade);
        row->set_value(T_ID, &t_id);
        row->set_value(T_DTS, &dts);
        row->set_value(T_ST_ID, st_id);
        row->set_value(T_TT_ID, tt_id);
        row->set_value(T_IS_CASH, &is_cash);
        row->set_value(T_S_SYMB, query->s_symb);
        row->set_value(T_QTY, &query->qty);
        row->set_value(T_BID_PRICE, &query->bid_price);
        row->set_value(T_CA_ID, &query->ca_id);
        row->set_value(T_EXEC_NAME, exec_name);
        row->set_value(T_TRADE_PRICE, &price);
        row->set_value(T_CHRG, &charge);
        row->set_value(T_COMM, &commission);
        row->set_value(T_TAX, &tax);
        row->set_value(T_LIFO, &lifo);
        get_cc_manager()->row_request(row, TAB_TRADE, INS);

        if (query->is_margin) {
            // trade_request
            // This is a margin (limit) buy
            row = new row_t(wl->t_trade_request);
            row->set_value(TR_T_ID, &t_id);
            row->set_value(TR_TT_ID, tt_id);
            row->set_value(TR_S_SYMB, query->s_symb);
            row->set_value(TR_QTY, &query->qty);
            row->set_value(TR_BID_PRICE, &query->bid_price);
            row->set_value(TR_B_ID, &b_id);
            get_cc_manager()->row_request(row, TAB_TRADE_REQUEST, INS);
        } else {
            // trade_history
            // row = new row_t(wl->t_trade_history);
            // row->set_value(TH_T_ID, &t_id);
            // row->set_value(TH_DTS, &dts);
            // row->set_value(TH_ST_ID, st_id);
            // get_cc_manager()->row_request(row, TAB_TRADE_HISTORY, INS);
        }

        _phase = 3;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    return RCOK;
}

RC TPCEStoreProcedure::execute_trade_result()
{
    RC rc = RCOK;
    uint64_t key;
    char * _curr_data;
    Catalog * schema = NULL;

    QueryTradeResultTPCE * query = (QueryTradeResultTPCE *) _query;
    WorkloadTPCE * wl = (WorkloadTPCE *) GET_WORKLOAD;

    int64_t tmp_t_id = wl->next_t_id_trade_result();
    if (tmp_t_id == -1) {
        // Currently there is no trade left for processing by Trade-Result transactions
        return RCOK;
    }
    query->t_id = uint64_t(tmp_t_id);

    if (_phase == 0) {
        key = tradeKey(query->t_id);
        get_cc_manager()->row_request(TAB_TRADE, IDX_TRADE, key, RD, 0, 1);

        _phase = 1;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 1) {
        key = tradeKey(query->t_id);
        schema = wl->t_trade->get_schema();
        _curr_data = get_cc_manager()->buffer_request(TAB_TRADE, IDX_TRADE, key, RD);

        if (!_curr_data) {
            // The requested trade id has not been processed by Trade-Order transactions yet
            return RCOK;
        }

        LOAD_VALUE(uint64_t, ca_id, schema, _curr_data, T_CA_ID);
        LOAD_VALUE(bool, is_cash, schema, _curr_data, T_IS_CASH);
        LOAD_VALUE(uint64_t, qty, schema, _curr_data, T_QTY);
        LOAD_VALUE(double, price, schema, _curr_data, T_BID_PRICE);
        memcpy(_s_symb, row_t::get_value(schema, T_S_SYMB, _curr_data), schema->get_field_size(T_S_SYMB));

        _ca_id = ca_id;
        _is_cash = is_cash;
        _qty = qty;
 
        uint64_t dts = 20240301;
        string s_symb_str(_s_symb);

        // holding
        row_t * row = new row_t(wl->t_holding);
        row->set_value(H_T_ID, &query->t_id);
        row->set_value(H_CA_ID, &ca_id);
        row->set_value(H_S_SYMB, _s_symb);
        row->set_value(H_DTS, &dts);
        row->set_value(H_PRICE, &price);
        row->set_value(H_QTY, &qty);
        get_cc_manager()->row_request(row, TAB_HOLDING, INS);

        key = holdingSummaryKey(ca_id, s_symb_str);
        get_cc_manager()->row_request(TAB_HOLDING_SUMMARY, IDX_HOLDING_SUMMARY, key, WR, 0, 1);

        key = accountKey(ca_id);
        get_cc_manager()->row_request(TAB_CUSTOMER_ACCOUNT, IDX_CUSTOMER_ACCOUNT, key, RD);

        _phase = 2;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 2) {
        string s_symb_str(_s_symb);
        key = holdingSummaryKey(_ca_id, s_symb_str);
        schema = wl->t_holding_summary->get_schema();
        _curr_data = get_cc_manager()->buffer_request(TAB_HOLDING_SUMMARY, IDX_HOLDING_SUMMARY, key, WR);

        // holding_summary
        if (_curr_data == NULL) {
            row_t * row = new row_t(wl->t_holding_summary);
            row->set_value(HS_CA_ID, &_ca_id);
            row->set_value(HS_S_SYMB, _s_symb);
            row->set_value(HS_QTY, &_qty);
            get_cc_manager()->row_request(row, TAB_HOLDING_SUMMARY, INS);
        } else {
            LOAD_VALUE(uint64_t, qty, schema, _curr_data, HS_QTY);
            qty += _qty;
            STORE_VALUE(qty, schema, _curr_data, HS_QTY);
        }

        key = accountKey(_ca_id);
        schema = wl->t_customer_account->get_schema();
        _curr_data = get_cc_manager()->buffer_request(TAB_CUSTOMER_ACCOUNT, IDX_CUSTOMER_ACCOUNT, key, RD);

        LOAD_VALUE(uint64_t, b_id, schema, _curr_data, CA_B_ID);
        _b_id = b_id;

        key = brokerKey(b_id);
        get_cc_manager()->row_request(TAB_BROKER, IDX_BROKER, key, WR);

        _phase = 3;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 3) {
        key = brokerKey(_b_id);
        schema = wl->t_broker->get_schema();
        _curr_data = get_cc_manager()->buffer_request(TAB_BROKER, IDX_BROKER, key, WR);

        LOAD_VALUE(double, comm_total, schema, _curr_data, B_COMM_TOTAL);
        LOAD_VALUE(uint64_t, num_trades, schema, _curr_data, B_NUM_TRADES);
        comm_total += (double) URand(100, 1000);
        num_trades++;
        STORE_VALUE(comm_total, schema, _curr_data, B_COMM_TOTAL);
        STORE_VALUE(num_trades, schema, _curr_data, B_NUM_TRADES);

        // TODO: Insert rows into Cash Transaction and Settlement

        _phase = 4;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    return RCOK;
}

RC TPCEStoreProcedure::execute_trade_status()
{
    RC rc = RCOK;
    uint64_t key;
    char * _curr_data;
    Catalog * schema = NULL;

    QueryTradeStatusTPCE * query = (QueryTradeStatusTPCE *) _query;
    WorkloadTPCE * wl = (WorkloadTPCE *) GET_WORKLOAD;

    if (_phase == 0) {
        key = accountKey(query->ca_id);
        get_cc_manager()->row_request(TAB_TRADE, IDX_TRADE_CA_ID, key, RD, 0, query->trade_num);

        _phase = 1;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 1) {
        key = accountKey(query->ca_id);
        schema = wl->t_trade->get_schema();
        vector<char *> rows;
        get_cc_manager()->buffer_request(rows, TAB_TRADE, IDX_TRADE_CA_ID, key, RD, 0, query->trade_num);

        for (auto it = rows.begin(); it != rows.end(); it++) {
            _curr_data = *it;

            __attribute__((unused)) LOAD_VALUE(uint64_t, t_id, schema, _curr_data, T_ID);
            __attribute__((unused)) LOAD_VALUE(uint64_t, qty, schema, _curr_data, T_QTY);
            __attribute__((unused)) char s_symb[15];
            memcpy(s_symb, row_t::get_value(schema, T_S_SYMB, _curr_data), schema->get_field_size(T_S_SYMB));
        }

        _phase = 2;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    return RCOK;
}

RC TPCEStoreProcedure::execute_trade_update()
{
    RC rc = RCOK;
    uint64_t key;
    char * _curr_data;
    Catalog * schema = NULL;

    QueryTradeUpdateTPCE * query = (QueryTradeUpdateTPCE *) _query;
    WorkloadTPCE * wl = (WorkloadTPCE *) GET_WORKLOAD;

    if (query->frame_to_exec == 1) {
        // First frame: update trades according to trade ids
        // Executor's names are modified
        if (_phase == 0) {
            for (uint64_t i = 0; i < query->trade_num; i++) {
                uint64_t t_id = query->t_id_list[i];
                key = tradeKey(t_id);
                get_cc_manager()->row_request(TAB_TRADE, IDX_TRADE, key, WR);
            }

            _phase = 1;
            rc = get_cc_manager()->process_requests();
            if (rc != RCOK) return rc;
        }

        if (_phase == 1) {
            char exec_name[50];
            MakeAlphaString(40, 50, exec_name);
            schema = wl->t_trade->get_schema();

            for (uint64_t i = 0; i < query->trade_num; i++) {
                uint64_t t_id = query->t_id_list[i];
                key = tradeKey(t_id);
                _curr_data = get_cc_manager()->buffer_request(TAB_TRADE, IDX_TRADE, key, WR);
                if (_curr_data == NULL) {
                    continue;
                }

                row_t::set_value(schema, T_EXEC_NAME, _curr_data, exec_name);
            }

            _phase = 2;
            rc = get_cc_manager()->process_requests();
            if (rc != RCOK) return rc;
        }
    } else if (query->frame_to_exec == 2) {
        // Second frame: update trades of a customer account id
        // The settlement cash type is modified
        if (_phase == 0) {
            key = accountKey(query->ca_id);
            get_cc_manager()->row_request(TAB_TRADE, IDX_TRADE_CA_ID, key, RD, 0, 50);

            _phase = 1;
            rc = get_cc_manager()->process_requests();
            if (rc != RCOK) return rc;
        }

        if (_phase == 1) {
            key = accountKey(query->ca_id);
            schema = wl->t_trade->get_schema();
            vector<char *> rows;
            get_cc_manager()->buffer_request(rows, TAB_TRADE, IDX_TRADE_CA_ID, key, RD, 0, 50);

            for (auto it = rows.begin(); it != rows.end(); it++) {
                _curr_data = *it;
                LOAD_VALUE(uint64_t, t_id, schema, _curr_data, T_ID);
                _t_id_list.emplace_back(t_id);
                key = tradeKey(t_id);
                get_cc_manager()->row_request(TAB_SETTLEMENT, IDX_SETTLEMENT, key, WR, 0, 1);
            }

            _phase = 2;
            rc = get_cc_manager()->process_requests();
            if (rc != RCOK) return rc;
        }

        if (_phase == 2) {
            char cash_type[40] = "Margin";
            schema = wl->t_settlement->get_schema();

            for (auto it = _t_id_list.begin(); it != _t_id_list.end(); it++) {
                uint64_t t_id = *it;
                key = tradeKey(t_id);
                _curr_data = get_cc_manager()->buffer_request(TAB_SETTLEMENT, IDX_SETTLEMENT, key, WR);
                if (_curr_data == NULL) {
                    continue;
                }

                row_t::set_value(schema, SE_CASH_TYPE, _curr_data, cash_type);
            }

            _phase = 3;
            rc = get_cc_manager()->process_requests();
            if (rc != RCOK) return rc;
        }
    } else if (query->frame_to_exec == 3) {
        // Third frame: update trades of a security symbol
        // The descriptions of cash trades are modified
        if (_phase == 0) {
            string s_symb_str(query->s_symb);
            key = securityKey(s_symb_str);
            get_cc_manager()->row_request(TAB_TRADE, IDX_TRADE_S_SYMB, key, RD, 0, 50);

            _phase = 1;
            rc = get_cc_manager()->process_requests();
            if (rc != RCOK) return rc;
        }

        if (_phase == 1) {
            string s_symb_str(query->s_symb);
            key = securityKey(s_symb_str);
            schema = wl->t_trade->get_schema();
            vector<char *> rows;
            get_cc_manager()->buffer_request(rows, TAB_TRADE, IDX_TRADE_S_SYMB, key, RD, 0, 50);

            for (auto it = rows.begin(); it != rows.end(); it++) {
                _curr_data = *it;
                LOAD_VALUE(uint64_t, t_id, schema, _curr_data, T_ID);
                _t_id_list.emplace_back(t_id);
                key = tradeKey(t_id);
                get_cc_manager()->row_request(TAB_CASH_TRANSACTION, IDX_CASH_TRANSACTION, key, WR, 0, 1);
            }

            _phase = 2;
            rc = get_cc_manager()->process_requests();
            if (rc != RCOK) return rc;
        }

        if (_phase == 2) {
            char ct_name[100];
            MakeAlphaString(20, 100, ct_name);
            schema = wl->t_cash_transaction->get_schema();

            for (auto it = _t_id_list.begin(); it != _t_id_list.end(); it++) {
                uint64_t t_id = *it;
                key = tradeKey(t_id);
                _curr_data = get_cc_manager()->buffer_request(TAB_CASH_TRANSACTION, IDX_CASH_TRANSACTION, key, WR);
                if (_curr_data == NULL) {
                    continue;
                }

                row_t::set_value(schema, CT_NAME, _curr_data, ct_name);
            }

            _phase = 3;
            rc = get_cc_manager()->process_requests();
            if (rc != RCOK) return rc;
        }
    }

    return RCOK;
}

void TPCEStoreProcedure::txn_abort()
{
    _ca_id = -1;
    _trade_request_list.clear();
    _is_cash = false;
    _qty = 0;
    memset(_s_symb, 0, 15);
    _b_id = 0;
    _t_id_list.clear();

    StoreProcedure::txn_abort();
}

#endif
