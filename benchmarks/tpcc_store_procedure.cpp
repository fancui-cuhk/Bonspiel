#include "tpcc.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "tpcc_const.h"
#include "tpcc_store_procedure.h"
#include "manager.h"
#include "cc_manager.h"
#include "row.h"
#include "table.h"
#include "index_base.h"
#include "catalog.h"

#if WORKLOAD == TPCC

TPCCStoreProcedure::TPCCStoreProcedure(TxnManager * txn_man, QueryBase * query)
    : StoreProcedure(txn_man, query)
{
    init();
}

TPCCStoreProcedure::~TPCCStoreProcedure()
{
}

void
TPCCStoreProcedure::init()
{
    StoreProcedure::init();
    _curr_step = 0;
    _curr_ol_number = 0;
    _curr_dist = 0;
    _ol_amount = 0;
    _ol_num = 0;
}

uint32_t
TPCCStoreProcedure::get_txn_type()
{
    return ((QueryTPCC *)_query)->type;
}

RC
TPCCStoreProcedure::execute()
{
    QueryTPCC * query = (QueryTPCC *) _query;
    assert(_txn->is_coord());
    switch (query->type) {
        case TPCC_PAYMENT:      return execute_payment();
        case TPCC_NEW_ORDER:    return execute_new_order();
        case TPCC_ORDER_STATUS: return execute_order_status();
        case TPCC_DELIVERY:     return execute_delivery();
        case TPCC_STOCK_LEVEL:  return execute_stock_level();
        default:                assert(false);
    }
}

RC
TPCCStoreProcedure::execute_payment()
{
    QueryPaymentTPCC * query = (QueryPaymentTPCC *) _query;
    WorkloadTPCC * wl = (WorkloadTPCC *) GET_WORKLOAD;

    RC rc = RCOK;
    uint64_t key;
    if (_phase == 0) {
        get_cc_manager()->row_request(TAB_WAREHOUSE, IDX_WAREHOUSE, query->w_id, WR);
        key = distKey(query->d_w_id, query->d_id);
        get_cc_manager()->row_request(TAB_DISTRICT, IDX_DISTRICT, key, WR);
        if (query->by_last_name) {
            key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
            get_cc_manager()->row_request(TAB_CUSTOMER, IDX_CUSTOMER_LAST, key, WR);
        } else {
            key = custKey(query->c_w_id, query->c_d_id, query->c_id);
            get_cc_manager()->row_request(TAB_CUSTOMER, IDX_CUSTOMER_ID, key, WR);
        }

        _phase = 1;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 1) {
        Catalog * schema = wl->t_warehouse->get_schema();
        _curr_data = get_cc_manager()->buffer_request(TAB_WAREHOUSE, IDX_WAREHOUSE, query->w_id, WR);
        LOAD_VALUE(double, w_ytd, schema, _curr_data, W_YTD);
        w_ytd += query->h_amount;
        STORE_VALUE(w_ytd, schema, _curr_data, W_YTD);
        __attribute__((unused)) LOAD_VALUE(char, w_name, schema, _curr_data, W_NAME);
        __attribute__((unused)) LOAD_VALUE(char, w_street_1, schema, _curr_data, W_STREET_1);
        __attribute__((unused)) LOAD_VALUE(char, w_street_2, schema, _curr_data, W_STREET_2);
        __attribute__((unused)) LOAD_VALUE(char, w_city, schema, _curr_data, W_CITY);
        __attribute__((unused)) LOAD_VALUE(char, w_state, schema, _curr_data, W_STATE);
        __attribute__((unused)) LOAD_VALUE(char, w_zip, schema, _curr_data, W_ZIP);

        schema = wl->t_district->get_schema();
        key = distKey(query->d_w_id, query->d_id);
        _curr_data = get_cc_manager()->buffer_request(TAB_DISTRICT, IDX_DISTRICT, key, WR);
        LOAD_VALUE(double, d_ytd, schema, _curr_data, D_YTD);
        d_ytd += query->h_amount;
        STORE_VALUE(d_ytd, schema, _curr_data, D_YTD);
        __attribute__((unused)) LOAD_VALUE(char, d_name, schema, _curr_data, D_NAME);
        __attribute__((unused)) LOAD_VALUE(char, d_street_1, schema, _curr_data, D_STREET_1);
        __attribute__((unused)) LOAD_VALUE(char, d_street_2, schema, _curr_data, D_STREET_2);
        __attribute__((unused)) LOAD_VALUE(char, d_city, schema, _curr_data, D_CITY);
        __attribute__((unused)) LOAD_VALUE(char, d_state, schema, _curr_data, D_STATE);
        __attribute__((unused)) LOAD_VALUE(char, d_zip, schema, _curr_data, D_ZIP);

        schema = wl->t_customer->get_schema();
        if (query->by_last_name) {
            key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
            _curr_data = get_cc_manager()->buffer_request(TAB_CUSTOMER, IDX_CUSTOMER_LAST, key, WR);
        } else {
            key = custKey(query->c_w_id, query->c_d_id, query->c_id);
            _curr_data = get_cc_manager()->buffer_request(TAB_CUSTOMER, IDX_CUSTOMER_ID, key, WR);
        }

        LOAD_VALUE(double, c_balance, schema, _curr_data, C_BALANCE);
        c_balance -= query->h_amount;
        STORE_VALUE(c_balance, schema, _curr_data, C_BALANCE);

        LOAD_VALUE(double, c_ytd_payment, schema, _curr_data, C_YTD_PAYMENT);
        c_ytd_payment -= query->h_amount;
        STORE_VALUE(c_ytd_payment, schema, _curr_data, C_YTD_PAYMENT);

        LOAD_VALUE(uint64_t, c_payment_cnt, schema, _curr_data, C_PAYMENT_CNT);
        c_payment_cnt += 1;
        STORE_VALUE(c_payment_cnt, schema, _curr_data, C_PAYMENT_CNT);

        __attribute__((unused)) LOAD_VALUE(char, c_first, schema, _curr_data, C_FIRST);
        __attribute__((unused)) LOAD_VALUE(char, c_middle, schema, _curr_data, C_MIDDLE);
        __attribute__((unused)) LOAD_VALUE(char, c_street_1, schema, _curr_data, C_STREET_1);
        __attribute__((unused)) LOAD_VALUE(char, c_street_2, schema, _curr_data, C_STREET_2);
        __attribute__((unused)) LOAD_VALUE(char, c_city, schema, _curr_data, C_CITY);
        __attribute__((unused)) LOAD_VALUE(char, c_state, schema, _curr_data, C_STATE);
        __attribute__((unused)) LOAD_VALUE(char, c_zip, schema, _curr_data, C_ZIP);
        __attribute__((unused)) LOAD_VALUE(char, c_phone, schema, _curr_data, C_PHONE);
        __attribute__((unused)) LOAD_VALUE(int64_t, c_since, schema, _curr_data, C_SINCE);
        __attribute__((unused)) LOAD_VALUE(char, c_credit, schema, _curr_data, C_CREDIT);
        __attribute__((unused)) LOAD_VALUE(int64_t, c_credit_lim, schema, _curr_data, C_CREDIT_LIM);
        __attribute__((unused)) LOAD_VALUE(int64_t, c_discount, schema, _curr_data, C_DISCOUNT);
        
        _phase = 2;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }
    
    return RCOK;
}

RC
TPCCStoreProcedure::execute_new_order()
{
    RC rc = RCOK;
    uint64_t key;
    // itemid_t * item;
    Catalog * schema = NULL;
    char * _curr_data;

    QueryNewOrderTPCC * query = (QueryNewOrderTPCC *) _query;
    WorkloadTPCC * wl = (WorkloadTPCC *) GET_WORKLOAD;

    uint64_t w_id = query->w_id;
    uint64_t d_id = query->d_id;
    uint64_t c_id = query->c_id;
    uint64_t ol_cnt = query->ol_cnt;
    Item_no * items = query->items;

    if (_phase == 0) {
        get_cc_manager()->row_request(TAB_WAREHOUSE, IDX_WAREHOUSE, w_id, RD);
        key = custKey(w_id, d_id, c_id);
        get_cc_manager()->row_request(TAB_CUSTOMER, IDX_CUSTOMER_ID, key, RD);
        key = distKey(w_id, d_id);
        get_cc_manager()->row_request(TAB_DISTRICT, IDX_DISTRICT, key, WR);

        for (_curr_ol_number = 0; _curr_ol_number < ol_cnt; _curr_ol_number ++) {
            key = items[_curr_ol_number].ol_i_id;
            if (key == 0) {
                _self_abort = true;
                return ABORT;
            }
            get_cc_manager()->row_request(TAB_ITEM, IDX_ITEM, key, RD, 0, 1);
        }

        for (_curr_ol_number = 0; _curr_ol_number < ol_cnt; _curr_ol_number ++) {
            Item_no & it = items[_curr_ol_number];
            key = stockKey(it.ol_supply_w_id, it.ol_i_id);
            get_cc_manager()->row_request(TAB_STOCK, IDX_STOCK, key, WR);
        }

        _phase = 1;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    } 

    if (_phase == 1) {
        schema = wl->t_warehouse->get_schema();
        _curr_data = get_cc_manager()->buffer_request(TAB_WAREHOUSE, IDX_WAREHOUSE, w_id, RD);
        LOAD_VALUE(double, w_tax, schema, _curr_data, W_TAX);
        _w_tax = w_tax;

        schema = wl->t_customer->get_schema();
        key = custKey(w_id, d_id, c_id);
        _curr_data = get_cc_manager()->buffer_request(TAB_CUSTOMER, IDX_CUSTOMER_ID, key, RD);
        LOAD_VALUE(uint64_t, c_discount, schema, _curr_data, C_DISCOUNT);
        _c_discount = c_discount;
        __attribute__((unused)) LOAD_VALUE(char, c_last, schema, _curr_data, C_LAST);
        __attribute__((unused)) LOAD_VALUE(char, c_credit, schema, _curr_data, C_CREDIT);

        key = distKey(w_id, d_id);
        schema = wl->t_district->get_schema();
        _curr_data = get_cc_manager()->buffer_request(TAB_DISTRICT, IDX_DISTRICT, key, WR);
        LOAD_VALUE(double, d_tax, schema, _curr_data, D_TAX);
        _d_tax = d_tax;
        LOAD_VALUE(int64_t, o_id, schema, _curr_data, D_NEXT_O_ID);
        _o_id = o_id;;
        o_id ++;
        STORE_VALUE(o_id, schema, _curr_data, D_NEXT_O_ID);
        
        key = orderKey(w_id, d_id, _o_id);
        schema = wl->t_order->get_schema();
        row_t * row = new row_t(wl->t_order);

        row->set_value(O_ID, &_o_id);
        row->set_value(O_C_ID, &c_id);
        row->set_value(O_D_ID, &d_id);
        row->set_value(O_W_ID, &w_id);
        row->set_value(O_ENTRY_D, &query->o_entry_d);
        row->set_value(O_OL_CNT, &ol_cnt);
        int64_t all_local = (query->remote? 0 : 1);
        row->set_value(O_ALL_LOCAL, &all_local);
        get_cc_manager()->row_request(row, TAB_ORDER, INS);

        key = neworderKey(w_id, d_id);
        schema = wl->t_neworder->get_schema();
        row = new row_t (wl->t_neworder);
        row->set_value(NO_O_ID, &_o_id);
        row->set_value(NO_D_ID, &d_id);
        row->set_value(NO_W_ID, &w_id);
        rc = get_cc_manager()->row_request(row, TAB_NEWORDER, INS);
        schema = wl->t_item->get_schema();
        
        for (_curr_ol_number = 0; _curr_ol_number < ol_cnt; _curr_ol_number ++) {
            key = items[_curr_ol_number].ol_i_id;
            vector<char *> results;
            get_cc_manager()->buffer_request(results, TAB_ITEM, IDX_ITEM, key, RD, 0, 1);
            if (results.empty()) {
                _self_abort = true;
                return ABORT;
            } else
                _curr_data = results[0];
            __attribute__((unused)) LOAD_VALUE(int64_t, i_price, schema, _curr_data, I_PRICE);
            _i_price[_curr_ol_number] = i_price;
            __attribute__((unused)) LOAD_VALUE(char, i_name, schema, _curr_data, I_NAME);
            __attribute__((unused)) LOAD_VALUE(char, i_data, schema, _curr_data, I_DATA);
        }
         
        for (_curr_ol_number = 0; _curr_ol_number < ol_cnt; _curr_ol_number ++) {
            Item_no * it = &items[_curr_ol_number];
            key = stockKey(it->ol_supply_w_id, it->ol_i_id);
            schema = wl->t_stock->get_schema();
            char * _curr_data = get_cc_manager()->buffer_request(TAB_STOCK, IDX_STOCK, key, WR);

            LOAD_VALUE(uint64_t, s_quantity, schema, _curr_data, S_QUANTITY);
            if (s_quantity > it->ol_quantity + 10)
                s_quantity = s_quantity - it->ol_quantity;
            else
                s_quantity = s_quantity - it->ol_quantity + 91;
            STORE_VALUE(s_quantity, schema, _curr_data, S_QUANTITY);
            __attribute__((unused)) LOAD_VALUE(int64_t, s_remote_cnt, schema, _curr_data, S_REMOTE_CNT);
#if !TPCC_SMALL
                LOAD_VALUE(int64_t, s_ytd, schema, _curr_data, S_YTD);
                s_ytd += it->ol_quantity;
                STORE_VALUE(s_ytd, schema, _curr_data, S_YTD);

                LOAD_VALUE(int64_t, s_order_cnt, schema, _curr_data, S_ORDER_CNT);
                s_order_cnt ++;
                STORE_VALUE(s_order_cnt, schema, _curr_data, S_ORDER_CNT);

                __attribute__((unused)) LOAD_VALUE(char, s_data, schema, _curr_data, S_DATA);
#endif
            if (it->ol_supply_w_id != w_id) {
                LOAD_VALUE(int64_t, s_remote_cnt, schema, _curr_data, S_REMOTE_CNT);
                s_remote_cnt ++;
                STORE_VALUE(s_remote_cnt, schema, _curr_data, S_REMOTE_CNT);
            }
        }

        for (_ol_num = 0; _ol_num < (int64_t)ol_cnt; _ol_num ++) {
            Item_no * it = &items[_ol_num];
            // all rows (local or remote) are inserted locally.
            double ol_amount = it->ol_quantity * _i_price[_ol_num] * (1 + _w_tax + _d_tax) * (1 - _c_discount);
            schema = wl->t_orderline->get_schema();

            row_t * row = new row_t(wl->t_orderline);
            row->set_value(OL_O_ID, &_o_id);
            row->set_value(OL_D_ID, &d_id);
            row->set_value(OL_W_ID, &w_id);
            row->set_value(OL_NUMBER, &_ol_num);
            row->set_value(OL_I_ID, &it->ol_i_id);
            row->set_value(OL_SUPPLY_W_ID, &it->ol_supply_w_id);
            row->set_value(OL_QUANTITY, &it->ol_quantity);
            row->set_value(OL_AMOUNT, &ol_amount);
            rc = get_cc_manager()->row_request(row, TAB_ORDERLINE, INS);
        }

        _phase = 2;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }
    
    return RCOK;
}

RC
TPCCStoreProcedure::execute_order_status()
{
    RC rc = RCOK;
    uint64_t key;
    char * _curr_data;
    Catalog * schema = NULL;

    QueryOrderStatusTPCC * query = (QueryOrderStatusTPCC *) _query;
    WorkloadTPCC * wl = (WorkloadTPCC *) GET_WORKLOAD;

    if (_phase == 0) {
        if (query->by_last_name) {
            key = custNPKey(query->c_last, query->d_id, query->w_id);
            get_cc_manager()->row_request(TAB_CUSTOMER, IDX_CUSTOMER_LAST, key, RD);
        } else {
            key = custKey(query->w_id, query->d_id, query->c_id);
            get_cc_manager()->row_request(TAB_CUSTOMER, IDX_CUSTOMER_ID, key, RD);
        }

        _phase = 1;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 1) {
        schema = wl->t_customer->get_schema();
        if (query->by_last_name) {
            key = custNPKey(query->c_last, query->d_id, query->w_id);
            _curr_data = get_cc_manager()->buffer_request(TAB_CUSTOMER, IDX_CUSTOMER_LAST, key, WR);
        } else {
            key = custKey(query->w_id, query->d_id, query->c_id);
            _curr_data = get_cc_manager()->buffer_request(TAB_CUSTOMER, IDX_CUSTOMER_ID, key, WR);
        }

        LOAD_VALUE(uint64_t, c_id, schema, _curr_data, C_ID);
        __attribute__((unused)) LOAD_VALUE(double, c_balance, schema, _curr_data, C_BALANCE);
        __attribute__((unused)) LOAD_VALUE(char, c_first, schema, _curr_data, C_FIRST);
        __attribute__((unused)) LOAD_VALUE(char, c_middle, schema, _curr_data, C_MIDDLE);
        __attribute__((unused)) LOAD_VALUE(char, c_last, schema, _curr_data, C_LAST);

        query->c_id = c_id;
        key = custKey(query->w_id, query->d_id, query->c_id);
        get_cc_manager()->row_request(TAB_ORDER, IDX_ORDER_CUST, key, RD);

        _phase = 2;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 2) {
        schema = wl->t_order->get_schema();
        key = custKey(query->w_id, query->d_id, query->c_id);
        _curr_data = get_cc_manager()->buffer_request(TAB_ORDER, IDX_ORDER_CUST, key, RD);
        if (_curr_data == NULL) {
            // No existing order for this customer, return
            return RCOK;
        }
        
        LOAD_VALUE(int64_t, o_id, schema, _curr_data, O_ID);
        __attribute__((unused)) LOAD_VALUE(int64_t, o_entry_d, schema, _curr_data, O_ENTRY_D);
        __attribute__((unused)) LOAD_VALUE(int64_t, o_carrier_id, schema, _curr_data, O_CARRIER_ID);

        query->o_id = o_id;
        key = orderlineKey(query->w_id, query->d_id, query->o_id);
        get_cc_manager()->row_request(TAB_ORDERLINE, IDX_ORDERLINE, key, RD, 1, 15);

        _phase = 3;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 3) {
        schema = wl->t_orderline->get_schema();
        key = orderlineKey(query->w_id, query->d_id, query->o_id);
        vector<char *> rows;
        get_cc_manager()->buffer_request(rows, TAB_ORDERLINE, IDX_ORDERLINE, key, RD, 1, 15);
        for (auto it = rows.begin(); it != rows.end(); it++) {
            _curr_data = *it;
            __attribute__((unused)) LOAD_VALUE(int64_t, ol_i_id, schema, _curr_data, OL_I_ID);
            __attribute__((unused)) LOAD_VALUE(int64_t, ol_supply_w_id, schema, _curr_data, OL_SUPPLY_W_ID);
            __attribute__((unused)) LOAD_VALUE(int64_t, ol_quantity, schema, _curr_data, OL_QUANTITY);
            __attribute__((unused)) LOAD_VALUE(double, ol_amount, schema, _curr_data, OL_AMOUNT);
            __attribute__((unused)) LOAD_VALUE(int64_t, ol_delivery_d, schema, _curr_data, OL_DELIVERY_D);
        }

        _phase = 4;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    return RCOK;
}

RC
TPCCStoreProcedure::execute_delivery()
{
    RC rc = RCOK;
    uint64_t key;
    char * _curr_data;
    Catalog * schema = NULL;

    QueryDeliveryTPCC * query = (QueryDeliveryTPCC *) _query;
    WorkloadTPCC * wl = (WorkloadTPCC *) GET_WORKLOAD;

    if (_phase == 0) {
        for (_curr_dist = 0; _curr_dist != DIST_PER_WARE; _curr_dist ++) {
            key = neworderKey(query->w_id, _curr_dist);
            // Read and delete the row in the new order table
            // The system does not guarantee that the fetched entry corresponds to the oldest order
            get_cc_manager()->row_request(TAB_NEWORDER, IDX_NEWORDER, key, RD, 1, 1);
            get_cc_manager()->row_request(TAB_NEWORDER, IDX_NEWORDER, key, DEL, 1, 1);
        }

        _phase = 1;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 1) {
        for (_curr_dist = 0; _curr_dist != DIST_PER_WARE; _curr_dist ++) {
            schema = wl->t_neworder->get_schema();
            key = neworderKey(query->w_id, _curr_dist);
            _curr_data = get_cc_manager()->buffer_request(TAB_NEWORDER, IDX_NEWORDER, key, RD);
            if (_curr_data == NULL) {
                // The current district has no order yet
                continue;
            }
            
            LOAD_VALUE(int64_t, o_id, schema, _curr_data, NO_O_ID);
            query->o_id_list.emplace_back(o_id);

            key = orderKey(query->w_id, _curr_dist, o_id);
            get_cc_manager()->row_request(TAB_ORDER, IDX_ORDER, key, WR);
        }

        _phase = 2;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 2) {
        for (_curr_dist = 0; _curr_dist != DIST_PER_WARE; _curr_dist ++) {
            schema = wl->t_order->get_schema();
            key = orderKey(query->w_id, _curr_dist, query->o_id_list[_curr_dist]);
            _curr_data = get_cc_manager()->buffer_request(TAB_ORDER, IDX_ORDER, key, WR);
            if (_curr_data == NULL) {
                // The current district has no order yet
                continue;
            }

            LOAD_VALUE(int64_t, o_c_id, schema, _curr_data, O_C_ID);
            query->c_id_list.emplace_back(o_c_id);
            STORE_VALUE(query->o_carrier_id, schema, _curr_data, O_CARRIER_ID);

            key = orderlineKey(query->w_id, _curr_dist, query->o_id_list[_curr_dist]);
            get_cc_manager()->row_request(TAB_ORDERLINE, IDX_ORDERLINE, key, RD, 1, 15);
        }

        _phase = 3;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 3) {
        for (_curr_dist = 0; _curr_dist != DIST_PER_WARE; _curr_dist ++) {
            schema = wl->t_orderline->get_schema();
            key = orderlineKey(query->w_id, _curr_dist, query->o_id_list[_curr_dist]);

            _ol_amount = 0;
            vector<char *> rows;
            get_cc_manager()->buffer_request(rows, TAB_ORDERLINE, IDX_ORDERLINE, key, RD, 1, 15);
            for (auto it = rows.begin(); it != rows.end(); it++) {
                _curr_data = *it;
                LOAD_VALUE(double, ol_amount, schema, _curr_data, OL_AMOUNT);
                _ol_amount += ol_amount;
            }
            query->ol_amount_list.emplace_back(_ol_amount);

            key = custKey(query->w_id, _curr_dist, query->c_id_list[_curr_dist]);
            get_cc_manager()->row_request(TAB_CUSTOMER, IDX_CUSTOMER_ID, key, WR);
        }

        _phase = 4;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 4) {
        for (_curr_dist = 0; _curr_dist != DIST_PER_WARE; _curr_dist ++) {
            schema = wl->t_customer->get_schema();
            key = custKey(query->w_id, _curr_dist, query->c_id_list[_curr_dist]);
            _curr_data = get_cc_manager()->buffer_request(TAB_CUSTOMER, IDX_CUSTOMER_ID, key, WR);
            
            LOAD_VALUE(double, c_balance, schema, _curr_data, C_BALANCE);
            c_balance += query->ol_amount_list[_curr_dist];
            STORE_VALUE(c_balance, schema, _curr_data, C_BALANCE);

            LOAD_VALUE(int64_t, c_delivery_cnt, schema, _curr_data, C_DELIVERY_CNT);
            c_delivery_cnt ++;
            STORE_VALUE(c_delivery_cnt, schema, _curr_data, C_DELIVERY_CNT);
        }

        _phase = 5;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    return RCOK;
}

RC
TPCCStoreProcedure::execute_stock_level()
{
    RC rc = RCOK;
    uint64_t key;
    char * _curr_data;
    Catalog * schema = NULL;

    QueryStockLevelTPCC * query = (QueryStockLevelTPCC *) _query;
    WorkloadTPCC * wl = (WorkloadTPCC *) GET_WORKLOAD;

    if (_phase == 0) {
        key = distKey(query->w_id, query->d_id);
        get_cc_manager()->row_request(TAB_DISTRICT, IDX_DISTRICT, key, RD);

        _phase = 1;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 1) {
        schema = wl->t_district->get_schema();
        key = distKey(query->w_id, query->d_id);
        _curr_data = get_cc_manager()->buffer_request(TAB_DISTRICT, IDX_DISTRICT, key, RD);

        LOAD_VALUE(int64_t, o_id, schema, _curr_data, D_NEXT_O_ID);
        _o_id = o_id;
        _curr_ol_number = _o_id - 20;

        for (; _curr_ol_number < _o_id; _curr_ol_number ++) {
            key = orderlineKey(query->w_id, query->d_id, _curr_ol_number);
            get_cc_manager()->row_request(TAB_ORDERLINE, IDX_ORDERLINE, key, RD, 1, 15);
        }

        _phase = 2;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 2) {
        schema = wl->t_orderline->get_schema();
        for (; _curr_ol_number < _o_id; _curr_ol_number ++) {
            key = orderlineKey(query->w_id, query->d_id, _curr_ol_number);
            vector<char *> rows;
            get_cc_manager()->buffer_request(rows, TAB_ORDERLINE, IDX_ORDERLINE, key, RD, 1, 15);
            for (auto it = rows.begin(); it != rows.end(); it++) {
                _curr_data = *it;
                LOAD_VALUE(int64_t, i_id, schema, _curr_data, OL_I_ID);
                _items.insert(i_id);
            }
        }

        for (auto it = _items.begin(); it != _items.end(); it++) {
            key = stockKey(query->w_id, *it);
            get_cc_manager()->row_request(TAB_STOCK, IDX_STOCK, key, RD);
        }

        _phase = 3;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    if (_phase == 3) {
        schema = wl->t_stock->get_schema();
        for (auto it = _items.begin(); it != _items.end(); it++) {
            key = stockKey(query->w_id, *it);
            _curr_data = get_cc_manager()->buffer_request(TAB_STOCK, IDX_STOCK, key, RD);
            __attribute__((unused)) LOAD_VALUE(int64_t, s_quantity, schema, _curr_data, S_QUANTITY);
        }

        _phase = 4;
        rc = get_cc_manager()->process_requests();
        if (rc != RCOK) return rc;
    }

    return RCOK;
}

void
TPCCStoreProcedure::txn_abort()
{
    _curr_step = 0;
    _curr_ol_number = 0;

    _curr_dist = 0;
    _ol_amount = 0;
    _ol_num = 0;

    StoreProcedure::txn_abort();
}

#endif