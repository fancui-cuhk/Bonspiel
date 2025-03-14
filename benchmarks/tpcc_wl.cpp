#include "global.h"
#include "helper.h"
#include "tpcc.h"
#include "workload.h"
#include "server_thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpcc_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "tpcc_const.h"
#include "tpcc_store_procedure.h"
#include "tpcc_query.h"
#include "manager.h"

#if WORKLOAD == TPCC

RC WorkloadTPCC::init()
{
    workload::init();
    string path = "./benchmarks/";
#if TPCC_SMALL
    path += "TPCC_short_schema.txt";
#else
    path += "TPCC_full_schema.txt";
#endif
    cout << "reading schema file: " << path << endl;
    init_schema( path.c_str() );
    cout << "TPCC schema initialized" << endl;
    next_tid = 0;
    init_table();
#if HOTNESS_ENABLE
    prepare_hotness_table();
#endif
    next_tid = 0;
    return RCOK;
}

RC WorkloadTPCC::init_schema(const char * schema_file)
{
    workload::init_schema(schema_file);

    t_warehouse = tables[TAB_WAREHOUSE];
    t_district = tables[TAB_DISTRICT];
    t_customer = tables[TAB_CUSTOMER];
    t_history = tables[TAB_HISTORY];
    t_neworder = tables[TAB_NEWORDER];
    t_order = tables[TAB_ORDER];
    t_orderline = tables[TAB_ORDERLINE];
    t_item = tables[TAB_ITEM];
    t_stock = tables[TAB_STOCK];

    i_item = indexes[IDX_ITEM];
    i_warehouse = indexes[IDX_WAREHOUSE];
    i_district = indexes[IDX_DISTRICT];
    i_customer_id = indexes[IDX_CUSTOMER_ID];
    i_customer_last = indexes[IDX_CUSTOMER_LAST];
    i_stock = indexes[IDX_STOCK];
    i_order = indexes[IDX_ORDER];
    i_order_cust = indexes[IDX_ORDER_CUST];
    i_orderline = indexes[IDX_ORDERLINE];
    i_neworder = indexes[IDX_NEWORDER];

    return RCOK;
}

void
WorkloadTPCC::table_to_indexes(uint32_t table_id, set<INDEX *> * indexes)
{
    switch(table_id) {
    case TAB_WAREHOUSE:
        indexes->insert(i_warehouse);
        return;
    case TAB_DISTRICT:
        indexes->insert(i_district);
        return;
    case TAB_CUSTOMER:
        indexes->insert(i_customer_id);
        indexes->insert(i_customer_last);
        return;
    case TAB_HISTORY:
        return;
    case TAB_NEWORDER:
        indexes->insert(i_neworder);
        return;
    case TAB_ORDER:
        indexes->insert(i_order);
        indexes->insert(i_order_cust);
        return;
    case TAB_ORDERLINE:
        indexes->insert(i_orderline);
        return;
    case TAB_ITEM:
        indexes->insert(i_item);
        return;
    case TAB_STOCK:
        indexes->insert(i_stock);
        return;
    default:
        assert(false);
    }
}

INDEX *
WorkloadTPCC::get_primary_index(uint32_t table_id)
{
    switch(table_id) {
    case TAB_WAREHOUSE:
        return i_warehouse;
    case TAB_DISTRICT:
        return i_district;
    case TAB_CUSTOMER:
        return i_customer_id;
    case TAB_HISTORY:
        return NULL;
    case TAB_NEWORDER:
        return i_neworder;
    case TAB_ORDER:
        return i_order;
    case TAB_ORDERLINE:
        return i_orderline;
    case TAB_ITEM:
        return i_item;
    case TAB_STOCK:
        return i_stock;
    default:
        assert(false);
    }
}

uint32_t
WorkloadTPCC::index_to_table(uint32_t index_id)
{
    switch (index_id) {
    case IDX_ITEM:
        return TAB_ITEM;
    case IDX_WAREHOUSE:
        return TAB_WAREHOUSE;
    case IDX_DISTRICT:
        return TAB_DISTRICT;
    case IDX_CUSTOMER_ID:
    case IDX_CUSTOMER_LAST:
        return TAB_CUSTOMER;
    case IDX_STOCK:
        return TAB_STOCK;
    case IDX_ORDER:
    case IDX_ORDER_CUST:
        return TAB_ORDER;
    case IDX_NEWORDER:
        return TAB_NEWORDER;
    case IDX_ORDERLINE:
        return TAB_ORDERLINE;
    default: {
        assert(false);
        return 0;
    }
    }
}

uint32_t
WorkloadTPCC::key_to_part(uint64_t key, uint32_t table_id)
{
    uint32_t num_wh = g_num_wh * g_num_parts;
    switch (table_id) {
    case TAB_WAREHOUSE:
        return TPCCHelper::wh_to_part( key );
    case TAB_DISTRICT:
        return TPCCHelper::wh_to_part( key % num_wh );
    case TAB_ITEM:
        // return TPCCHelper::item_to_part( key );
        return g_part_id;
    case TAB_STOCK:
        return TPCCHelper::wh_to_part( key % num_wh );
    case TAB_CUSTOMER:
        return TPCCHelper::wh_to_part( key % num_wh );
    case TAB_ORDER:
        return TPCCHelper::wh_to_part( key % num_wh );
    case TAB_ORDERLINE:
        return TPCCHelper::wh_to_part( key % num_wh );
    case TAB_NEWORDER:
        return TPCCHelper::wh_to_part( key % num_wh );
    }
    assert(false);
    return 0;
}

RC WorkloadTPCC::init_table() {
    num_wh = g_num_wh;

/******** fill in data ************/
// data filling process:
//- item
//- wh
//    - stock
//     - dist
//      - cust
//          - hist
//        - order
//        - new order
//        - order line
/**********************************/
    pthread_t * p_thds = new pthread_t[g_num_wh - 1];
    for (uint32_t i = 0; i < g_num_wh - 1; i++)
        pthread_create(&p_thds[i], NULL, threadInitWarehouse, this);
    threadInitWarehouse(this);
    for (uint32_t i = 0; i < g_num_wh - 1; i++)
        pthread_join(p_thds[i], NULL);

    printf("TPCC Data Initialization Complete!\n");
    return RCOK;
}

void WorkloadTPCC::init_tab_item() {
    for (uint64_t i = 1; i <= g_max_items; i++) {
#if !REPLICATE_ITEM_TABLE
        if (i % g_num_parts != g_part_id)
            continue;
#endif
        row_t * row;
        t_item->get_new_row(row, 0);
        row->set_value(I_ID, &i);
        uint64_t id = URand(1L, 10000L);
        row->set_value(I_IM_ID, &id);
        char name[24];
        MakeAlphaString(14, 24, name);
        row->set_value(I_NAME, name);
        id = URand(1, 100);
        row->set_value(I_PRICE, &id);
        char data[50];
        MakeAlphaString(26, 50, data);
        if (RAND(10) == 0)
            strcpy(data, "original");
        row->set_value(I_DATA, data);

        index_insert(i_item, i, row);
    }
}

void WorkloadTPCC::init_tab_wh(uint64_t wid) {
    assert( TPCCHelper::wh_to_part(wid) == g_part_id );
    row_t * row;
    t_warehouse->get_new_row(row, 0);

    row->set_value(W_ID, &wid);
    char name[10];
    MakeAlphaString(6, 10, name);
    row->set_value(W_NAME, name);
    char street[20];
    MakeAlphaString(10, 20, street);
    row->set_value(W_STREET_1, street);
    MakeAlphaString(10, 20, street);
    row->set_value(W_STREET_2, street);
    MakeAlphaString(10, 20, street);
    row->set_value(W_CITY, street);
    char state[3];
    MakeAlphaString(2, 3, state);
    row->set_value(W_STATE, state);
    char zip[10];
       MakeNumberString(9, 10, zip);
    row->set_value(W_ZIP, zip);
       double tax = (double)URand(0L, 200L) / 1000.0;
       double w_ytd = 300000.00;
    row->set_value(W_TAX, &tax);
    row->set_value(W_YTD, &w_ytd);

    index_insert(i_warehouse, wid, row);
    return;
}

void WorkloadTPCC::init_tab_dist(uint64_t wid) {
    for (uint64_t did = 0; did < DIST_PER_WARE; did++) {
        row_t * row;
        t_district->get_new_row(row, 0);

        row->set_value(D_ID, &did);
        row->set_value(D_W_ID, &wid);
        char name[10];
        MakeAlphaString(6, 10, name);
        row->set_value(D_NAME, name);
        char street[20];
        MakeAlphaString(10, 20, street);
        row->set_value(D_STREET_1, street);
        MakeAlphaString(10, 20, street);
        row->set_value(D_STREET_2, street);
        MakeAlphaString(10, 20, street);
        row->set_value(D_CITY, street);
        char state[3];
        MakeAlphaString(2, 3, state);
        row->set_value(D_STATE, state);
        char zip[10];
        MakeNumberString(9, 10, zip);
        row->set_value(D_ZIP, zip);
        double tax = (double)URand(0L, 200L) / 1000.0;
        double w_ytd=30000.00;
        row->set_value(D_TAX, &tax);
        row->set_value(D_YTD, &w_ytd);
        int64_t id = 3001;
        row->set_value(D_NEXT_O_ID, &id);

        index_insert(i_district, distKey(wid, did), row);
    }
}

void WorkloadTPCC::init_tab_stock(uint64_t wid) {
    for (uint64_t iid = 1; iid <= g_max_items; iid++) {
        row_t * row;
        t_stock->get_new_row(row, 0);
        row->set_value(S_I_ID, &iid);
        row->set_value(S_W_ID, &wid);
        int64_t quantity = URand(10, 100);
        int64_t remote_cnt = 0;
        row->set_value(S_QUANTITY, &quantity);
        row->set_value(S_REMOTE_CNT, &remote_cnt);
#if !TPCC_SMALL
        int64_t ytd = 0;
        int64_t order_cnt = 0;
        row->set_value(S_YTD, &ytd);
        row->set_value(S_ORDER_CNT, &order_cnt);
        char s_data[50];
        int len = MakeAlphaString(26, 50, s_data);
        if (rand() % 100 < 10) {
            int idx = URand(0, len - 8);
            strcpy(&s_data[idx], "original");
        }
        row->set_value(S_DATA, s_data);
#endif
        index_insert(i_stock, stockKey(wid, iid), row);
    }
}

void WorkloadTPCC::init_tab_cust(uint64_t did, uint64_t wid) {
    assert(g_cust_per_dist >= 1000);
    for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++) {
        row_t * row;
        t_customer->get_new_row(row, 0);

        row->set_value(C_ID, &cid);
        row->set_value(C_D_ID, &did);
        row->set_value(C_W_ID, &wid);
        char c_last[LASTNAME_LEN];
        if (cid <= 1000)
            Lastname(cid - 1, c_last);
        else
            Lastname(cid % 1000, c_last);
        row->set_value(C_LAST, c_last);
#if !TPCC_SMALL
        char tmp[3] = "OE";
        row->set_value(C_MIDDLE, tmp);
        char c_first[FIRSTNAME_LEN];
        MakeAlphaString(FIRSTNAME_MINLEN, sizeof(c_first), c_first);
        row->set_value(C_FIRST, c_first);
        char street[20];
        MakeAlphaString(10, 20, street);
        row->set_value(C_STREET_1, street);
        MakeAlphaString(10, 20, street);
        row->set_value(C_STREET_2, street);
        MakeAlphaString(10, 20, street);
        row->set_value(C_CITY, street);
        char state[3];
        MakeAlphaString(2, 3, state);
        row->set_value(C_STATE, state);
        char zip[10];
        MakeNumberString(9, 10, zip);
        row->set_value(C_ZIP, zip);
        char phone[17];
        MakeNumberString(16, 17, phone);
        row->set_value(C_PHONE, phone);
        int64_t since = 0;
        int64_t credit_lim = 50000;
        int64_t delivery_cnt = 0;
        row->set_value(C_SINCE, &since);
        row->set_value(C_CREDIT_LIM, &credit_lim);
        row->set_value(C_DELIVERY_CNT, &delivery_cnt);
        char c_data[500];
        MakeAlphaString(300, 500, c_data);
        row->set_value(C_DATA, c_data);
#endif
        if (RAND(10) == 0) {
            char tmp[] = "GC";
            row->set_value(C_CREDIT, tmp);
        } else {
            char tmp[] = "BC";
            row->set_value(C_CREDIT, tmp);
        }
        double discount = RAND(5000) / 10000.0;
        row->set_value(C_DISCOUNT, &discount);
        double balance = -10;
        double payment = 10;
        int64_t cnt = 1;
        row->set_value(C_BALANCE, &balance);
        row->set_value(C_YTD_PAYMENT, &payment);
        row->set_value(C_PAYMENT_CNT, &cnt);
        uint64_t key;
        key = custNPKey(c_last, did, wid);
        assert(key_to_part(key, TAB_CUSTOMER) == g_part_id);
        index_insert(i_customer_last, key, row);
        key = custKey(wid, did, cid);
        assert(key_to_part(key, TAB_CUSTOMER) == g_part_id);
        index_insert(i_customer_id, key, row);
    }
}

void WorkloadTPCC::init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id) {
    row_t * row;
    t_history->get_new_row(row, 0);
    row->set_value(H_C_ID, &c_id);
    row->set_value(H_C_D_ID, &d_id);
    row->set_value(H_D_ID, &d_id);
    row->set_value(H_C_W_ID, &w_id);
    row->set_value(H_W_ID, &w_id);
    int64_t date = 0;
    row->set_value(H_DATE, &date);
    double amount = 10;
    row->set_value(H_AMOUNT, &amount);
#if !TPCC_SMALL
    char h_data[24];
    MakeAlphaString(12, 24, h_data);
    row->set_value(H_DATA, h_data);
#endif
}

void WorkloadTPCC::init_tab_order(uint64_t did, uint64_t wid) {
    uint64_t perm[g_cust_per_dist];
    init_permutation(perm, wid); /* initialize permutation of customer numbers */
    for (uint64_t oid = 1; oid <= g_cust_per_dist; oid++) {
        row_t * row;
        t_order->get_new_row(row, 0);
        uint64_t o_ol_cnt = 1;
        uint64_t cid = perm[oid - 1];
        row->set_value(O_ID, &oid);
        row->set_value(O_C_ID, &cid);
        row->set_value(O_D_ID, &did);
        row->set_value(O_W_ID, &wid);
        uint64_t o_entry = 2013;
        row->set_value(O_ENTRY_D, &o_entry);
        int64_t id = (oid < 2101) ? URand(1, 10) : 0;

        row->set_value(O_CARRIER_ID, &id);

        o_ol_cnt = URand(5, 15);
        row->set_value(O_OL_CNT, &o_ol_cnt);
        int64_t all_local = 1;
        row->set_value(O_ALL_LOCAL, &all_local);

        uint64_t key = orderKey( wid, did, oid);
        index_insert(i_order, key, row);

        key = custKey(wid, did, cid);
        index_insert(i_order_cust, key, row);

        // ORDER-LINE
#if !TPCC_SMALL
        for (uint64_t ol = 1; ol <= o_ol_cnt; ol++) {
            t_orderline->get_new_row(row, 0);
            row->set_value(OL_O_ID, &oid);
            row->set_value(OL_D_ID, &did);
            row->set_value(OL_W_ID, &wid);
            row->set_value(OL_NUMBER, &ol);
            int64_t id = URand(1, 100000);
            row->set_value(OL_I_ID, &id);
            row->set_value(OL_SUPPLY_W_ID, &wid);
            double amount = 0;
            int64_t date = 0;
            if (oid < 2101) {
                row->set_value(OL_DELIVERY_D, &o_entry);
                row->set_value(OL_AMOUNT, &amount);
            } else {
                row->set_value(OL_DELIVERY_D, &date);
                amount = URand(1, 999999)/100.0;
                row->set_value(OL_AMOUNT, &amount);
            }
            int64_t quantity = 5;
            row->set_value(OL_QUANTITY, &quantity);
            char ol_dist_info[24];
            MakeAlphaString(20, 24, ol_dist_info);
            row->set_value(OL_DIST_INFO, ol_dist_info);

            uint64_t key = orderlineKey( wid, did, oid );
            index_insert(i_orderline, key, row);
        }
#endif
        // NEW ORDER
        if (oid > 2100) {
            t_neworder->get_new_row(row, 0);
            row->set_value(NO_O_ID, &oid);
            row->set_value(NO_D_ID, &did);
            row->set_value(NO_W_ID, &wid);

            uint64_t key = neworderKey(wid, did);
            index_insert(i_neworder, key, row);
        }
    }
}

void
WorkloadTPCC::init_permutation(uint64_t * perm_c_id, uint64_t wid) {
    uint32_t i;
    // Init with consecutive values
    for (i = 0; i < g_cust_per_dist; i++)
        perm_c_id[i] = i + 1;

    // shuffle
    for (i = 0; i < g_cust_per_dist - 1; i++) {
        uint64_t j = URand(i + 1, g_cust_per_dist - 1);
        uint64_t tmp = perm_c_id[i];
        perm_c_id[i] = perm_c_id[j];
        perm_c_id[j] = tmp;
    }
}

void *
WorkloadTPCC::threadInitWarehouse(void * This)
{
    WorkloadTPCC * wl = (WorkloadTPCC *) This;
    int tid = ATOM_FETCH_ADD(wl->next_tid, 1);
    uint32_t wid = g_num_wh * g_part_id + tid;
    assert((uint64_t) tid < g_num_wh);

    if (tid == 0)
        wl->init_tab_item();
    wl->init_tab_wh( wid );
    wl->init_tab_dist( wid );
    wl->init_tab_stock( wid );
    for (uint64_t did = 0; did < DIST_PER_WARE; did++) {
        wl->init_tab_cust(did, wid);
        wl->init_tab_order(did, wid);
        for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++)
            wl->init_tab_hist(cid, did, wid);
    }
    return NULL;
}

StoreProcedure *
WorkloadTPCC::create_store_procedure(TxnManager * txn, QueryBase * query)
{
    return new TPCCStoreProcedure(txn, query);
}

QueryBase *
WorkloadTPCC::deserialize_subquery(char * data)
{
    QueryTPCC * q = (QueryTPCC *) data;
    if (q->type == TPCC_NEW_ORDER) {
        QueryNewOrderTPCC * query = new QueryNewOrderTPCC(data);
        return query;
    } else if (q->type == TPCC_PAYMENT) {
        QueryPaymentTPCC * query = new QueryPaymentTPCC(data);
        return query;
    } else
        assert(false);
}

QueryBase *
WorkloadTPCC::gen_query(int is_mp)
{
    double x = glob_manager->rand_double();
    if (x < g_perc_payment)
        return new QueryPaymentTPCC(is_mp);
    x -= g_perc_payment;
    if (x < g_perc_new_order)
        return new QueryNewOrderTPCC(is_mp);
    x -= g_perc_new_order;
    if (x < g_perc_order_status)
        return new QueryOrderStatusTPCC();
    x -= g_perc_order_status;
    if (x < g_perc_delivery)
        return new QueryDeliveryTPCC();
    x -= g_perc_delivery;
    if (x < PERC_STOCKLEVEL)
        return new QueryStockLevelTPCC();

    assert(false);
    return NULL;
}

QueryBase *
WorkloadTPCC::clone_query(QueryBase * query)
{
    QueryTPCC * q = (QueryTPCC *) query;
    switch (q->type) {
    case TPCC_PAYMENT:
        return new QueryPaymentTPCC((QueryPaymentTPCC *) query);
    case TPCC_NEW_ORDER:
        return new QueryNewOrderTPCC((QueryNewOrderTPCC *) query);
    case TPCC_ORDER_STATUS:
        return new QueryOrderStatusTPCC((QueryOrderStatusTPCC *) query);
    case TPCC_DELIVERY:
        return new QueryDeliveryTPCC((QueryDeliveryTPCC *) query);
    case TPCC_STOCK_LEVEL:
        return new QueryStockLevelTPCC((QueryStockLevelTPCC *) query);
    default:
        assert(false);
    }
}

uint64_t
WorkloadTPCC::get_primary_key(row_t * row)
{
    table_t * table = row->get_table();
    int64_t wid, did, cid, oid, iid, olnum;
    switch (table->get_table_id()) {
    case TAB_WAREHOUSE: {
        row->get_value(W_ID, &wid);
        return wid;
    }
    case TAB_DISTRICT: {
        row->get_value(D_ID, &did);
        row->get_value(D_W_ID, &wid);
        return distKey(wid, did);
    }
    case TAB_CUSTOMER: {
        row->get_value(C_ID, &cid);
        row->get_value(C_D_ID, &did);
        row->get_value(C_W_ID, &wid);
        return custKey(wid, did, cid);
    }
    case TAB_HISTORY: {
        assert(false);
    }
    case TAB_NEWORDER: {
        row->get_value(NO_O_ID, &oid);
        row->get_value(NO_D_ID, &did);
        row->get_value(NO_W_ID, &wid);
        return orderKey(wid, did, oid);
    }
    case TAB_ORDER: {
        row->get_value(O_ID, &oid);
        row->get_value(O_D_ID, &did);
        row->get_value(O_W_ID, &wid);
        return orderKey(wid, did, oid);
    }
    case TAB_ORDERLINE: {
        row->get_value(OL_O_ID, &oid);
        row->get_value(OL_D_ID, &did);
        row->get_value(OL_W_ID, &wid);
        row->get_value(OL_NUMBER, &olnum);
        return orderlinePrimaryKey(wid, did, oid, olnum);
    }
    case TAB_ITEM: {
        row->get_value(I_ID, &iid);
        return iid;
    }
    case TAB_STOCK: {
        row->get_value(S_I_ID, &iid);
        row->get_value(S_W_ID, &wid);
        return stockKey(wid, iid);
    }
    default:
        assert(false);
    }
}

uint64_t
WorkloadTPCC::get_index_key(row_t * row, uint32_t index_id)
{
    int64_t wid, did, cid, oid;
    char * c_last;
    switch (index_id) {
    case IDX_ITEM:
    case IDX_WAREHOUSE:
    case IDX_DISTRICT:
    case IDX_CUSTOMER_ID:
    case IDX_STOCK:
    case IDX_ORDER:
        return get_primary_key(row);
    case IDX_NEWORDER:
        row->get_value(NO_W_ID, &wid);
        row->get_value(NO_D_ID, &did);
        return neworderKey(wid, did);
    case IDX_CUSTOMER_LAST:
        row->get_value(C_W_ID, &wid);
        row->get_value(C_D_ID, &did);
        c_last = row->get_value(C_LAST);
        return custNPKey(c_last, did, wid);
    case IDX_ORDER_CUST:
        row->get_value(O_W_ID, &wid);
        row->get_value(O_D_ID, &did);
        row->get_value(O_C_ID, &cid);
        return custKey(wid, did, cid);
    case IDX_ORDERLINE:
        row->get_value(OL_W_ID, &wid);
        row->get_value(OL_D_ID, &did);
        row->get_value(OL_O_ID, &oid);
        return orderlineKey(wid, did, oid);
    default:
        assert(false);
    }
}

#if HOTNESS_ENABLE
void
WorkloadTPCC::prepare_hotness_table()
{
    uint32_t num_table = 9;
    vector<uint64_t> num_row
    {
        g_num_wh,                                            // Warehouse
        g_num_wh * DIST_PER_WARE,                            // District
        g_num_wh * DIST_PER_WARE * g_cust_per_dist,          // Customer
        2 * g_num_wh * DIST_PER_WARE * g_cust_per_dist,      // History   [to be inserted]
        2 * g_num_wh * DIST_PER_WARE * g_cust_per_dist,      // New Order [to be inserted]
        2 * g_num_wh * DIST_PER_WARE * g_cust_per_dist,      // Order     [to be inserted]
        2 * g_num_wh * DIST_PER_WARE * g_cust_per_dist * 15, // Orderline [to be inserted]
        0,                                                   // Item
        g_num_wh * g_max_items,                              // Stock
    };
    glob_manager->resize_hotness_table(num_table, num_row);
}
#endif

#endif
