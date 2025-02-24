#include "global.h"
#include "helper.h"
#include "tpce.h"
#include "workload.h"
#include "server_thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpce_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "tpce_const.h"
#include "tpce_store_procedure.h"
#include "tpce_query.h"
#include "manager.h"

#if WORKLOAD == TPCE

RC WorkloadTPCE::init()
{
    workload::init();
    string path = "./benchmarks/TPCE_schema.txt";
    cout << "reading schema file: " << path << endl;
    init_schema(path.c_str());
    cout << "TPCE schema initialized" << endl;
    // The database is not partitioned, no distributed txn
    if (g_num_parts == 1) {
        g_tpce_perc_remote = 0;
    }
    init_table();
#if HOTNESS_ENABLE
    prepare_hotness_table();
#endif
    _t_id = g_init_trade_num + 1;
    while (_t_id % g_num_parts != g_part_id) {
        _t_id++;
    }
    _t_id_trade_result = 1;
    while (_t_id_trade_result % g_num_parts != g_part_id) {
        _t_id_trade_result++;
    }
    return RCOK;
}

RC WorkloadTPCE::init_schema(const char * schema_file)
{
    workload::init_schema(schema_file);

    t_account_permission = tables[TAB_ACCOUNT_PERMISSION];
    t_customer = tables[TAB_CUSTOMER];
    t_customer_account = tables[TAB_CUSTOMER_ACCOUNT];
    t_customer_taxrate = tables[TAB_CUSTOMER_TAXRATE];
    t_holding = tables[TAB_HOLDING];
    t_holding_history = tables[TAB_HOLDING_HISTORY];
    t_holding_summary = tables[TAB_HOLDING_SUMMARY];
    t_watch_item = tables[TAB_WATCH_ITEM];
    t_watch_list = tables[TAB_WATCH_LIST];
    t_broker = tables[TAB_BROKER];
    t_cash_transaction = tables[TAB_CASH_TRANSACTION];
    t_charge = tables[TAB_CHARGE];
    t_commission_rate = tables[TAB_COMMISSION_RATE];
    t_settlement = tables[TAB_SETTLEMENT];
    t_trade = tables[TAB_TRADE];
    t_trade_history = tables[TAB_TRADE_HISTORY];
    t_trade_request = tables[TAB_TRADE_REQUEST];
    t_trade_type = tables[TAB_TRADE_TYPE];
    t_company = tables[TAB_COMPANY];
    t_company_competitor = tables[TAB_COMPANY_COMPETITOR];
    t_daily_market = tables[TAB_DAILY_MARKET];
    t_exchange = tables[TAB_EXCHANGE];
    t_financial = tables[TAB_FINANCIAL];
    t_industry = tables[TAB_INDUSTRY];
    t_last_trade = tables[TAB_LAST_TRADE];
    t_news_item = tables[TAB_NEWS_ITEM];
    t_news_xref = tables[TAB_NEWS_XREF];
    t_sector = tables[TAB_SECTOR];
    t_security = tables[TAB_SECURITY];
    t_address = tables[TAB_ADDRESS];
    t_status_type = tables[TAB_STATUS_TYPE];
    t_taxrate = tables[TAB_TAXRATE];
    t_zip_code = tables[TAB_ZIP_CODE];

    i_account_permission = indexes[IDX_ACCOUNT_PERMISSION];
    i_customer = indexes[IDX_CUSTOMER];
    i_customer_account = indexes[IDX_CUSTOMER_ACCOUNT];
    i_customer_account_c_id = indexes[IDX_CUSTOMER_ACCOUNT_C_ID];
    i_customer_account_b_id = indexes[IDX_CUSTOMER_ACCOUNT_B_ID];
    i_customer_taxrate = indexes[IDX_CUSTOMER_TAXRATE];
    i_holding = indexes[IDX_HOLDING];
    i_holding_ca_id = indexes[IDX_HOLDING_CA_ID];
    i_holding_s_symb = indexes[IDX_HOLDING_S_SYMB];
    i_holding_history = indexes[IDX_HOLDING_HISTORY];
    i_holding_summary = indexes[IDX_HOLDING_SUMMARY];
    i_watch_item = indexes[IDX_WATCH_ITEM];
    i_watch_list = indexes[IDX_WATCH_LIST];
    i_broker = indexes[IDX_BROKER];
    i_cash_transaction = indexes[IDX_CASH_TRANSACTION];
    i_charge = indexes[IDX_CHARGE];
    i_commission_rate = indexes[IDX_COMMISSION_RATE];
    i_settlement = indexes[IDX_SETTLEMENT];
    i_trade = indexes[IDX_TRADE];
    i_trade_ca_id = indexes[IDX_TRADE_CA_ID];
    i_trade_s_symb = indexes[IDX_TRADE_S_SYMB];
    i_trade_history = indexes[IDX_TRADE_HISTORY];
    i_trade_request = indexes[IDX_TRADE_REQUEST];
    i_trade_request_b_id = indexes[IDX_TRADE_REQUEST_B_ID];
    i_trade_request_s_symb = indexes[IDX_TRADE_REQUEST_S_SYMB];
    i_trade_type = indexes[IDX_TRADE_TYPE];
    i_company = indexes[IDX_COMPANY];
    i_company_competitor = indexes[IDX_COMPANY_COMPETITOR];
    i_daily_market = indexes[IDX_DAILY_MARKET];
    i_exchange = indexes[IDX_EXCHANGE];
    i_financial = indexes[IDX_FINANCIAL];
    i_industry = indexes[IDX_INDUSTRY];
    i_last_trade = indexes[IDX_LAST_TRADE];
    i_news_item = indexes[IDX_NEWS_ITEM];
    i_news_xref = indexes[IDX_NEWS_XREF];
    i_sector = indexes[IDX_SECTOR];
    i_security = indexes[IDX_SECURITY];
    i_address = indexes[IDX_ADDRESS];
    i_status_type = indexes[IDX_STATUS_TYPE];
    i_taxrate = indexes[IDX_TAXRATE];
    i_zip_code = indexes[IDX_ZIP_CODE];

    return RCOK;
}

// This function returns the next trade id for Trade-Order transactions
uint64_t WorkloadTPCE::next_t_id()
{
    return ATOM_FETCH_ADD(_t_id, g_num_parts);
}

// This function returns the next trade id for Trade-Result transactions
// If there is no unprocessed trades, this function return -1
int64_t WorkloadTPCE::next_t_id_trade_result()
{
    if (_t_id > _t_id_trade_result)
        return ATOM_FETCH_ADD(_t_id_trade_result, g_num_parts);
    else
        return -1;
}

void WorkloadTPCE::table_to_indexes(uint32_t table_id, set<INDEX *> * indexes)
{
    switch (table_id) {
    case TAB_ACCOUNT_PERMISSION:
        indexes->insert(i_account_permission);
        return;
    case TAB_CUSTOMER:
        indexes->insert(i_customer);
        return;
    case TAB_CUSTOMER_ACCOUNT:
        indexes->insert(i_customer_account);
        indexes->insert(i_customer_account_c_id);
        indexes->insert(i_customer_account_b_id);
        return;
    case TAB_CUSTOMER_TAXRATE:
        indexes->insert(i_customer_taxrate);
        return;
    case TAB_HOLDING:
        indexes->insert(i_holding);
        indexes->insert(i_holding_ca_id);
        indexes->insert(i_holding_s_symb);
        return;
    case TAB_HOLDING_HISTORY:
        indexes->insert(i_holding_history);
        return;
    case TAB_HOLDING_SUMMARY:
        indexes->insert(i_holding_summary);
        return;
    case TAB_WATCH_ITEM:
        indexes->insert(i_watch_item);
        return;
    case TAB_WATCH_LIST:
        indexes->insert(i_watch_list);
        return;
    case TAB_BROKER:
        indexes->insert(i_broker);
        return;
    case TAB_CASH_TRANSACTION:
        indexes->insert(i_cash_transaction);
        return;
    case TAB_CHARGE:
        indexes->insert(i_charge);
        return;
    case TAB_COMMISSION_RATE:
        indexes->insert(i_commission_rate);
        return;
    case TAB_SETTLEMENT:
        indexes->insert(i_settlement);
        return;
    case TAB_TRADE:
        indexes->insert(i_trade);
        indexes->insert(i_trade_ca_id);
        indexes->insert(i_trade_s_symb);
        return;
    case TAB_TRADE_HISTORY:
        indexes->insert(i_trade_history);
        return;
    case TAB_TRADE_REQUEST:
        indexes->insert(i_trade_request);
        indexes->insert(i_trade_request_b_id);
        indexes->insert(i_trade_request_s_symb);
        return;
    case TAB_TRADE_TYPE:
        indexes->insert(i_trade_type);
        return;
    case TAB_COMPANY:
        indexes->insert(i_company);
        return;
    case TAB_COMPANY_COMPETITOR:
        indexes->insert(i_company_competitor);
        return;
    case TAB_DAILY_MARKET:
        indexes->insert(i_daily_market);
        return;
    case TAB_EXCHANGE:
        indexes->insert(i_exchange);
        return;
    case TAB_FINANCIAL:
        indexes->insert(i_financial);
        return;
    case TAB_INDUSTRY:
        indexes->insert(i_industry);
        return;
    case TAB_LAST_TRADE:
        indexes->insert(i_last_trade);
        return;
    case TAB_NEWS_ITEM:
        indexes->insert(i_news_item);
        return;
    case TAB_NEWS_XREF:
        indexes->insert(i_news_xref);
        return;
    case TAB_SECTOR:
        indexes->insert(i_sector);
        return;
    case TAB_SECURITY:
        indexes->insert(i_security);
        return;
    case TAB_ADDRESS:
        indexes->insert(i_address);
        return;
    case TAB_STATUS_TYPE:
        indexes->insert(i_status_type);
        return;
    case TAB_TAXRATE:
        indexes->insert(i_taxrate);
        return;
    case TAB_ZIP_CODE:
        indexes->insert(i_zip_code);
        return;
    default:
        assert(false);
    }
}

INDEX * WorkloadTPCE::get_primary_index(uint32_t table_id)
{
    switch (table_id) {
    case TAB_ACCOUNT_PERMISSION:
        return i_account_permission;
    case TAB_CUSTOMER:
        return i_customer;
    case TAB_CUSTOMER_ACCOUNT:
        return i_customer_account;
    case TAB_CUSTOMER_TAXRATE:
        return i_customer_taxrate;
    case TAB_HOLDING:
        return i_holding;
    case TAB_HOLDING_HISTORY:
        return i_holding_history;
    case TAB_HOLDING_SUMMARY:
        return i_holding_summary;
    case TAB_WATCH_ITEM:
        return i_watch_item;
    case TAB_WATCH_LIST:
        return i_watch_list;
    case TAB_BROKER:
        return i_broker;
    case TAB_CASH_TRANSACTION:
        return i_cash_transaction;
    case TAB_CHARGE:
        return i_charge;
    case TAB_COMMISSION_RATE:
        return i_commission_rate;
    case TAB_SETTLEMENT:
        return i_settlement;
    case TAB_TRADE:
        return i_trade;
    case TAB_TRADE_HISTORY:
        return i_trade_history;
    case TAB_TRADE_REQUEST:
        return i_trade_request;
    case TAB_TRADE_TYPE:
        return i_trade_type;
    case TAB_COMPANY:
        return i_company;
    case TAB_COMPANY_COMPETITOR:
        return i_company_competitor;
    case TAB_DAILY_MARKET:
        return i_daily_market;
    case TAB_EXCHANGE:
        return i_exchange;
    case TAB_FINANCIAL:
        return i_financial;
    case TAB_INDUSTRY:
        return i_industry;
    case TAB_LAST_TRADE:
        return i_last_trade;
    case TAB_NEWS_ITEM:
        return i_news_item;
    case TAB_NEWS_XREF:
        return i_news_xref;
    case TAB_SECTOR:
        return i_sector;
    case TAB_SECURITY:
        return i_security;
    case TAB_ADDRESS:
        return i_address;
    case TAB_STATUS_TYPE:
        return i_status_type;
    case TAB_TAXRATE:
        return i_taxrate;
    case TAB_ZIP_CODE:
        return i_zip_code;
    default:
        assert(false);
    }
    return nullptr;
}

uint32_t WorkloadTPCE::index_to_table(uint32_t index_id)
{
    switch (index_id) {
    case IDX_ACCOUNT_PERMISSION:
        return TAB_ACCOUNT_PERMISSION;
    case IDX_CUSTOMER:
        return TAB_CUSTOMER;
    case IDX_CUSTOMER_ACCOUNT:
        return TAB_CUSTOMER_ACCOUNT;
    case IDX_CUSTOMER_ACCOUNT_C_ID:
        return TAB_CUSTOMER_ACCOUNT;
    case IDX_CUSTOMER_ACCOUNT_B_ID:
        return TAB_CUSTOMER_ACCOUNT;
    case IDX_CUSTOMER_TAXRATE:
        return TAB_CUSTOMER_TAXRATE;
    case IDX_HOLDING:
        return TAB_HOLDING;
    case IDX_HOLDING_CA_ID:
        return TAB_HOLDING;
    case IDX_HOLDING_S_SYMB:
        return TAB_HOLDING;
    case IDX_HOLDING_HISTORY:
        return TAB_HOLDING_HISTORY;
    case IDX_HOLDING_SUMMARY:
        return TAB_HOLDING_SUMMARY;
    case IDX_WATCH_ITEM:
        return TAB_WATCH_ITEM;
    case IDX_WATCH_LIST:
        return TAB_WATCH_LIST;
    case IDX_BROKER:
        return TAB_BROKER;
    case IDX_CASH_TRANSACTION:
        return TAB_CASH_TRANSACTION;
    case IDX_CHARGE:
        return TAB_CHARGE;
    case IDX_COMMISSION_RATE:
        return TAB_COMMISSION_RATE;
    case IDX_SETTLEMENT:
        return TAB_SETTLEMENT;
    case IDX_TRADE:
        return TAB_TRADE;
    case IDX_TRADE_CA_ID:
        return TAB_TRADE;
    case IDX_TRADE_S_SYMB:
        return TAB_TRADE;
    case IDX_TRADE_HISTORY:
        return TAB_TRADE_HISTORY;
    case IDX_TRADE_REQUEST:
        return TAB_TRADE_REQUEST;
    case IDX_TRADE_REQUEST_B_ID:
        return TAB_TRADE_REQUEST;
    case IDX_TRADE_REQUEST_S_SYMB:
        return TAB_TRADE_REQUEST;
    case IDX_TRADE_TYPE:
        return TAB_TRADE_TYPE;
    case IDX_COMPANY:
        return TAB_COMPANY;
    case IDX_COMPANY_COMPETITOR:
        return TAB_COMPANY_COMPETITOR;
    case IDX_DAILY_MARKET:
        return TAB_DAILY_MARKET;
    case IDX_EXCHANGE:
        return TAB_EXCHANGE;
    case IDX_FINANCIAL:
        return TAB_FINANCIAL;
    case IDX_INDUSTRY:
        return TAB_INDUSTRY;
    case IDX_LAST_TRADE:
        return TAB_LAST_TRADE;
    case IDX_NEWS_ITEM:
        return TAB_NEWS_ITEM;
    case IDX_NEWS_XREF:
        return TAB_NEWS_XREF;
    case IDX_SECTOR:
        return TAB_SECTOR;
    case IDX_SECURITY:
        return TAB_SECURITY;
    case IDX_ADDRESS:
        return TAB_ADDRESS;
    case IDX_STATUS_TYPE:
        return TAB_STATUS_TYPE;
    case IDX_TAXRATE:
        return TAB_TAXRATE;
    case IDX_ZIP_CODE:
        return TAB_ZIP_CODE;
    default:
        assert(false);
    }
    return 0;
}

uint32_t WorkloadTPCE::key_to_part(uint64_t key, uint32_t table_id)
{
    switch (table_id) {
    case TAB_ACCOUNT_PERMISSION:
        return key % g_num_parts;
    case TAB_CUSTOMER:
        return key % g_num_parts;
    case TAB_CUSTOMER_ACCOUNT:
        return key % g_num_parts;
    case TAB_CUSTOMER_TAXRATE:
        return key % g_num_parts;
    case TAB_HOLDING:
        return key % g_num_parts;
    case TAB_HOLDING_HISTORY:
        return key % g_num_parts;
    case TAB_HOLDING_SUMMARY:
        return (key >> 32) % g_num_parts;
    case TAB_WATCH_ITEM:
        return g_part_id;
    case TAB_WATCH_LIST:
        return g_part_id;
    case TAB_BROKER:
        return key % g_num_parts;
    case TAB_CASH_TRANSACTION:
        return key % g_num_parts;
    case TAB_CHARGE:
        return g_part_id;
    case TAB_COMMISSION_RATE:
        return g_part_id;
    case TAB_SETTLEMENT:
        return key % g_num_parts;
    case TAB_TRADE:
        return key % g_num_parts;
    case TAB_TRADE_HISTORY:
        return key % g_num_parts;
    case TAB_TRADE_REQUEST:
        return key % g_num_parts;
    case TAB_TRADE_TYPE:
        return g_part_id;
    case TAB_COMPANY:
        return key % g_num_parts;
    case TAB_COMPANY_COMPETITOR:
        return key % g_num_parts;
    case TAB_DAILY_MARKET:
        return key % g_num_parts;
    case TAB_EXCHANGE:
        return g_part_id;
    case TAB_FINANCIAL:
        return key % g_num_parts;
    case TAB_INDUSTRY:
        return g_part_id;
    case TAB_LAST_TRADE:
        return key % g_num_parts;
    case TAB_NEWS_ITEM:
        return g_part_id;
    case TAB_NEWS_XREF:
        return g_part_id;
    case TAB_SECTOR:
        return g_part_id;
    case TAB_SECURITY:
        return key % g_num_parts;
    case TAB_ADDRESS:
        return g_part_id;
    case TAB_STATUS_TYPE:
        return g_part_id;
    case TAB_TAXRATE:
        return g_part_id;
    case TAB_ZIP_CODE:
        return g_part_id;
    default:
        assert(false);
    }
    return 0;
}

RC WorkloadTPCE::init_table()
{
    // Partitioned tables
    init_tab_account_permission();
    init_tab_customer();
    init_tab_customer_account();
    init_tab_customer_taxrate();
    init_tab_holding_and_trade();
    init_tab_broker();
    init_tab_company_and_financial();
    init_tab_security_and_daily_market();
    init_tab_company_competitor();

    // Replicated tables
    init_tab_watch_item();
    init_tab_watch_list();
    init_tab_charge();
    init_tab_commission_rate();
    init_tab_trade_type();
    init_tab_exchange();
    init_tab_industry();
    init_tab_news_item();
    init_tab_news_xref();
    init_tab_sector();
    init_tab_address();
    init_tab_status_type();
    init_tab_taxrate();
    init_tab_zip_code();

    printf("TPCE Data Initialization Complete!\n");
    return RCOK;
}

// Initialization of partitioned tables

void WorkloadTPCE::init_tab_account_permission()
{
    row_t * row;
    uint64_t key;

    for (uint64_t i = 1; i <= g_account_num; i++) {
        // Partition the table according to accound ID
        if (i % g_num_parts != g_part_id) {
            continue;
        }

        char acl[5] = "1111";
        // All customers share the same tax ID
        char tax_id[4] = "US1";
        char last_name[25];
        MakeAlphaString(15, 25, last_name);
        char first_name[20];
        MakeAlphaString(10, 20, first_name);

        t_account_permission->get_new_row(row);
        row->set_value(AP_CA_ID, &i);
        row->set_value(AP_ACL, acl);
        row->set_value(AP_TAX_ID, tax_id);
        row->set_value(AP_L_NAME, last_name);
        row->set_value(AP_F_NAME, first_name);

        // Insert to indexes
        key = accountKey(i);
        assert(key_to_part(key, TAB_ACCOUNT_PERMISSION) == g_part_id);
        index_insert(i_account_permission, key, row);
    }
}

void WorkloadTPCE::init_tab_customer()
{
    row_t * row;
    uint64_t key;

    for (uint64_t i = 1; i <= g_customer_num; i++) {
        if (i % g_num_parts != g_part_id) {
            continue;
        }

        char tax_id[7] = "US1";
        char status_id[5] = "ACTV";
        char last_name[25];
        MakeAlphaString(15, 25, last_name);
        char first_name[20];
        MakeAlphaString(10, 20, first_name);
        char gender[2];
        if (RAND(2) == 0) {
            gender[0] = 'M';
        } else {
            gender[0] = 'F';
        }
        gender[1] = '\0';
        uint64_t tier = RAND(3) + 1;
        // Code below is for simplicity
        // All customers have the same DOB
        uint64_t dob = 19701231;
        // All customers have the same address and phone number
        uint64_t ad_id = 1;
        char country[4] = "1";
        char area[4] = "212";
        char local[10] = "12345678";
        char ext[5] = "123";
        char email[50] = "firstname@domain.com";

        t_customer->get_new_row(row);
        row->set_value(C_ID, &i);
        row->set_value(C_TAX_ID, tax_id);
        row->set_value(C_ST_ID, status_id);
        row->set_value(C_L_NAME, last_name);
        row->set_value(C_F_NAME, first_name);
        row->set_value(C_GNDR, gender);
        row->set_value(C_TIER, &tier);
        row->set_value(C_DOB, &dob);
        row->set_value(C_AD_ID, &ad_id);
        row->set_value(C_CTRY_1, country);
        row->set_value(C_AREA_1, area);
        row->set_value(C_LOCAL_1, local);
        row->set_value(C_EXT_1, ext);
        row->set_value(C_CTRY_2, country);
        row->set_value(C_AREA_2, area);
        row->set_value(C_LOCAL_2, local);
        row->set_value(C_EXT_2, ext);
        row->set_value(C_CTRY_3, country);
        row->set_value(C_AREA_3, area);
        row->set_value(C_LOCAL_3, local);
        row->set_value(C_EXT_3, ext);
        row->set_value(C_EMAIL_1, email);
        row->set_value(C_EMAIL_2, email);

        key = customerKey(i);
        assert(key_to_part(key, TAB_CUSTOMER) == g_part_id);
        index_insert(i_customer, key, row);
    }
}

void WorkloadTPCE::init_tab_customer_account()
{
    row_t * row;
    uint64_t key;

    for (uint64_t i = 1; i <= g_account_num; i++) {
        if (i % g_num_parts != g_part_id) {
            continue;
        }

        uint32_t x = URand(1, 100);

        uint64_t b_id = URand(1, g_broker_num);
        if (x >= g_tpce_perc_remote) {
            // Local broker
            while (b_id % g_num_parts != g_part_id) {
                b_id = URand(1, g_broker_num);
            }
        } else {
            // Remote broker
            while (b_id % g_num_parts == g_part_id) {
                b_id = URand(1, g_broker_num);
            }
        }

        uint64_t c_id = URand(1, g_customer_num);
        if (x >= g_tpce_perc_remote) {
            // Local customer
            while (c_id % g_num_parts != g_part_id) {
                c_id = URand(1, g_customer_num);
            }
        } else {
            // Remote customer
            while (c_id % g_num_parts == g_part_id) {
                c_id = URand(1, g_customer_num);
            }
        }

        char name[50];
        MakeAlphaString(40, 50, name);
        uint64_t tax_st = 2;
        // Enough balance for future trades
        double bal = 1000000000;

        t_customer_account->get_new_row(row);
        row->set_value(CA_ID, &i);
        row->set_value(CA_B_ID, &b_id);
        row->set_value(CA_C_ID, &c_id);
        row->set_value(CA_NAME, name);
        row->set_value(CA_TAX_ST, &tax_st);
        row->set_value(CA_BAL, &bal);

        key = accountKey(i);
        assert(key_to_part(key, TAB_CUSTOMER_ACCOUNT) == g_part_id);
        index_insert(i_customer_account, key, row);
        key = customerKey(c_id);
        index_insert(i_customer_account_c_id, key, row);
        key = brokerKey(b_id);
        index_insert(i_customer_account_b_id, key, row);
    }
}

void WorkloadTPCE::init_tab_customer_taxrate()
{
    row_t * row;
    uint64_t key;

    for (uint64_t i = 1; i <= g_customer_num; i++) {
        if (i % g_num_parts != g_part_id) {
            continue;
        }

        char tax_id[4] = "US1";

        t_customer_taxrate->get_new_row(row);
        row->set_value(CX_C_ID, &i);
        row->set_value(CX_TX_ID, tax_id);

        key = customerKey(i);
        assert(key_to_part(key, TAB_CUSTOMER_TAXRATE) == g_part_id);
        index_insert(i_customer_taxrate, key, row);
    }
}

// This function initializes the following tables:
// - holding
// - holding_history
// - holding_summary
// - trade
// - trade_request
// - trade_history
// - last_trade
// - settlement
// - cash_transaction
void WorkloadTPCE::init_tab_holding_and_trade()
{
    row_t * row;
    uint64_t key;
    // g_num_parts is added to exclude 0
    uint64_t s_symb_int = g_part_id + (g_part_id == 0 ? g_num_parts : 0);
    unordered_map<uint64_t, vector<uint64_t>> last_trades;
    unordered_map<uint64_t, unordered_map<uint64_t, uint64_t>> holdings;

    for (uint64_t i = 1; i <= g_init_trade_num; i++) {
        if (i % g_num_parts != g_part_id) {
            continue;
        }

        uint64_t ca_id = URand(1, g_account_num);
        while (ca_id % g_num_parts != g_part_id) {
            ca_id = URand(1, g_account_num);
        }
        uint64_t b_id = URand(1, g_broker_num);
        while (b_id % g_num_parts != g_part_id) {
            b_id = URand(1, g_broker_num);
        }
        // Use integers as security symbols for simplicity
        // All trades are on local securities, for the sake of last_trade table
        string s_symb_str = to_string(s_symb_int);
        s_symb_int += g_num_parts;
        if (s_symb_int > g_security_num) {
            // g_num_parts is added to exclude 0
            s_symb_int = g_part_id + (g_part_id == 0 ? g_num_parts : 0);
        }
        char s_symb[15];
        strcpy(s_symb, s_symb_str.c_str());
        uint64_t dts = 20240101;
        uint64_t due_date = 20240103;
        double price = RAND(100) + 1;
        // uint64_t before_qty = 0;
        uint64_t qty = RAND(1000) + 1;
        double amount = price * qty;
        holdings[ca_id][s_symb_int] = qty;
        bool is_margin = (RAND(10) == 9) ? true : false;
        char st_id[5];
        char tt_id[4];
        if (is_margin) {
            strcpy(st_id, "PNDG");
            strcpy(tt_id, "TLB");
        } else {
            strcpy(st_id, "CMPT");
            strcpy(tt_id, "TMB");
        }
        // Around 90% of the trades are cash transactions
        bool is_cash = !is_margin;
        char cash_type[40];
        if (is_cash) {
            strcpy(cash_type, "Cash Account");
        } else {
            strcpy(cash_type, "Margin");
        }
        double bid_price = price;
        char exec_name[50];
        MakeAlphaString(40, 50, exec_name);
        double charge = RAND(100) + 1;
        double commission = RAND(1000);
        double tax = 0.01 * amount;
        bool lifo = true;
        double open_price = price + RAND(10) * ((RAND(2) == 0) ? 1 : -1);
        uint64_t volume = URand(qty, 10000);
        char ct_name[100];
        MakeAlphaString(20, 100, ct_name);
        if (last_trades[s_symb_int].size() == 0) {
            last_trades[s_symb_int].emplace_back(dts);
            last_trades[s_symb_int].emplace_back(price);
            last_trades[s_symb_int].emplace_back(open_price);
            last_trades[s_symb_int].emplace_back(volume);
        } else {
            last_trades[s_symb_int][0] = dts;
            last_trades[s_symb_int][1] = price;
            last_trades[s_symb_int][2] = open_price;
            last_trades[s_symb_int][3] = volume;
        }

        // For the sake of Trade-Result transactions, no initial trades
        // are inserted to the holding and holding history table
        // holding
        // t_holding->get_new_row(row);
        // row->set_value(H_T_ID, &i);
        // row->set_value(H_CA_ID, &ca_id);
        // row->set_value(H_S_SYMB, s_symb);
        // row->set_value(H_DTS, &dts);
        // row->set_value(H_PRICE, &price);
        // row->set_value(H_QTY, &qty);

        // key = tradeKey(i);
        // assert(key_to_part(key, TAB_HOLDING) == g_part_id);
        // index_insert(i_holding, key, row);
        // key = accountKey(ca_id);
        // index_insert(i_holding_ca_id, key, row);
        // key = securityKey(s_symb_str);
        // index_insert(i_holding_s_symb, key, row);

        // Currently holding_history is not used in the workload
        // holding_history
        // t_holding_history->get_new_row(row);
        // row->set_value(HH_H_T_ID, &i);
        // row->set_value(HH_T_ID, &i);
        // row->set_value(HH_BEFORE_QTY, &before_qty);
        // row->set_value(HH_AFTER_QTY, &qty);

        // key = tradeKey(i);
        // assert(key_to_part(key, TAB_HOLDING_HISTORY) == g_part_id);
        // index_insert(i_holding_history, key, row);

        // trade
        t_trade->get_new_row(row);
        row->set_value(T_ID, &i);
        row->set_value(T_DTS, &dts);
        row->set_value(T_ST_ID, st_id);
        row->set_value(T_TT_ID, tt_id);
        row->set_value(T_IS_CASH, &is_cash);
        row->set_value(T_S_SYMB, s_symb);
        row->set_value(T_QTY, &qty);
        row->set_value(T_BID_PRICE, &bid_price);
        row->set_value(T_CA_ID, &ca_id);
        row->set_value(T_EXEC_NAME, exec_name);
        row->set_value(T_TRADE_PRICE, &price);
        row->set_value(T_CHRG, &charge);
        row->set_value(T_COMM, &commission);
        row->set_value(T_TAX, &tax);
        row->set_value(T_LIFO, &lifo);

        key = tradeKey(i);
        assert(key_to_part(key, TAB_TRADE) == g_part_id);
        index_insert(i_trade, key, row);
        key = accountKey(ca_id);
        index_insert(i_trade_ca_id, key, row);
        key = securityKey(s_symb_str);
        index_insert(i_trade_s_symb, key, row);

        if (is_margin) {
            // trade_request
            // This is a margin (limit) buy
            t_trade_request->get_new_row(row);
            row->set_value(TR_T_ID, &i);
            row->set_value(TR_TT_ID, tt_id);
            row->set_value(TR_S_SYMB, s_symb);
            row->set_value(TR_QTY, &qty);
            row->set_value(TR_BID_PRICE, &bid_price);
            row->set_value(TR_B_ID, &b_id);

            key = tradeKey(i);
            assert(key_to_part(key, TAB_TRADE_REQUEST) == g_part_id);
            index_insert(i_trade_request, key, row);
            key = brokerKey(i);
            index_insert(i_trade_request_b_id, key, row);
            key = securityKey(s_symb_str);
            index_insert(i_trade_request_s_symb, key, row);
        } else {
            // trade_history
            // t_trade_history->get_new_row(row);
            // row->set_value(TH_T_ID, &i);
            // row->set_value(TH_DTS, &dts);
            // row->set_value(TH_ST_ID, st_id);

            // key = tradeKey(i);
            // assert(key_to_part(key, TAB_TRADE_HISTORY) == g_part_id);
            // index_insert(i_trade_history, key, row);
        }

        // settlement
        t_settlement->get_new_row(row);
        row->set_value(SE_T_ID, &i);
        row->set_value(SE_CASH_TYPE, cash_type);
        row->set_value(SE_CASH_DUE_DATE, &due_date);
        row->set_value(SE_AMT, &amount);

        key = tradeKey(i);
        assert(key_to_part(key, TAB_SETTLEMENT) == g_part_id);
        index_insert(i_settlement, key, row);

        // cash_transaction
        if (is_cash) {
            t_cash_transaction->get_new_row(row);
            row->set_value(CT_T_ID, &i);
            row->set_value(CT_DTS, &dts);
            row->set_value(CT_AMT, &amount);
            row->set_value(CT_NAME, ct_name);

            key = tradeKey(i);
            assert(key_to_part(key, TAB_CASH_TRANSACTION) == g_part_id);
            index_insert(i_cash_transaction, key, row);
        }
    }

    // last_trade
    for (auto lt_it = last_trades.begin(); lt_it != last_trades.end(); lt_it++) {
        uint64_t s_symb_int = lt_it->first;
        uint64_t dts        = lt_it->second[0];
        uint64_t price      = lt_it->second[1];
        uint64_t open_price = lt_it->second[2];
        uint64_t volume     = lt_it->second[3];

        string s_symb_str = to_string(s_symb_int);
        char s_symb[15];
        strcpy(s_symb, s_symb_str.c_str());

        t_last_trade->get_new_row(row);
        row->set_value(LT_S_SYMB, s_symb);
        row->set_value(LT_DTS, &dts);
        row->set_value(LT_PRICE, &price);
        row->set_value(LT_OPEN_PRICE, &open_price);
        row->set_value(LT_VOL, &volume);

        key = securityKey(s_symb_str);
        assert(key_to_part(key, TAB_LAST_TRADE) == g_part_id);
        index_insert(i_last_trade, key, row);
    }

    // holding_summary
    // This table is partitioned according to the customer account id
    for (auto acc_it = holdings.begin(); acc_it != holdings.end(); acc_it++) {
        uint64_t ca_id = acc_it->first;
        unordered_map<uint64_t, uint64_t> m = acc_it->second;

        for (auto sec_it = m.begin(); sec_it != m.end(); sec_it++) {
            string s_symb_str = to_string(sec_it->first);
            char s_symb[15];
            strcpy(s_symb, s_symb_str.c_str());

            t_holding_summary->get_new_row(row);
            row->set_value(HS_CA_ID, &ca_id);
            row->set_value(HS_S_SYMB, s_symb);
            row->set_value(HS_QTY, &sec_it->second);

            key = holdingSummaryKey(ca_id, s_symb_str);
            assert(key_to_part(key, TAB_HOLDING_SUMMARY) == g_part_id);
            index_insert(i_holding_summary, key, row);
        }
    }
}

void WorkloadTPCE::init_tab_broker()
{
    row_t * row;
    uint64_t key;

    for (uint64_t i = 1; i <= g_broker_num; i++) {
        if (i % g_num_parts != g_part_id) {
            continue;
        }

        char st_id[5] = "ACTV";
        char name[50];
        MakeAlphaString(20, 50, name);
        uint64_t num_trades = RAND(100) + 1;
        double comm_total = num_trades * 100;

        t_broker->get_new_row(row);
        row->set_value(B_ID, &i);
        row->set_value(B_ST_ID, st_id);
        row->set_value(B_NAME, name);
        row->set_value(B_NUM_TRADES, &num_trades);
        row->set_value(B_COMM_TOTAL, &comm_total);

        key = brokerKey(i);
        assert(key_to_part(key, TAB_BROKER) == g_part_id);
        index_insert(i_broker, key, row);
    }
}

// This function initializes the following tables:
// - company
// - financial
void WorkloadTPCE::init_tab_company_and_financial()
{
    row_t * row;
    uint64_t key;

    for (uint64_t i = 1; i <= g_company_num; i++) {
        if (i % g_num_parts != g_part_id) {
            continue;
        }

        char st_id[5] = "ACTV";
        char name[60];
        MakeAlphaString(20, 60, name);
        uint64_t in_id_int = (i % 6) + 1;
        string in_id_str = to_string(in_id_int);
        char in_id[3];
        strcpy(in_id, in_id_str.c_str());
        char sp_rate[5] = "STD";
        char ceo[46];
        MakeAlphaString(10, 46, ceo);
        uint64_t ad_id = 1;
        char desc[150];
        MakeAlphaString(100, 150, desc);
        uint64_t open_date = 19701231;
        uint64_t year = 2024;
        uint64_t qtr = 1;
        uint64_t qtr_start_date = 20240101;
        double revenue = URand(1000, 1000000);
        double net_earn = revenue / 2;
        double basic_eps = RAND(10) + 1;
        double dilut_eps = basic_eps * 0.75;
        double margin = net_earn / revenue;
        double inventory = URand(1000, 10000);
        double asset = URand(1000, 1000000);
        double liability = RAND(10000);
        uint64_t out_basic = URand(100, 1000);
        uint64_t out_dilut = out_basic;

        // company
        t_company->get_new_row(row);
        row->set_value(CO_ID, &i);
        row->set_value(CO_ST_ID, st_id);
        row->set_value(CO_NAME, name);
        row->set_value(CO_IN_ID, in_id);
        row->set_value(CO_SP_RATE, sp_rate);
        row->set_value(CO_CEO, ceo);
        row->set_value(CO_AD_ID, &ad_id);
        row->set_value(CO_DESC, desc);
        row->set_value(CO_OPEN_DATE, &open_date);

        key = companyKey(i);
        assert(key_to_part(key, TAB_COMPANY) == g_part_id);
        index_insert(i_company, key, row);

        // financial
        t_financial->get_new_row(row);
        row->set_value(FI_CO_ID, &i);
        row->set_value(FI_YEAR, &year);
        row->set_value(FI_QTR, &qtr);
        row->set_value(FI_QTR_START_DATE, &qtr_start_date);
        row->set_value(FI_REVENUE, &revenue);
        row->set_value(FI_NET_EARN, &net_earn);
        row->set_value(FI_BASIC_EPS, &basic_eps);
        row->set_value(FI_DILUT_EPS, &dilut_eps);
        row->set_value(FI_MARGIN, &margin);
        row->set_value(FI_INVENTORY, &inventory);
        row->set_value(FI_ASSETS, &asset);
        row->set_value(FI_LIABILITY, &liability);
        row->set_value(FI_OUT_BASIC, &out_basic);
        row->set_value(FI_OUT_DILUT, &out_dilut);

        key = companyKey(i);
        assert(key_to_part(key, TAB_FINANCIAL) == g_part_id);
        index_insert(i_financial, key, row);
    }
}

// This function initializes the following tables:
// - security
// - daily_market
void WorkloadTPCE::init_tab_security_and_daily_market()
{
    row_t * row;
    uint64_t key;

    for (uint64_t i = 1; i <= g_security_num; i++) {
        if (i % g_num_parts != g_part_id) {
            continue;
        }

        uint64_t date = 20240102;
        string s_symb_str = to_string(i);
        char s_symb[15];
        strcpy(s_symb, s_symb_str.c_str());
        double close = RAND(100) + 1;
        double high = close + 10;
        double low = close - 10;
        uint64_t volume = RAND(1000);
        char issue[7] = "COMMON";
        char st_id[5] = "ACTV";
        char name[70];
        MakeAlphaString(10, 70, name);
        char ex_id[7] = "NASDAQ";
        uint64_t co_id = (i % 100 + 1) * g_num_parts + g_part_id;
        uint64_t num_out = RAND(100000);
        uint64_t start_date = 20230101;
        uint64_t exch_date = 20230201;
        double pe = (RAND(5) + 1) / 10;
        double wk_high = URand(10, 100);
        double wk_low = wk_high / 2;
        uint64_t wk_high_date = 20230301;
        uint64_t wk_low_date = 20230401;
        double dividend = RAND(50);
        double yield = dividend / wk_high * 100;

        // security
        t_security->get_new_row(row);
        row->set_value(S_SYMB, s_symb);
        row->set_value(S_ISSUE, issue);
        row->set_value(S_ST_ID, st_id);
        row->set_value(S_NAME, name);
        row->set_value(S_EX_ID, ex_id);
        row->set_value(S_CO_ID, &co_id);
        row->set_value(S_NUM_OUT, &num_out);
        row->set_value(S_START_DATE, &start_date);
        row->set_value(S_EXCH_DATE, &exch_date);
        row->set_value(S_PE, &pe);
        row->set_value(S_52WK_HIGH, &wk_high);
        row->set_value(S_52WK_HIGH_DATE, &wk_high_date);
        row->set_value(S_52WK_LOW, &wk_low);
        row->set_value(S_52WK_LOW_DATE, &wk_low_date);
        row->set_value(S_DIVIDEND, &dividend);
        row->set_value(S_YIELD, &yield);

        key = securityKey(s_symb_str);
        assert(key_to_part(key, TAB_SECURITY) == g_part_id);
        index_insert(i_security, key, row);

        // daily_market
        t_daily_market->get_new_row(row);
        row->set_value(DM_DATE, &date);
        row->set_value(DM_S_SYMB, s_symb);
        row->set_value(DM_CLOSE, &close);
        row->set_value(DM_HIGH, &high);
        row->set_value(DM_LOW, &low);
        row->set_value(DM_VOL, &volume);

        key = securityKey(s_symb_str);
        assert(key_to_part(key, TAB_DAILY_MARKET) == g_part_id);
        index_insert(i_daily_market, key, row);
    }
}

// Currently this table is not in use
void WorkloadTPCE::init_tab_company_competitor() { }

// Initialization of replicated tables

// Currently this table is not in use
void WorkloadTPCE::init_tab_watch_item() { }

// Currently this table is not in use
void WorkloadTPCE::init_tab_watch_list() { }

void WorkloadTPCE::init_tab_charge()
{
    row_t * row;
    uint64_t key;
    double charge = 10;
    vector<string> tt_ids {"TMB", "TMS", "TSL", "TLS", "TLB"};

    for (string tt_id_str : tt_ids) {
        for (uint64_t tier = 1; tier <= 3; tier++) {
            char tt_id[4];
            strcpy(tt_id, tt_id_str.c_str());

            t_charge->get_new_row(row);
            row->set_value(CH_TT_ID, tt_id);
            row->set_value(CH_C_TIER, &tier);
            row->set_value(CH_CHRG, &charge);

            key = chargeKey(tier, tt_id_str);
            index_insert(i_charge, key, row);
        }
    }
}

void WorkloadTPCE::init_tab_commission_rate()
{
    row_t * row;
    uint64_t key;
    char ex_id[7] = "NASDAQ";
    uint64_t from_qty = 0;
    uint64_t to_qty = 1000000000;
    double rate = 10;
    vector<string> tt_ids {"TMB", "TMS", "TSL", "TLS", "TLB"};

    for (string tt_id_str : tt_ids) {
        for (uint64_t tier = 1; tier <= 3; tier++) {
            char tt_id[4];
            strcpy(tt_id, tt_id_str.c_str());

            t_commission_rate->get_new_row(row);
            row->set_value(CR_C_TIER, &tier);
            row->set_value(CR_TT_ID, tt_id);
            row->set_value(CR_EX_ID, ex_id);
            row->set_value(CR_FROM_QTY, &from_qty);
            row->set_value(CR_TO_QTY, &to_qty);
            row->set_value(CR_RATE, &rate);

            string ex_id_str(ex_id);
            key = commissionRateKey(tier, tt_id_str, ex_id_str, from_qty);
            index_insert(i_commission_rate, key, row);
        }
    }
}

void WorkloadTPCE::init_tab_trade_type()
{
    row_t * row;
    uint64_t key;
    vector<string> tt_ids {"TMB", "TMS", "TSL", "TLS", "TLB"};
    vector<string> names {"Market-Buy", "Market-Sell", "Stop-Loss", "Limit-Sell", "Limit-Buy"};
    bool is_sell, is_mrkt;

    for (uint32_t i = 0; i < tt_ids.size(); i++) {
        string tt_id_str = tt_ids[i];
        char tt_id[4];
        char name[12];
        strcpy(tt_id, tt_id_str.c_str());
        strcpy(name, names[i].c_str());
        if (tt_id_str == "TLB") {
            is_sell = false;
            is_mrkt = false;
        } else if (tt_id_str == "TLS") {
            is_sell = true;
            is_mrkt = false;
        } else if (tt_id_str == "TMB") {
            is_sell = false;
            is_mrkt = true;
        } else if (tt_id_str == "TMS") {
            is_sell = true;
            is_mrkt = true;
        } else if (tt_id_str == "TSL") {
            is_sell = true;
            is_mrkt = false;
        } else {
            assert(false);
        }

        t_trade_type->get_new_row(row);
        row->set_value(TT_ID, tt_id);
        row->set_value(TT_NAME, name);
        row->set_value(TT_IS_SELL, &is_sell);
        row->set_value(TT_IS_MRKT, &is_mrkt);

        key = tradeTypeKey(tt_id_str);
        index_insert(i_trade_type, key, row);
    }
}

void WorkloadTPCE::init_tab_exchange()
{
    row_t * row;
    uint64_t key;

    char ex_id[7] = "NASDAQ";
    char name[100] = "Nasdaq Stock Market";
    uint64_t num_symb = g_security_num;
    uint64_t open = 800;
    uint64_t close = 1800;
    char desc[100] = "National Association of Securities Dealers Automated Quotations Stock Market";
    uint64_t ad_id = 1;

    t_exchange->get_new_row(row);
    row->set_value(EX_ID, ex_id);
    row->set_value(EX_NAME, name);
    row->set_value(EX_NUM_SYMB, &num_symb);
    row->set_value(EX_OPEN, &open);
    row->set_value(EX_CLOSE, &close);
    row->set_value(EX_DESC, desc);
    row->set_value(EX_AD_ID, &ad_id);

    key = exchangeKey();
    index_insert(i_exchange, key, row);
}

void WorkloadTPCE::init_tab_industry()
{
    row_t * row;
    uint64_t key;
    vector<string> names {"Air Travel", "Air Cargo", "Software", "Consumer Banking", "Merchant Banking"};

    for (uint64_t i = 0; i < names.size(); i++) {
        uint64_t in_id_int = i + 1;
        string in_id_str = to_string(in_id_int);
        char in_id[3];
        strcpy(in_id, in_id_str.c_str());
        char name[50];
        strcpy(name, names[i].c_str());
        uint64_t sc_id_int;
        if (in_id_int == 1 || in_id_int == 2) {
            sc_id_int = 1;
        } else if (in_id_int == 3) {
            sc_id_int = 2;
        } else {
            sc_id_int = 3;
        }
        char sc_id[3];
        strcpy(sc_id, to_string(sc_id_int).c_str());

        t_industry->get_new_row(row);
        row->set_value(IN_ID, in_id);
        row->set_value(IN_NAME, name);
        row->set_value(IN_SC_ID, sc_id);

        key = industryKey(in_id_str);
        index_insert(i_industry, key, row);
    }
}

// Currently this table is not in use
void WorkloadTPCE::init_tab_news_item() { }

// Currently this table is not in use
void WorkloadTPCE::init_tab_news_xref() { }

void WorkloadTPCE::init_tab_sector()
{
    row_t * row;
    uint64_t key;
    vector<string> names {"Airline", "Information Technology", "Banking"};

    for (uint64_t i = 0; i < names.size(); i++) {
        uint64_t sc_id_int = i + 1;
        string sc_id_str = to_string(sc_id_int);
        char sc_id[3];
        strcpy(sc_id, sc_id_str.c_str());
        char name[30];
        strcpy(name, names[i].c_str());

        t_sector->get_new_row(row);
        row->set_value(SC_ID, sc_id);
        row->set_value(SC_NAME, name);

        key = sectorKey(sc_id_str);
        index_insert(i_sector, key, row);
    }
}

void WorkloadTPCE::init_tab_address()
{
    row_t * row;
    uint64_t key;

    uint64_t ad_id = 1;
    char line1[80] = "1 Apple Park Way";
    char line2[80] = "Cupertino, California";
    char zc_code[12] = "95014";
    char ctry[80] = "United States";

    t_address->get_new_row(row);
    row->set_value(AD_ID, &ad_id);
    row->set_value(AD_LINE1, line1);
    row->set_value(AD_LINE2, line2);
    row->set_value(AD_ZC_CODE, zc_code);
    row->set_value(AD_CTRY, ctry);

    key = addressKey(ad_id);
    index_insert(i_address, key, row);
}

void WorkloadTPCE::init_tab_status_type()
{
    row_t * row;
    uint64_t key;
    vector<string> st_ids {"ACTV", "CMPT", "CNCL", "PNDG", "SBMT"};
    vector<string> names {"Active", "Completed", "Canceled", "Pending", "Submitted"};

    for (uint32_t i = 0; i < st_ids.size(); i++) {
        string st_id_str = st_ids[i];
        string name_str = names[i];
        char st_id[5];
        char name[10];
        strcpy(st_id, st_id_str.c_str());
        strcpy(name, name_str.c_str());

        t_status_type->get_new_row(row);
        row->set_value(ST_ID, st_id);
        row->set_value(ST_NAME, name);

        key = statusTypeKey(st_id_str);
        index_insert(i_status_type, key, row);
    }
}

void WorkloadTPCE::init_tab_taxrate()
{
    row_t * row;
    uint64_t key;

    char tx_id[5] = "US1";
    char name[50] = "United States";
    double rate = 0.05;

    t_taxrate->get_new_row(row);
    row->set_value(TX_ID, tx_id);
    row->set_value(TX_NAME, name);
    row->set_value(TX_RATE, &rate);

    key = taxrateKey();
    index_insert(i_taxrate, key, row);
}

void WorkloadTPCE::init_tab_zip_code()
{
    row_t * row;
    uint64_t key;

    char zc_code[12] = "95014";
    char town[80] = "Cupertino";
    char div[80] = "California";

    t_zip_code->get_new_row(row);
    row->set_value(ZC_CODE, zc_code);
    row->set_value(ZC_TOWN, town);
    row->set_value(ZC_DIV, div);

    key = zipCodeKey();
    index_insert(i_zip_code, key, row);
}

StoreProcedure * WorkloadTPCE::create_store_procedure(TxnManager * txn, QueryBase * query)
{
    return new TPCEStoreProcedure(txn, query);
}

// Not in use
QueryBase * WorkloadTPCE::deserialize_subquery(char * data)
{
    return (QueryTPCE *) data;
}

QueryBase * WorkloadTPCE::gen_query(int is_mp)
{
    double x = glob_manager->rand_double();
    if (x < g_perc_broker_volume)
        return new QueryBrokerVolumeTPCE();
    x -= g_perc_broker_volume;
    if (x < g_perc_customer_position)
        return new QueryCustomerPositionTPCE();
    x -= g_perc_customer_position;
    if (x < g_perc_market_feed)
        return new QueryMarketFeedTPCE();
    x -= g_perc_market_feed;
    if (x < g_perc_market_watch)
        return new QueryMarketWatchTPCE();
    x -= g_perc_market_watch;
    if (x < g_perc_security_detail)
        return new QuerySecurityDetailTPCE();
    x -= g_perc_security_detail;
    if (x < g_perc_trade_lookup)
        return new QueryTradeLookupTPCE();
    x -= g_perc_trade_lookup;
    if (x < g_perc_trade_order)
        return new QueryTradeOrderTPCE();
    x -= g_perc_trade_order;
    if (x < g_perc_trade_result)
        return new QueryTradeResultTPCE();
    x -= g_perc_trade_result;
    if (x < g_perc_trade_status)
        return new QueryTradeStatusTPCE();
    x -= g_perc_trade_status;
    if (x < g_perc_trade_update)
        return new QueryTradeUpdateTPCE();
    
    assert(false);
    return NULL;
}

QueryBase * WorkloadTPCE::clone_query(QueryBase * query)
{
    QueryTPCE * q = (QueryTPCE *) query;
    switch (q->type) {
    case TPCE_BROKER_VOLUME:
        return new QueryBrokerVolumeTPCE((QueryBrokerVolumeTPCE *) query);
    case TPCE_CUSTOMER_POSITION:
        return new QueryCustomerPositionTPCE((QueryCustomerPositionTPCE *) query);
    case TPCE_MARKET_FEED:
        return new QueryMarketFeedTPCE((QueryMarketFeedTPCE *) query);
    case TPCE_MARKET_WATCH:
        return new QueryMarketWatchTPCE((QueryMarketWatchTPCE *) query);
    case TPCE_SECURITY_DETAIL:
        return new QuerySecurityDetailTPCE((QuerySecurityDetailTPCE *) query);
    case TPCE_TRADE_LOOKUP:
        return new QueryTradeLookupTPCE((QueryTradeLookupTPCE *) query);
    case TPCE_TRADE_ORDER:
        return new QueryTradeOrderTPCE((QueryTradeOrderTPCE *) query);
    case TPCE_TRADE_RESULT:
        return new QueryTradeResultTPCE((QueryTradeResultTPCE *) query);
    case TPCE_TRADE_STATUS:
        return new QueryTradeStatusTPCE((QueryTradeStatusTPCE *) query);
    case TPCE_TRADE_UPDATE:
        return new QueryTradeUpdateTPCE((QueryTradeUpdateTPCE *) query);
    default:
        assert(false);
    }
}

uint64_t WorkloadTPCE::get_primary_key(row_t * row)
{
    uint64_t c_id, ca_id, t_id, b_id, co_id, ad_id, tier, qty;
    char s_symb[15], tt_id[4], ex_id[7], in_id[3], sc_id[3], st_id[5];

    table_t * table = row->get_table();
    switch (table->get_table_id()) {
    case TAB_ACCOUNT_PERMISSION: {
        row->get_value(AP_CA_ID, &ca_id);
        return accountKey(ca_id);
    }
    case TAB_CUSTOMER: {
        row->get_value(C_ID, &c_id);
        return customerKey(c_id);
    }
    case TAB_CUSTOMER_ACCOUNT: {
        row->get_value(CA_ID, &ca_id);
        return accountKey(ca_id);
    }
    case TAB_CUSTOMER_TAXRATE: {
        row->get_value(CX_C_ID, &c_id);
        return customerKey(c_id);
    }
    case TAB_HOLDING: {
        row->get_value(H_T_ID, &t_id);
        return tradeKey(t_id);
    }
    case TAB_HOLDING_HISTORY: {
        row->get_value(HH_T_ID, &t_id);
        return tradeKey(t_id);
    }
    case TAB_HOLDING_SUMMARY: {
        row->get_value(HS_CA_ID, &ca_id);
        row->get_value(HS_S_SYMB, s_symb);
        string s_symb_str(s_symb);
        return holdingSummaryKey(ca_id, s_symb_str);
    }
    case TAB_WATCH_ITEM: {
        assert(false);
    }
    case TAB_WATCH_LIST: {
        assert(false);
    }
    case TAB_BROKER: {
        row->get_value(B_ID, &b_id);
        return brokerKey(b_id);
    }
    case TAB_CASH_TRANSACTION: {
        row->get_value(CT_T_ID, &t_id);
        return tradeKey(t_id);
    }
    case TAB_CHARGE: {
        row->get_value(CH_TT_ID, tt_id);
        row->get_value(CH_C_TIER, &tier);
        string tt_id_str(tt_id);
        return chargeKey(tier, tt_id_str);
    }
    case TAB_COMMISSION_RATE: {
        row->get_value(CR_C_TIER, &tier);
        row->get_value(CR_TT_ID, tt_id);
        row->get_value(CR_EX_ID, ex_id);
        row->get_value(CR_FROM_QTY, &qty);
        string tt_id_str(tt_id);
        string ex_id_str(ex_id);
        return commissionRateKey(tier, tt_id_str, ex_id_str, qty);
    }
    case TAB_SETTLEMENT: {
        row->get_value(SE_T_ID, &t_id);
        return tradeKey(t_id);
    }
    case TAB_TRADE: {
        row->get_value(T_ID, &t_id);
        return tradeKey(t_id);
    }
    case TAB_TRADE_HISTORY: {
        row->get_value(TH_T_ID, &t_id);
        return tradeKey(t_id);
    }
    case TAB_TRADE_REQUEST: {
        row->get_value(TR_T_ID, &t_id);
        return tradeKey(t_id);
    }
    case TAB_TRADE_TYPE: {
        row->get_value(TT_ID, tt_id);
        string tt_id_str(tt_id);
        return tradeTypeKey(tt_id_str);
    }
    case TAB_COMPANY: {
        row->get_value(CO_ID, &co_id);
        return companyKey(co_id);
    }
    case TAB_COMPANY_COMPETITOR: {
        assert(false);
    }
    case TAB_DAILY_MARKET: {
        row->get_value(DM_S_SYMB, s_symb);
        string s_symb_str(s_symb);
        return securityKey(s_symb_str);
    }
    case TAB_EXCHANGE: {
        // Only one exchange
        return exchangeKey();
    }
    case TAB_FINANCIAL: {
        row->get_value(FI_CO_ID, &co_id);
        return companyKey(co_id);
    }
    case TAB_INDUSTRY: {
        row->get_value(IN_ID, in_id);
        string in_id_str(in_id);
        return industryKey(in_id_str);
    }
    case TAB_LAST_TRADE: {
        row->get_value(LT_S_SYMB, s_symb);
        string s_symb_str(s_symb);
        return securityKey(s_symb_str);
    }
    case TAB_NEWS_ITEM: {
        assert(false);
    }
    case TAB_NEWS_XREF: {
        assert(false);
    }
    case TAB_SECTOR: {
        row->get_value(SC_ID, sc_id);
        string sc_id_str(sc_id);
        return sectorKey(sc_id_str);
    }
    case TAB_SECURITY: {
        row->get_value(S_SYMB, s_symb);
        string s_symb_str(s_symb);
        return securityKey(s_symb_str);
    }
    case TAB_ADDRESS: {
        row->get_value(AD_ID, &ad_id);
        return addressKey(ad_id);
    }
    case TAB_STATUS_TYPE: {
        row->get_value(ST_ID, st_id);
        string st_id_str(st_id);
        return statusTypeKey(st_id_str);
    }
    case TAB_TAXRATE: {
        return taxrateKey();
    }
    case TAB_ZIP_CODE: {
        return zipCodeKey();
    }
    default:
        assert(false);
    }
}

uint64_t WorkloadTPCE::get_index_key(row_t * row, uint32_t index_id)
{
    uint64_t c_id, b_id, ca_id;
    char s_symb[15];

    switch (index_id) {
    // Primary indexes
    case IDX_ACCOUNT_PERMISSION:
    case IDX_CUSTOMER:
    case IDX_CUSTOMER_ACCOUNT:
    case IDX_CUSTOMER_TAXRATE:
    case IDX_HOLDING:
    case IDX_HOLDING_HISTORY:
    case IDX_HOLDING_SUMMARY:
    case IDX_WATCH_ITEM:
    case IDX_WATCH_LIST:
    case IDX_BROKER:
    case IDX_CASH_TRANSACTION:
    case IDX_CHARGE:
    case IDX_COMMISSION_RATE:
    case IDX_SETTLEMENT:
    case IDX_TRADE:
    case IDX_TRADE_HISTORY:
    case IDX_TRADE_REQUEST:
    case IDX_TRADE_TYPE:
    case IDX_COMPANY:
    case IDX_COMPANY_COMPETITOR:
    case IDX_DAILY_MARKET:
    case IDX_EXCHANGE:
    case IDX_FINANCIAL:
    case IDX_INDUSTRY:
    case IDX_LAST_TRADE:
    case IDX_NEWS_ITEM:
    case IDX_NEWS_XREF:
    case IDX_SECTOR:
    case IDX_SECURITY:
    case IDX_ADDRESS:
    case IDX_STATUS_TYPE:
    case IDX_TAXRATE:
    case IDX_ZIP_CODE: {
        return get_primary_key(row);
    }
    // Secondary indexes
    case IDX_CUSTOMER_ACCOUNT_C_ID: {
        row->get_value(CA_C_ID, &c_id);
        return customerKey(c_id);
    }
    case IDX_CUSTOMER_ACCOUNT_B_ID: {
        row->get_value(CA_B_ID, &b_id);
        return customerKey(b_id);
    }
    case IDX_HOLDING_CA_ID: {
        row->get_value(H_CA_ID, &ca_id);
        return accountKey(ca_id);
    }
    case IDX_HOLDING_S_SYMB: {
        row->get_value(H_S_SYMB, s_symb);
        string s_symb_str(s_symb);
        return securityKey(s_symb_str);
    }
    case IDX_TRADE_REQUEST_B_ID: {
        row->get_value(TR_B_ID, &b_id);
        return brokerKey(b_id);
    }
    case IDX_TRADE_CA_ID: {
        row->get_value(T_CA_ID, &ca_id);
        return accountKey(ca_id);
    }
    case IDX_TRADE_S_SYMB: {
        row->get_value(T_S_SYMB, s_symb);
        string s_symb_str(s_symb);
        return securityKey(s_symb_str);
    }
    case IDX_TRADE_REQUEST_S_SYMB: {
        row->get_value(TR_S_SYMB, s_symb);
        string s_symb_str(s_symb);
        return securityKey(s_symb_str);
    }
    default:
        assert(false);
    }
}

#if HOTNESS_ENABLE
void WorkloadTPCE::prepare_hotness_table()
{
    uint32_t num_table = 33;
    // Replicated tables do not need hotness statistics
    vector<uint64_t> num_row
    {
        g_account_num,
        g_customer_num,
        g_account_num,
        g_customer_num,
        2 * g_init_trade_num,
        g_init_trade_num,
        g_account_num,
        0,
        0,
        g_broker_num,
        g_init_trade_num,
        0,
        0,
        g_init_trade_num,
        2 * g_init_trade_num,
        2 * g_init_trade_num,
        g_init_trade_num,
        0,
        g_company_num,
        0,
        g_security_num,
        0,
        g_company_num,
        0,
        g_security_num,
        0,
        0,
        0,
        g_security_num,
        0,
        0,
        0,
        0,
    };
    glob_manager->resize_hotness_table(num_table, num_row);
}
#endif

#endif