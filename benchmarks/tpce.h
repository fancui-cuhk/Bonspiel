#pragma once

#include "txn.h"
#include "workload.h"

class table_t;
class INDEX;

class WorkloadTPCE : public workload {
public:
    RC               init();
    RC               init_table();
    RC               init_schema(const char * schema_file);
    uint64_t         next_t_id();
    int64_t          next_t_id_trade_result();

    QueryBase *      gen_query(int is_mp = 0);
    QueryBase *      clone_query(QueryBase * query);
    StoreProcedure * create_store_procedure(TxnManager * txn, QueryBase * query);
    QueryBase *      deserialize_subquery(char * data);

    uint32_t         key_to_part(uint64_t key, uint32_t table_id);
    uint32_t         index_to_table(uint32_t index_id);
    void             table_to_indexes(uint32_t table_id, set<INDEX *> * indexes);
    uint64_t         get_primary_key(row_t * row);
    uint64_t         get_index_key(row_t * row, uint32_t index_id);
    table_t *        get_table(uint32_t table_id) { return tables[table_id]; }
    INDEX *          get_index(uint32_t index_id) { return indexes[index_id]; }
    INDEX *          get_primary_index(uint32_t table_id);

#if HOTNESS_ENABLE
    void             prepare_hotness_table();
#endif

    table_t * t_account_permission;
    table_t * t_customer;
    table_t * t_customer_account;
    table_t * t_customer_taxrate;
    table_t * t_holding;
    table_t * t_holding_history;
    table_t * t_holding_summary;
    table_t * t_watch_item;
    table_t * t_watch_list;
    table_t * t_broker;
    table_t * t_cash_transaction;
    table_t * t_charge;
    table_t * t_commission_rate;
    table_t * t_settlement;
    table_t * t_trade;
    table_t * t_trade_history;
    table_t * t_trade_request;
    table_t * t_trade_type;
    table_t * t_company;
    table_t * t_company_competitor;
    table_t * t_daily_market;
    table_t * t_exchange;
    table_t * t_financial;
    table_t * t_industry;
    table_t * t_last_trade;
    table_t * t_news_item;
    table_t * t_news_xref;
    table_t * t_sector;
    table_t * t_security;
    table_t * t_address;
    table_t * t_status_type;
    table_t * t_taxrate;
    table_t * t_zip_code;

    INDEX *   i_account_permission;
    INDEX *   i_customer;
    INDEX *   i_customer_account;
    INDEX *   i_customer_account_c_id;
    INDEX *   i_customer_account_b_id;
    INDEX *   i_customer_taxrate;
    INDEX *   i_holding;
    INDEX *   i_holding_ca_id;
    INDEX *   i_holding_s_symb;
    INDEX *   i_holding_history;
    INDEX *   i_holding_summary;
    INDEX *   i_watch_item;
    INDEX *   i_watch_list;
    INDEX *   i_broker;
    INDEX *   i_cash_transaction;
    INDEX *   i_charge;
    INDEX *   i_commission_rate;
    INDEX *   i_settlement;
    INDEX *   i_trade;
    INDEX *   i_trade_ca_id;
    INDEX *   i_trade_s_symb;
    INDEX *   i_trade_history;
    INDEX *   i_trade_request;
    INDEX *   i_trade_request_b_id;
    INDEX *   i_trade_request_s_symb;
    INDEX *   i_trade_type;
    INDEX *   i_company;
    INDEX *   i_company_competitor;
    INDEX *   i_daily_market;
    INDEX *   i_exchange;
    INDEX *   i_financial;
    INDEX *   i_industry;
    INDEX *   i_last_trade;
    INDEX *   i_news_item;
    INDEX *   i_news_xref;
    INDEX *   i_sector;
    INDEX *   i_security;
    INDEX *   i_address;
    INDEX *   i_status_type;
    INDEX *   i_taxrate;
    INDEX *   i_zip_code;

private:
    // Partitioned tables
    void init_tab_account_permission();
    void init_tab_customer();
    void init_tab_customer_account();
    void init_tab_customer_taxrate();
    void init_tab_holding_and_trade();
    void init_tab_broker();
    void init_tab_company_and_financial();
    void init_tab_security_and_daily_market();
    void init_tab_company_competitor();

    // Replicated tables
    void init_tab_watch_item();
    void init_tab_watch_list();
    void init_tab_charge();
    void init_tab_commission_rate();
    void init_tab_trade_type();
    void init_tab_exchange();
    void init_tab_industry();
    void init_tab_news_item();
    void init_tab_news_xref();
    void init_tab_sector();
    void init_tab_address();
    void init_tab_status_type();
    void init_tab_taxrate();
    void init_tab_zip_code();

    // Trade id used in Trade-Order transactions
    uint64_t _t_id;
    // Trade id used in Trade-Result transactions
    uint64_t _t_id_trade_result;
};