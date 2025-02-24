#pragma once

// Tables

enum {
    TAB_ACCOUNT_PERMISSION,
    TAB_CUSTOMER,
    TAB_CUSTOMER_ACCOUNT,
    TAB_CUSTOMER_TAXRATE,
    TAB_HOLDING,
    TAB_HOLDING_HISTORY,
    TAB_HOLDING_SUMMARY,
    TAB_WATCH_ITEM,
    TAB_WATCH_LIST,
    TAB_BROKER,
    TAB_CASH_TRANSACTION,
    TAB_CHARGE,
    TAB_COMMISSION_RATE,
    TAB_SETTLEMENT,
    TAB_TRADE,
    TAB_TRADE_HISTORY,
    TAB_TRADE_REQUEST,
    TAB_TRADE_TYPE,
    TAB_COMPANY,
    TAB_COMPANY_COMPETITOR,
    TAB_DAILY_MARKET,
    TAB_EXCHANGE,
    TAB_FINANCIAL,
    TAB_INDUSTRY,
    TAB_LAST_TRADE,
    TAB_NEWS_ITEM,
    TAB_NEWS_XREF,
    TAB_SECTOR,
    TAB_SECURITY,
    TAB_ADDRESS,
    TAB_STATUS_TYPE,
    TAB_TAXRATE,
    TAB_ZIP_CODE
};

// Indices

enum {
    IDX_ACCOUNT_PERMISSION,
    IDX_CUSTOMER,
    IDX_CUSTOMER_ACCOUNT,
    IDX_CUSTOMER_ACCOUNT_C_ID,
    IDX_CUSTOMER_ACCOUNT_B_ID,
    IDX_CUSTOMER_TAXRATE,
    IDX_HOLDING,
    IDX_HOLDING_CA_ID,
    IDX_HOLDING_S_SYMB,
    IDX_HOLDING_HISTORY,
    IDX_HOLDING_SUMMARY,
    IDX_WATCH_ITEM,
    IDX_WATCH_LIST,
    IDX_BROKER,
    IDX_CASH_TRANSACTION,
    IDX_CHARGE,
    IDX_COMMISSION_RATE,
    IDX_SETTLEMENT,
    IDX_TRADE,
    IDX_TRADE_CA_ID,
    IDX_TRADE_S_SYMB,
    IDX_TRADE_HISTORY,
    IDX_TRADE_REQUEST,
    IDX_TRADE_REQUEST_B_ID,
    IDX_TRADE_REQUEST_S_SYMB,
    IDX_TRADE_TYPE,
    IDX_COMPANY,
    IDX_COMPANY_COMPETITOR,
    IDX_DAILY_MARKET,
    IDX_EXCHANGE,
    IDX_FINANCIAL,
    IDX_INDUSTRY,
    IDX_LAST_TRADE,
    IDX_NEWS_ITEM,
    IDX_NEWS_XREF,
    IDX_SECTOR,
    IDX_SECURITY,
    IDX_ADDRESS,
    IDX_STATUS_TYPE,
    IDX_TAXRATE,
    IDX_ZIP_CODE
};

// Transactions

enum {
    TPCE_BROKER_VOLUME,
    TPCE_CUSTOMER_POSITION,
    TPCE_MARKET_FEED,
    TPCE_MARKET_WATCH,
    TPCE_SECURITY_DETAIL,
    TPCE_TRADE_LOOKUP,
    TPCE_TRADE_ORDER,
    TPCE_TRADE_RESULT,
    TPCE_TRADE_STATUS,
    TPCE_TRADE_UPDATE
};

// Columns

enum {
    AP_CA_ID,
    AP_ACL,
    AP_TAX_ID,
    AP_L_NAME,
    AP_F_NAME
};

enum {
    C_ID,
    C_TAX_ID,
    C_ST_ID,
    C_L_NAME,
    C_F_NAME,
    C_GNDR,
    C_TIER,
    C_DOB,
    C_AD_ID,
    C_CTRY_1,
    C_AREA_1,
    C_LOCAL_1,
    C_EXT_1,
    C_CTRY_2,
    C_AREA_2,
    C_LOCAL_2,
    C_EXT_2,
    C_CTRY_3,
    C_AREA_3,
    C_LOCAL_3,
    C_EXT_3,
    C_EMAIL_1,
    C_EMAIL_2
};

enum {
    CA_ID,
    CA_B_ID,
    CA_C_ID,
    CA_NAME,
    CA_TAX_ST,
    CA_BAL
};

enum {
    CX_TX_ID,
    CX_C_ID
};

enum {
    H_T_ID,
    H_CA_ID,
    H_S_SYMB,
    H_DTS,
    H_PRICE,
    H_QTY
};

enum {
    HH_H_T_ID,
    HH_T_ID,
    HH_BEFORE_QTY,
    HH_AFTER_QTY
};

enum {
    HS_CA_ID,
    HS_S_SYMB,
    HS_QTY
};

enum {
    WI_WL_ID,
    WI_S_SYMB
};

enum {
    WL_ID,
    WL_C_ID
};

enum {
    B_ID,
    B_ST_ID,
    B_NAME,
    B_NUM_TRADES,
    B_COMM_TOTAL
};

enum {
    CT_T_ID,
    CT_DTS,
    CT_AMT,
    CT_NAME
};

enum {
    CH_TT_ID,
    CH_C_TIER,
    CH_CHRG
};

enum {
    CR_C_TIER,
    CR_TT_ID,
    CR_EX_ID,
    CR_FROM_QTY,
    CR_TO_QTY,
    CR_RATE
};

enum {
    SE_T_ID,
    SE_CASH_TYPE,
    SE_CASH_DUE_DATE,
    SE_AMT
};

enum {
    T_ID,
    T_DTS,
    T_ST_ID,
    T_TT_ID,
    T_IS_CASH,
    T_S_SYMB,
    T_QTY,
    T_BID_PRICE,
    T_CA_ID,
    T_EXEC_NAME,
    T_TRADE_PRICE,
    T_CHRG,
    T_COMM,
    T_TAX,
    T_LIFO
};

enum {
    TH_T_ID,
    TH_DTS,
    TH_ST_ID
};

enum {
    TR_T_ID,
    TR_TT_ID,
    TR_S_SYMB,
    TR_QTY,
    TR_BID_PRICE,
    TR_B_ID
};

enum {
    TT_ID,
    TT_NAME,
    TT_IS_SELL,
    TT_IS_MRKT
};

enum {
    CO_ID,
    CO_ST_ID,
    CO_NAME,
    CO_IN_ID,
    CO_SP_RATE,
    CO_CEO,
    CO_AD_ID,
    CO_DESC,
    CO_OPEN_DATE
};

enum {
    CP_CO_ID,
    CP_COMP_CO_ID,
    CP_IN_ID
};

enum {
    DM_DATE,
    DM_S_SYMB,
    DM_CLOSE,
    DM_HIGH,
    DM_LOW,
    DM_VOL
};

enum {
    EX_ID,
    EX_NAME,
    EX_NUM_SYMB,
    EX_OPEN,
    EX_CLOSE,
    EX_DESC,
    EX_AD_ID
};

enum {
    FI_CO_ID,
    FI_YEAR,
    FI_QTR,
    FI_QTR_START_DATE,
    FI_REVENUE,
    FI_NET_EARN,
    FI_BASIC_EPS,
    FI_DILUT_EPS,
    FI_MARGIN,
    FI_INVENTORY,
    FI_ASSETS,
    FI_LIABILITY,
    FI_OUT_BASIC,
    FI_OUT_DILUT
};

enum {
    IN_ID,
    IN_NAME,
    IN_SC_ID
};

enum {
    LT_S_SYMB,
    LT_DTS,
    LT_PRICE,
    LT_OPEN_PRICE,
    LT_VOL
};

enum {
    NI_ID,
    NI_HEADLINE,
    NI_SUMMARY,
    NI_ITEM,
    NI_DTS,
    NI_SOURCE,
    NI_AUTHOR
};

enum {
    NX_NI_ID,
    NX_CO_ID
};

enum {
    SC_ID,
    SC_NAME
};

enum {
    S_SYMB,
    S_ISSUE,
    S_ST_ID,
    S_NAME,
    S_EX_ID,
    S_CO_ID,
    S_NUM_OUT,
    S_START_DATE,
    S_EXCH_DATE,
    S_PE,
    S_52WK_HIGH,
    S_52WK_HIGH_DATE,
    S_52WK_LOW,
    S_52WK_LOW_DATE,
    S_DIVIDEND,
    S_YIELD
};

enum {
    AD_ID,
    AD_LINE1,
    AD_LINE2,
    AD_ZC_CODE,
    AD_CTRY
};

enum {
    ST_ID,
    ST_NAME
};

enum {
    TX_ID,
    TX_NAME,
    TX_RATE
};

enum {
    ZC_CODE,
    ZC_TOWN,
    ZC_DIV
};

// Other constants

enum {
    TMB,
    TMS,
    TSL,
    TLS,
    TLB
};

enum {
    ACTV,
    CMPT,
    CNCL,
    PNDG,
    SBMT
};

#define INVALID_STATUS 6