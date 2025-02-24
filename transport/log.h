#pragma once

#include "global.h"
#include "helper.h"

enum LogType {
    LOG_PREPARE,
    LOG_LOCAL_COMMIT,
    LOG_DIST_COMMIT,
    LOG_ABORT,
    LOG_CALVIN
};

struct LogRecord {
    size_t   size;
    uint64_t txn_id;
    LogType  type;
    char     data[];
};