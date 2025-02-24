#pragma once

#include "global.h"

#if CC_ALG == SILO

class TxnManager;
class CCManager;
class SiloManager;
class row_t;

class Row_silo {
public:
#if MULTI_VERSION
    static int          _history_num;
#endif
    Row_silo() : Row_silo(NULL) {};
    Row_silo(row_t * row);

    RC                 read(TxnManager * txn, char * data, uint64_t &wts, bool latch = true);
    void               write(TxnManager * txn, char * data, uint64_t wts, bool latch = true);
    RC                 update(char * data, uint64_t wts);

    void               latch() { pthread_mutex_lock(&_latch); }
    void               unlatch() { pthread_mutex_unlock(&_latch); }

    RC                 lock(TxnManager * txn);
    void               release(TxnManager * txn, RC rc);

    uint64_t           get_wts() { return _wts; }
    void               set_wts(uint64_t wts) { _wts = wts; };

    bool               is_deleted() { return _deleted; }
    void               delete_row(uint64_t del_ts);

    row_t *             _row;
    pthread_mutex_t     _latch;
    uint64_t            _wts;

    TxnManager *        _lock_owner;
    #define MAN(txn) ((SiloManager *) (txn)->get_cc_manager())
    struct CompareWait {
        bool operator() (TxnManager * en1, TxnManager * en2) const;
    };

#if OCC_LOCK_TYPE == WAIT_DIE
    std::set<TxnManager *, CompareWait> _waiting_set;
#endif

    bool                _deleted;
    uint64_t            _delete_timestamp;
};
// __attribute__ ((aligned(64)));

#endif
