#pragma once

#include "global.h"

#if CC_ALG == SUNDIAL

class TxnManager;
class CCManager;
class SundialManager;
class row_t;

class Row_sundial {
public:
    Row_sundial() : Row_sundial(NULL) {};
    Row_sundial(row_t * row);

    RC                  read(TxnManager * txn, char * data, uint64_t & wts, uint64_t & rts, bool latch = true);
    void                write(TxnManager * txn, char * data, uint64_t wts, bool latch = true);

    void                latch() { pthread_mutex_lock(&_latch); }
    void                unlatch() { pthread_mutex_unlock(&_latch); }

    RC                  lock(TxnManager * txn);
    void                release(TxnManager * txn, RC rc);

    uint64_t            get_wts() { return _wts; }
    uint64_t            get_rts() { return _rts; }
    void                set_ts(uint64_t wts, uint64_t rts);
    bool                try_renew(uint64_t wts, uint64_t rts, TxnManager *txn);

    bool                is_deleted() { return _deleted; }
    void                delete_row(uint64_t del_ts);

    bool                update_rts(uint64_t wts, uint64_t rts);
    bool                update_data(uint64_t wts, char *data);

    row_t *             _row;
    pthread_mutex_t     _latch;
    uint64_t            _wts;
    uint64_t            _rts;

    TxnManager *        _lock_owner;
    #define MAN(txn) ((SundialManager *) (txn)->get_cc_manager())

    // If OCC_LOCK_TYPE is NO_WAIT, _lock_waiting_set is used on appliers
    struct CompareWait {
        bool operator() (TxnManager * en1, TxnManager * en2) const;
    };
    set<TxnManager *, CompareWait> _lock_waiting_set;

    bool                           _deleted;
    uint64_t                       _delete_timestamp;
};

#endif
