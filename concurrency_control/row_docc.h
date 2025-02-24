#pragma once

#include "global.h"

#if CC_ALG == DOCC

class TxnManager;
class CCManager;
class DoccManager;
class row_t;

class Row_docc {
public:
#if MULTI_VERSION
    static int          _history_num;
#endif
    Row_docc() : Row_docc(NULL) {};
    Row_docc(row_t * row);

    RC                  read(TxnManager * txn, char * data, uint64_t & wts, uint64_t & rts, bool latch = true);
    void                write(TxnManager * txn, char * data, uint64_t wts, bool latch = true);

    RC                  reserve_and_read(TxnManager * txn, char * data, uint64_t & wts, uint64_t & rts, access_t reser_type, bool latch = true);
    void                unreserve(TxnManager * txn, uint64_t wts, bool latch = true);

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

    void                notify_reserve_waiting_set();

    row_t *             _row;
    pthread_mutex_t     _latch;
    uint64_t            _wts;
    uint64_t            _rts;
#if EARLY_WRITE_INSTALL
    bool                _fresh_flag;
#endif

    TxnManager *        _lock_owner;
    #define MAN(txn) ((DoccManager *) (txn)->get_cc_manager())

    // If OCC_LOCK_TYPE is NO_WAIT, _lock_waiting_set is used on appliers
    struct CompareWait {
        bool operator() (TxnManager * en1, TxnManager * en2) const;
    };
    set<TxnManager *, CompareWait> _lock_waiting_set;

    // No need to enforce order here
    access_t                       _reserve_type;
    unordered_set<TxnManager *>    _reserve_set;
    struct ReserveWait {
        TxnManager * txn;
        access_t     type;
    };

#if MULTIPLE_PRIORITY_LEVELS
    struct CompareReservePriority {
        bool operator() (ReserveWait * rw1, ReserveWait * rw2) const;
    };
    set<ReserveWait *, CompareReservePriority> _reserve_waiting_set;
#else
    unordered_set<ReserveWait *>   _reserve_waiting_set;
#endif

    bool                           _deleted;
    uint64_t                       _delete_timestamp;
};
// __attribute__ ((aligned(64)));

#endif
