#include "row_silo.h"
#include "row.h"
#include "txn.h"
#include <mm_malloc.h>
#include "silo_manager.h"
#include "manager.h"
#include "stdlib.h"
#include "table.h"

#if CC_ALG==SILO

#if OCC_LOCK_TYPE == WAIT_DIE || OCC_WAW_LOCK
bool
Row_silo::CompareWait::operator() (TxnManager * en1, TxnManager * en2) const
{
    return MAN(en1)->get_priority() < MAN(en2)->get_priority();
}
#endif

Row_silo::Row_silo(row_t * row)
{
    _row = row;
    _wts = 0;
    _deleted = false;
    _delete_timestamp = 0;
    pthread_mutex_init( &_latch, NULL );
    _lock_owner = NULL;
}

RC
Row_silo::read(TxnManager * txn, char * data, uint64_t &wts, bool latch)
{
    if (latch)
        this->latch();
   
    wts = _wts;
    if (data)
        memcpy(data, _row->get_data(), _row->get_tuple_size());

    if (latch)
        this->unlatch();
    return RCOK;
}

RC
Row_silo::update(char * data, uint64_t wts)
{
    if (wts > _wts) {
        this->latch();
        if (wts > _wts) {
            _wts = wts;
            _row->set_data(data);
        }
        this->unlatch();
    }
    return RCOK;
}

void
Row_silo::write(TxnManager * txn, char * data, uint64_t wts, bool latch)
{
    if (latch)
        this->latch();

    assert(!_deleted || wts < _delete_timestamp);
    assert(wts > _wts);
    assert(txn == _lock_owner);

    _wts = wts;
    _row->copy(data);

    if (latch)
        this->unlatch();
}

RC
Row_silo::lock(TxnManager * txn)
{
    RC rc = RCOK;
    this->latch();
#if OCC_LOCK_TYPE == NO_WAIT
    if (_lock_owner != NULL && _lock_owner != txn) {
        rc = ABORT;
    } else {
        _lock_owner = txn;
        rc = RCOK;
    }
#elif OCC_LOCK_TYPE == WAIT_DIE
    if (_lock_owner == NULL || _lock_owner == txn) {
        _lock_owner = txn;
        rc = RCOK;
    } else {
        M_ASSERT(txn->get_txn_id() != _lock_owner->get_txn_id(), "txn=%ld, _lock_owner=%ld. ID=%ld\n", (uint64_t)txn, (uint64_t)_lock_owner, txn->get_txn_id());
        // txn has higher priority, should wait.
        if (_waiting_set.size() >= g_max_num_waits)
            rc = ABORT;
        else if (MAN(txn)->get_priority() < MAN(_lock_owner)->get_priority()) {
            MAN(txn)->set_txn_waiting();
            _waiting_set.insert(txn);
            rc = WAIT;
        } else
            rc = ABORT;
    }
#endif
    this->unlatch();
    return rc;
}

void
Row_silo::release(TxnManager * txn, RC rc)
{
#if OCC_LOCK_TYPE == NO_WAIT
    if (_lock_owner != txn)
        return;
    this->latch();
    assert(_lock_owner == txn);
    _lock_owner = NULL;
    this->unlatch();
#elif OCC_LOCK_TYPE == WAIT_DIE
    TxnManager* wakeup_txn = NULL;
    this->latch();
    if (_lock_owner != txn) {
        _waiting_set.erase( txn );
        this->unlatch();
        return;
    }

    if (_waiting_set.size() > 0) {
        auto last_it = _waiting_set.end();
        last_it --;
        TxnManager * next = *last_it;
        _lock_owner = next;
        assert(MAN(next)->get_priority() < MAN(txn)->get_priority());
        next->set_txn_ready(RCOK);
        wakeup_txn = next;
        _waiting_set.erase(next);
    } else {
        _lock_owner = NULL;
    }

    if (wakeup_txn && wakeup_txn->is_txn_ready()) {
        uint32_t queue_id = glob_manager->txnid_to_server_thread(wakeup_txn->get_txn_id());
        ready_queues[ queue_id ]->push(wakeup_txn->get_txn_id());
    }
    this->unlatch();
#endif
}

void
Row_silo::delete_row(uint64_t del_ts)
{
    latch();
    _deleted = true;
    _delete_timestamp = del_ts;
    unlatch();
}

#endif
