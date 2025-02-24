#include "row_sundial.h"
#include "row.h"
#include "txn.h"
#include <mm_malloc.h>
#include "sundial_manager.h"
#include "manager.h"
#include "stdlib.h"
#include "table.h"

#if CC_ALG == SUNDIAL

bool
Row_sundial::CompareWait::operator() (TxnManager * en1, TxnManager * en2) const
{
    return MAN(en1)->get_priority() < MAN(en2)->get_priority();
}

Row_sundial::Row_sundial(row_t * row)
{
    _row = row;
    _wts = 0;
    _rts = 0;
    _deleted = false;
    _delete_timestamp = 0;
    _lock_owner = NULL;
    pthread_mutex_init( &_latch, NULL );
}

// This function reads _row->data into data
RC
Row_sundial::read(TxnManager * txn, char * data, uint64_t & wts, uint64_t & rts, bool latch)
{
    if (latch)
        this->latch();

    wts = _wts;
    rts = _rts;
    if (data)
        memcpy(data, _row->get_data(), _row->get_tuple_size());

    if (latch)
        this->unlatch();
    return RCOK;
}

// This function installs data into _row->data
void
Row_sundial::write(TxnManager * txn, char * data, uint64_t wts, bool latch)
{
    if (latch)
        this->latch();

    assert(!_deleted || wts < _delete_timestamp);
    if (g_role == ROLE_LEADER) {
        assert(wts > _wts && wts > _rts);
    } else {
        if (wts <= _rts) {
            if (latch)
                this->unlatch();
            return;
        }
    }
    assert(txn == _lock_owner);

    _wts = wts;
    _rts = wts;
    _row->copy(data);

    if (latch)
        this->unlatch();
}

// This function tries to renew _rts to be rts
bool
Row_sundial::try_renew(uint64_t wts, uint64_t rts, TxnManager * txn)
{
    if (wts != _wts) {
        INC_INT_STATS(int_aborts_rs1, 1);
        return false;
    }

    latch();

    if (wts != _wts) {
        unlatch();
        INC_INT_STATS(int_aborts_rs2, 1);
        return false;
    }

    if (rts <= _rts) {
        unlatch();
        return true;
    }

    if (_lock_owner != NULL && _lock_owner != txn) {
        unlatch();
        if (txn->is_mp_txn()) {
            if (_lock_owner->is_mp_txn()) {
                INC_INT_STATS(dist_txn_abort_dist, 1);
            } else {
                INC_INT_STATS(dist_txn_abort_local, 1);
            }
        } else {
            if (_lock_owner->is_mp_txn()) {
                INC_INT_STATS(local_txn_abort_dist, 1);
            } else {
                INC_INT_STATS(local_txn_abort_local, 1);
            }
        }
        return false;
    }

    if (_deleted) {
        bool success = (wts == _wts && rts < _delete_timestamp);
        unlatch();
        return success;
    }

    _rts = rts;
    assert(_rts >= _wts);
    unlatch();
    return true;
}

void
Row_sundial::set_ts(uint64_t wts, uint64_t rts)
{
    latch();
    _wts = wts;
    _rts = rts;
    assert(_rts >= _wts);
    unlatch();
}

// This function tries to lock the row for txn
RC
Row_sundial::lock(TxnManager * txn)
{
    RC rc = RCOK;

    if (!txn->is_applier()) {
        if (_lock_owner == txn)
            return RCOK;

        latch();

    #if OCC_LOCK_TYPE == NO_WAIT
        if (_lock_owner != NULL && _lock_owner != txn) {
            // No waiting is allowed
            if (txn->is_mp_txn()) {
                if (_lock_owner->is_mp_txn()) {
                    INC_INT_STATS(dist_txn_abort_dist, 1);
                } else {
                    INC_INT_STATS(dist_txn_abort_local, 1);
                }
            } else {
                if (_lock_owner->is_mp_txn()) {
                    INC_INT_STATS(local_txn_abort_dist, 1);
                } else {
                    INC_INT_STATS(local_txn_abort_local, 1);
                }
            }
            rc = ABORT;
        } else {
            _lock_owner = txn;
            rc = RCOK;
        }
    #else
        if (_lock_owner == NULL || _lock_owner == txn) {
            _lock_owner = txn;
            rc = RCOK;
        } else {
            M_ASSERT(txn->get_txn_id() != _lock_owner->get_txn_id(), "txn=%ld, _lock_owner=%ld. ID=%ld\n", (uint64_t)txn, (uint64_t)_lock_owner, txn->get_txn_id());
            if (_lock_waiting_set.size() >= g_max_num_waits) {
                rc = ABORT;
            } else if (MAN(txn)->get_priority() < MAN(_lock_owner)->get_priority()) {
                // Allow waiting if the acquiring transaction (txn) has a higher priority
                MAN(txn)->set_txn_waiting();
                _lock_waiting_set.insert(txn);
                rc = WAIT;
            } else {
                INC_INT_STATS(int_aborts_ws2, 1);
                rc = ABORT;
            }
        }
    #endif
        unlatch();
    } else {
        // The acquiring txn is an applier
        if (MAN(txn)->get_commit_ts() < _wts)
            return FINISH;

        if (_lock_owner == txn)
            return RCOK;

        latch();
        if (MAN(txn)->get_commit_ts() < _wts) {
            // txn->_min_commit_ts < _wts means that some later log has been applied
            // so no need to apply this one
            unlatch();
            return FINISH;
        }

        // assert(MAN(txn)->get_commit_ts() > _rts);
        if (_lock_owner == NULL || _lock_owner == txn) {
            _lock_owner = txn;
            rc = RCOK;
        } else {
            // No need to enforce waiting order on appliers
            MAN(txn)->set_txn_waiting();
            _lock_waiting_set.insert(txn);
            rc = WAIT;
        }
        unlatch();
    }

    return rc;
}

// This function tries to release the lock on the row held by txn
void
Row_sundial::release(TxnManager * txn, RC rc)
{
#if OCC_LOCK_TYPE == NO_WAIT
    if (_lock_owner != txn)
        return;

    this->latch();

    assert(_lock_owner == txn);
    _lock_owner = NULL;

    this->unlatch();
#else
    TxnManager * wakeup_txn = NULL;
    this->latch();
    if (_lock_owner != txn) {
        _lock_waiting_set.erase( txn );
        this->unlatch();
        return;
    }

    if (_lock_waiting_set.size() > 0) {
        // _lock_waiting_set is sorted in the ascending order of priority
        auto last_it = _lock_waiting_set.end();
        last_it--;
        TxnManager * next = *last_it;
        _lock_owner = next;
        assert(MAN(next)->get_priority() < MAN(txn)->get_priority() || txn->is_applier());
        next->set_txn_ready(RCOK);
        wakeup_txn = next;
        _lock_waiting_set.erase(next);
    } else {
        _lock_owner = NULL;
    }
    if (wakeup_txn && wakeup_txn->is_txn_ready()) {
        uint32_t queue_id = glob_manager->txnid_to_server_thread(wakeup_txn->get_txn_id());
        M_ASSERT (wakeup_txn->get_txn_state() == TxnManager::RUNNING || wakeup_txn->get_txn_state() == TxnManager::PREPARING,
                "Invalid transaction state: %d\n", wakeup_txn->get_txn_state());
        ready_queues[ queue_id ]->push(wakeup_txn->get_txn_id());
    }

    this->unlatch();
#endif
}

// This function deletes the row at timestamp del_ts
void
Row_sundial::delete_row(uint64_t del_ts)
{
    latch();
    _deleted = true;
    _delete_timestamp = del_ts;
    unlatch();
}

// This function updates _rts to rts if _wts == wts
bool
Row_sundial::update_rts(uint64_t wts, uint64_t rts)
{
    if (wts < _wts)
        return true;
    if (wts > _wts)
        return false;

    latch();
    if (wts > _wts) {
        unlatch();
        return false;
    } else if (wts < _wts) {
        unlatch();
        return true;
    } else {
        assert(_wts == wts);
        if (rts > _rts)
            _rts = rts;
    }
    unlatch();
    return true;
}

// This function updates _row->data to be data (similar to write)
bool
Row_sundial::update_data(uint64_t wts, char *data) {
    assert(wts != _wts);
    if (wts < _wts)
        return true;
    latch();
    assert(wts > _wts);
    _row->copy(data);
    _wts = wts;
    _rts = wts;
    unlatch();
    return true;
}

#endif
