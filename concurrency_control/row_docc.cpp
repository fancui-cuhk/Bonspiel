#include "row_docc.h"
#include "row.h"
#include "txn.h"
#include <mm_malloc.h>
#include "docc_manager.h"
#include "manager.h"
#include "stdlib.h"
#include "table.h"

#if CC_ALG == DOCC

bool
Row_docc::CompareWait::operator() (TxnManager * en1, TxnManager * en2) const
{
    return MAN(en1)->get_priority() < MAN(en2)->get_priority();
}

#if MULTIPLE_PRIORITY_LEVELS
bool
Row_docc::CompareReservePriority::operator() (ReserveWait * rw1, ReserveWait * rw2) const
{
    // Currently MULTIPLE_PRIORITY_LEVELS only works with BONSPIEL
    assert(ACCESS_METHOD == BONSPIEL);

    // assert(rw1->txn->is_mp_txn());
    // assert(rw2->txn->is_mp_txn());

    // Txns with the highest priority is ordered in the front
    return rw1->txn->get_reserve_priority() > rw2->txn->get_reserve_priority();
}
#endif

Row_docc::Row_docc(row_t * row)
{
    _row = row;
    _wts = 0;
    _rts = 0;
#if EARLY_WRITE_INSTALL
    _fresh_flag = false;
#endif
    _reserve_type = NA;
    _reserve_set.clear();
    _reserve_waiting_set.clear();
    _deleted = false;
    _delete_timestamp = 0;
    _lock_owner = NULL;
    pthread_mutex_init( &_latch, NULL );
}

// This function reads _row->data into data
RC
Row_docc::read(TxnManager * txn, char * data, uint64_t & wts, uint64_t & rts, bool latch)
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
Row_docc::write(TxnManager * txn, char * data, uint64_t wts, bool latch)
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
    // The row is updated, clear existing reservations
    _reserve_set.clear();
    _reserve_type = NA;

    if (latch)
        this->unlatch();
}

// This function reads _row->data into data after trying to reserve the record
// Reservation might fail here
RC
Row_docc::reserve_and_read(TxnManager * txn, char * data, uint64_t & wts, uint64_t & rts, access_t reser_type, bool latch)
{
    assert(reser_type == RD || reser_type == WR);

    if (latch)
        this->latch();

    if (_lock_owner != NULL) {
        // The record is locked now
    #if EARLY_WRITE_INSTALL
        // Register the txn in the waiting set
        MAN(txn)->set_txn_waiting();
        ReserveWait * rw = new ReserveWait;
        rw->txn = txn;
        rw->type = reser_type;
        _reserve_waiting_set.insert(rw);
        if (!_fresh_flag) {
            // The txn waits only when the record's fresh flag is not set
            if (latch)
                this->unlatch();
            return WAIT;
        } else {
            // The record is still locked, skip the reservation, directly read
            goto read;
        }
    #else
        // No optimization, just wait
        MAN(txn)->set_txn_waiting();
        ReserveWait * rw = new ReserveWait;
        rw->txn = txn;
        rw->type = reser_type;
        _reserve_waiting_set.insert(rw);
        if (latch)
            this->unlatch();
        return WAIT;
    #endif
    }

#if MULTIPLE_PRIORITY_LEVELS
    if (!_reserve_set.empty()) {
        // The record is reserved now
        auto it = _reserve_set.begin();
        uint32_t reserve_priority = (*it)->get_reserve_priority();

        if (reserve_priority < txn->get_reserve_priority()) {
            // The record is reserved by lower-priority txns
            // Preempt the reservation
            _reserve_set.clear();
            _reserve_set.insert(txn);
            _reserve_type = reser_type;
        } else if (reserve_priority == txn->get_reserve_priority()) {
            // The record is reserved by equal-priority txns
            // Join the reservation set
            _reserve_set.insert(txn);
            _reserve_type = max(_reserve_type, reser_type);
        } else {
            // The record is reserved by higher-priority txns
            // Reservation fails, do nothing
        #if ABORT_AFTER_WRITE_RESERVATION_FAIL
            if (reser_type == WR)
                return ABORT;
        #endif
        }
    } else {
        _reserve_set.insert(txn);
        _reserve_type = reser_type;
    }
#else
    _reserve_set.insert(txn);
    _reserve_type = max(_reserve_type, reser_type);
#endif

#if EARLY_WRITE_INSTALL
read:
#endif

    wts = _wts;
    rts = _rts;
    if (data)
        memcpy(data, _row->get_data(), _row->get_tuple_size());

    if (latch)
        this->unlatch();
    return RCOK;
}

// This function unreserves a record if the record is unchanged
void
Row_docc::unreserve(TxnManager * txn, uint64_t wts, bool latch)
{
    if (wts != _wts)
        return;

    if (latch)
        this->latch();

    if (wts == _wts) {
        // The record is not updated, unreserve it
        _reserve_set.erase(txn);
        if (_reserve_set.empty())
            _reserve_type = NA;
    }

    if (latch)
        this->unlatch();
}

// This function tries to renew _rts to be rts
bool
Row_docc::try_renew(uint64_t wts, uint64_t rts, TxnManager * txn)
{
    if (wts != _wts) {
        // The record has been changed
        INC_INT_STATS(int_aborts_rs1, 1);
        return false;
    }

    if (!txn->is_mp_txn()) {
        // Local txns cannot renew timestamp for records reserved by distributed ones
        if (!_reserve_set.empty() && _reserve_type == WR) {
            INC_INT_STATS(int_aborts_rs2, 1);
            return false;
        }
    }

    if (rts <= _rts)
        return true;

    latch();

    if (wts != _wts) {
        unlatch();
        INC_INT_STATS(int_aborts_rs1, 1);
        return false;
    }

    if (!txn->is_mp_txn()) {
        if (!_reserve_set.empty() && _reserve_type == WR) {
            unlatch();
            INC_INT_STATS(int_aborts_rs2, 1);
            return false;
        }
    }

    if (rts <= _rts) {
        unlatch();
        return true;
    }

    if (_lock_owner != NULL && _lock_owner != txn) {
        unlatch();
        INC_INT_STATS(int_aborts_rs3, 1);
        if (txn->is_mp_txn()) {
            if (_lock_owner != NULL) {
                if (_lock_owner->is_mp_txn()) {
                    INC_INT_STATS(dist_txn_abort_dist, 1);
                } else {
                    INC_INT_STATS(dist_txn_abort_local, 1);
                }
            }
        } else {
            if (_lock_owner != NULL) {
                if (_lock_owner->is_mp_txn()) {
                    INC_INT_STATS(local_txn_abort_dist, 1);
                } else {
                    INC_INT_STATS(local_txn_abort_local, 1);
                }
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
Row_docc::set_ts(uint64_t wts, uint64_t rts)
{
    latch();
    _wts = wts;
    _rts = rts;
    assert(_rts >= _wts);
    unlatch();
}

// this function notifies txns in _reserve_waiting_set that they can go ahead to take reservation
void
Row_docc::notify_reserve_waiting_set()
{
    this->latch();

  #if MULTIPLE_PRIORITY_LEVELS
    if (_reserve_waiting_set.size() > 0) {
        auto it = _reserve_waiting_set.begin();
        uint32_t reserve_priority = (*it)->txn->get_reserve_priority();

        for (; it != _reserve_waiting_set.end(); it++) {
            TxnManager * wakeup_txn = (*it)->txn;
            access_t type = (*it)->type;
            if (wakeup_txn->get_txn_state() != TxnManager::RUNNING) {
                continue;
            }
            if (wakeup_txn->get_reserve_priority() == reserve_priority) {
                _reserve_set.insert(wakeup_txn);
                _reserve_type = max(_reserve_type, type);
            }
            wakeup_txn->set_txn_ready(RCOK);
            if (wakeup_txn->is_txn_ready()) {
                uint32_t queue_id = glob_manager->txnid_to_server_thread(wakeup_txn->get_txn_id());
                ready_queues[ queue_id ]->push(wakeup_txn->get_txn_id());
            }
            delete (*it);
        }
        _reserve_waiting_set.clear();
    }
  #else
    if (_reserve_waiting_set.size() > 0) {
        for (auto it = _reserve_waiting_set.begin(); it != _reserve_waiting_set.end(); it++) {
            TxnManager * wakeup_txn = (*it)->txn;
            access_t type = (*it)->type;
            if (wakeup_txn->get_txn_state() != TxnManager::RUNNING) {
                continue;
            }
            _reserve_set.insert(wakeup_txn);
            _reserve_type = max(_reserve_type, type);
            wakeup_txn->set_txn_ready(RCOK);
            if (wakeup_txn->is_txn_ready()) {
                uint32_t queue_id = glob_manager->txnid_to_server_thread(wakeup_txn->get_txn_id());
                ready_queues[ queue_id ]->push(wakeup_txn->get_txn_id());
            }
            delete (*it);
        }
        _reserve_waiting_set.clear();
    }
  #endif

    this->unlatch();
}

// This function tries to lock the row for txn
RC
Row_docc::lock(TxnManager * txn)
{
    RC rc = RCOK;

    if (!txn->is_applier()) {
        // The acquiring txn is the coordinator or a partition leader
        if (MAN(txn)->get_commit_ts() <= _rts) {
            // txn->_min_commit_ts <= _rts so txn cannot lock and write this row
            INC_INT_STATS(int_aborts_ws1, 1);
            return ABORT;
        }

        if (!txn->is_mp_txn()) {
            if (_reserve_set.size() > 0) {
                // Local txns cannot lock reserved records
                return ABORT;
            }
        }

        if (_lock_owner == txn)
            return RCOK;

        latch();

        if (MAN(txn)->get_commit_ts() <= _rts) {
            // After latching, check again for _rts violation
            unlatch();
            INC_INT_STATS(int_aborts_ws1, 1);
            return ABORT;
        }

        if (!txn->is_mp_txn()) {
            if (_reserve_set.size() > 0) {
                // After latching, check again for reservation
                unlatch();
                return ABORT;
            }
        }

    #if OCC_LOCK_TYPE == NO_WAIT
        if (_lock_owner != NULL && _lock_owner != txn) {
            // No waiting is allowed
            if (txn->is_mp_txn()) {
                if (_lock_owner != NULL) {
                    if (_lock_owner->is_mp_txn()) {
                        INC_INT_STATS(dist_txn_abort_dist, 1);
                    } else {
                        INC_INT_STATS(dist_txn_abort_local, 1);
                    }
                }
            } else {
                if (_lock_owner != NULL) {
                    if (_lock_owner->is_mp_txn()) {
                        INC_INT_STATS(local_txn_abort_dist, 1);
                    } else {
                        INC_INT_STATS(local_txn_abort_local, 1);
                    }
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

// This function tries to release the lock on the record held by txn
void
Row_docc::release(TxnManager * txn, RC rc)
{
#if OCC_LOCK_TYPE == NO_WAIT
    if (_lock_owner != txn)
        return;

    this->latch();
    assert(_lock_owner == txn);
    _lock_owner = NULL;
  #if EARLY_WRITE_INSTALL
    // Unset the fresh flag when releasing lock
    _fresh_flag = false;
  #endif
    this->unlatch();

    notify_reserve_waiting_set();
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
Row_docc::delete_row(uint64_t del_ts)
{
    latch();
    _deleted = true;
    _delete_timestamp = del_ts;
    unlatch();
}

// This function updates _rts to rts if _wts == wts
bool
Row_docc::update_rts(uint64_t wts, uint64_t rts)
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
Row_docc::update_data(uint64_t wts, char *data) {
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
