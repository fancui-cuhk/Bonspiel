#include "row.h"
#include "txn.h"
#include "row_lock.h"
#include "manager.h"
#include "lock_manager.h"

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == WOUND_WAIT || CC_ALG == CALVIN

#if CC_ALG == WAIT_DIE

#if PRIORITY_LOCK

bool
Row_lock::CompareWait::operator() (const WaitEntry &en1, const WaitEntry &en2) const
{
    if (en1.txn->is_mp_txn() != en2.txn->is_mp_txn())
        return en1.txn->is_mp_txn() < en2.txn->is_mp_txn();
    return LOCK_MAN(en1.txn)->get_priority() < LOCK_MAN(en2.txn)->get_priority();
}

bool
Row_lock::CompareLock::operator() (TxnManager * txn1, TxnManager * txn2) const
{
    if (txn1->is_mp_txn() != txn2->is_mp_txn())
        return txn1->is_mp_txn() < txn2->is_mp_txn();
    return LOCK_MAN(txn1)->get_priority() < LOCK_MAN(txn2)->get_priority();
}

#else

bool
Row_lock::CompareWait::operator() (const WaitEntry &en1, const WaitEntry &en2) const
{
    return LOCK_MAN(en1.txn)->get_priority() < LOCK_MAN(en2.txn)->get_priority();
}

bool
Row_lock::CompareLock::operator() (TxnManager * txn1, TxnManager * txn2) const
{
    return LOCK_MAN(txn1)->get_priority() < LOCK_MAN(txn2)->get_priority();
}

#endif

#endif

#if CC_ALG == WOUND_WAIT

#if PRIORITY_LOCK

bool
Row_lock::CompareWait::operator() (const WaitEntry &en1, const WaitEntry &en2) const
{
    if (en1.txn->is_mp_txn() != en2.txn->is_mp_txn())
        return en1.txn->is_mp_txn() > en2.txn->is_mp_txn();
    return LOCK_MAN(en1.txn)->get_priority() > LOCK_MAN(en2.txn)->get_priority();
}

bool
Row_lock::CompareLock::operator() (TxnManager * txn1, TxnManager * txn2) const
{
    if (txn1->is_mp_txn() != txn2->is_mp_txn())
        return txn1->is_mp_txn() > txn2->is_mp_txn();
    return LOCK_MAN(txn1)->get_priority() > LOCK_MAN(txn2)->get_priority();
}

#else

bool
Row_lock::CompareWait::operator() (const WaitEntry &en1, const WaitEntry &en2) const
{
    return LOCK_MAN(en1.txn)->get_priority() > LOCK_MAN(en2.txn)->get_priority();
}

bool
Row_lock::CompareLock::operator() (TxnManager * txn1, TxnManager * txn2) const
{
    return LOCK_MAN(txn1)->get_priority() > LOCK_MAN(txn2)->get_priority();
}

#endif

#endif

Row_lock::Row_lock()
{
    _row = NULL;
    pthread_mutex_init(&_latch, NULL);
    _lock_type = LOCK_NONE;
    _max_num_waits = g_max_num_waits;
    _upgrading_txn = NULL;
}

Row_lock::Row_lock(row_t * row)
    : Row_lock()
{
    _row = row;
}

void
Row_lock::latch()
{
    pthread_mutex_lock( &_latch );
}

void
Row_lock::unlatch()
{
    pthread_mutex_unlock( &_latch );
}

void
Row_lock::init(row_t * row)
{
    _row = row;
    pthread_mutex_init(&_latch, NULL);
    _lock_type = LOCK_NONE;
      _max_num_waits = g_max_num_waits;
}

static inline void
_wakeup_txn(TxnManager *txn)
{
    if (txn->is_txn_ready() && 
       (txn->get_txn_state() == TxnManager::RUNNING || txn->get_txn_state() == TxnManager::PREPARING)) {
        uint32_t queue_id = glob_manager->txnid_to_server_thread(txn->get_txn_id());
        ready_queues[ queue_id ]->push(txn->get_txn_id());
    }
}

RC
Row_lock::lock_get(LockType type, TxnManager * txn, bool need_latch)
{
    RC rc = RCOK;

    if (!txn->is_applier()) {
        // The acquiring txn is the coordinator or a partition leader
        if (need_latch)
            pthread_mutex_lock( &_latch );
        if (_locking_set.find(txn) != _locking_set.end()) {
            // This is not in use now, as there are no lock upgrading requests
            // The acquiring txn already holds lock on the record
            if (_lock_type != type) {
                if (_lock_type == LOCK_SH) {
                    // Upgrade SH lock to EX lock
                    assert(type == LOCK_EX);
                    _upgrading_txn = txn;
                    if (_locking_set.size() == 1) {
                        // No other SH-lock owners, directly upgrade
                        _lock_type = LOCK_EX;
                        rc = RCOK;
                    } else {
                    #if CC_ALG == NO_WAIT
                        rc = ABORT;
                    #elif CC_ALG == WAIT_DIE
                        _lock_type = LOCK_UPGRADING;
                        rc = WAIT;
                    #elif CC_ALG == WOUND_WAIT
                        bool can_wound = true;
                        for (auto it = _locking_set.begin(); it != _locking_set.end(); it++) {
                            if (LOCK_MAN(txn)->get_priority() > LOCK_MAN(*it)->get_priority() || LOCK_MAN(*it)->get_cc_state() > LockManager::CC_PREPARING) {
                                can_wound = false;
                                break;
                            }
                        }
                        if (can_wound) {
                            for (auto it = _locking_set.begin(); it != _locking_set.end();) {
                                if (*it != txn) {
                                    if (LOCK_MAN(*it)->get_cc_state() > LockManager::CC_PREPARING) {
                                        // This txn cannot be wounded
                                        goto wait;
                                    }
                                    LOCK_MAN(*it)->wound();
                                    it = _locking_set.erase(it);
                                } else {
                                    it++;
                                }
                            }
                            _lock_type = LOCK_EX;
                            _locking_set.insert(txn);
                            rc = RCOK;
                        } else {
                        wait:
                            _lock_type = LOCK_UPGRADING;
                            rc = WAIT;
                        }
                    #endif
                    }
                } else {
                    assert(_lock_type == LOCK_UPGRADING);
                    rc = ABORT;
                }
            } // else just ignore.
            if (need_latch)
                pthread_mutex_unlock( &_latch );
            return rc;
        }
        bool conflict = conflict_lock(_lock_type, type);
    #if CC_ALG == NO_WAIT || CC_ALG == CALVIN
        if (conflict) {
            assert(!_locking_set.empty());
            auto it = _locking_set.begin();
            if (txn->is_mp_txn()) {
                if ((*it)->is_mp_txn()) {
                    INC_INT_STATS(dist_txn_abort_dist, 1);
                } else {
                    INC_INT_STATS(dist_txn_abort_local, 1);
                }
            } else {
                if ((*it)->is_mp_txn()) {
                    INC_INT_STATS(local_txn_abort_dist, 1);
                } else {
                    INC_INT_STATS(local_txn_abort_local, 1);
                }
            }
            if (type == _lock_type) {
                assert(type == LOCK_EX);
                INC_INT_STATS(num_aborts_ws, 1);
            } else {
                INC_INT_STATS(num_aborts_rs, 1);
            }
            rc = ABORT;
        }
    #elif CC_ALG == WAIT_DIE
        // check conflict between incoming txn and waiting txns.
        if (!conflict)
            if (!_waiting_set.empty() && LOCK_MAN(_waiting_set.rbegin()->txn)->get_priority() > LOCK_MAN(txn)->get_priority())
                conflict = true;
        if (conflict) {
            assert(!_locking_set.empty());
            if (_waiting_set.size() > _max_num_waits) {
                rc = ABORT;
            } else if (LOCK_MAN(txn)->get_priority() > LOCK_MAN(*_locking_set.begin())->get_priority() ||
                (!_waiting_set.empty() && LOCK_MAN(txn)->get_priority() > LOCK_MAN(_waiting_set.begin()->txn)->get_priority()) ) {
                // DEBUG_PRINT("[DEBUG] Txn %lu abort: locker %lu: %d\n", txn->get_txn_id(), (*_locking_set.begin())->get_txn_id(), LOCK_MAN(txn)->get_ts() > LOCK_MAN(*_locking_set.begin())->get_ts());
                rc = ABORT;
            } else {
                rc = WAIT;
            }
        }
        if (rc == WAIT) {
            WaitEntry entry = {type, txn};
            assert(!_locking_set.empty());
            
            for (auto entry : _waiting_set)
                assert(entry.txn != txn);
            
            txn->get_cc_manager()->set_txn_waiting();
            _waiting_set.insert(entry);
        }
        // ABORT Stats
        if (rc == ABORT) {
            if (type == _lock_type && type == LOCK_EX) {
                INC_INT_STATS(num_aborts_ws, 1);
            } else {
                INC_INT_STATS(num_aborts_rs, 1);
            }
        }
    #elif CC_ALG == WOUND_WAIT
        if (!conflict)
            if (!_waiting_set.empty() && LOCK_MAN(_waiting_set.begin()->txn)->get_priority() > LOCK_MAN(txn)->get_priority())
                conflict = true;
        if (conflict) {
            assert(!_locking_set.empty());
            bool can_wound = true;
            for (auto it = _locking_set.begin(); it != _locking_set.end(); it++) {
                if (LOCK_MAN(txn)->get_priority() > LOCK_MAN(*it)->get_priority() || LOCK_MAN(*it)->get_cc_state() > LockManager::CC_PREPARING) {
                    can_wound = false;
                    break;
                }
            }
            if (can_wound) {
                // WOUND
                for (auto it = _locking_set.begin(); it != _locking_set.end();) {
                    if (*it != txn) {
                        if (LOCK_MAN(*it)->get_cc_state() > LockManager::CC_PREPARING) {
                            // This txn cannot be wounded
                            goto cannot_wound;
                        }
                        LOCK_MAN(*it)->wound();
                        it = _locking_set.erase(it);
                    } else {
                        it++;
                    }
                }
                _lock_type = LOCK_EX;
                rc = RCOK;
            } else {
            cannot_wound:
                // WAIT
                if (_waiting_set.size() > _max_num_waits) {
                    rc = ABORT;
                } else {
                    rc = WAIT;
                }
            }
        }
        if (rc == WAIT) {
            WaitEntry entry = {type, txn};
            assert(!_locking_set.empty());

            for (auto entry : _waiting_set)
                assert(entry.txn != txn);

            txn->get_cc_manager()->set_txn_waiting();
            _waiting_set.insert(entry);
        }
        if (rc == ABORT) {
            if (type == _lock_type && type == LOCK_EX) {
                INC_INT_STATS(num_aborts_ws, 1);
            } else {
                INC_INT_STATS(num_aborts_rs, 1);
            }
        }
    #endif

    #if CC_ALG == CALVIN
        assert(rc == RCOK);
    #endif

        if (rc == RCOK) {
            _lock_type = type;
            uint32_t size = _locking_set.size();
            _locking_set.insert(txn);
            M_ASSERT(_locking_set.size() == size + 1, "locking_set.size=%ld, size=%d\n", _locking_set.size(), size);
        }

        if (need_latch)
            pthread_mutex_unlock( &_latch );
    } else {
        // The acquiring txn is an applier
        // All locks are exclusive locks. Use a simple CC here: RCOK or WAIT
        assert(type == LOCK_EX);

        if (_applier_lock_owner == txn)
            return RCOK;
        pthread_mutex_lock( &_latch );
        if (_applier_lock_owner == NULL || _applier_lock_owner == txn) {
            _applier_lock_owner = txn;
            rc = RCOK;
        } else {
            LOCK_MAN(txn)->set_txn_waiting();
            _applier_waiting_set.insert(txn);
            rc = WAIT;
        }
        pthread_mutex_unlock( &_latch );
    }

    return rc;
}

RC
Row_lock::lock_release(TxnManager * txn, RC rc)
{
    if (!txn->is_applier()) {
        // The acquiring txn is the coordinator or a partition leader
        pthread_mutex_lock( &_latch );
        uint32_t released = _locking_set.erase(txn);
    #if CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT
        // txn is not holding the lock, remove it from _waiting_set
        if (released == 0) {
            for (std::set<WaitEntry>::iterator it = _waiting_set.begin(); it != _waiting_set.end(); it++) {
                if (it->txn == txn) {
                    _waiting_set.erase(it);
                    break;
                }
            }
            pthread_mutex_unlock( &_latch );
            return RCOK;
        }
    #endif
        if (released == 0) {
            pthread_mutex_unlock( &_latch );
            return RCOK;
        }

    #if CC_ALG == WAIT_DIE
        assert(LOCK_MAN(txn)->get_priority() > 0);
        bool done = (released == 1) ? false : true;
        // handle upgrade
        if (_lock_type == LOCK_UPGRADING) {
            assert(_locking_set.size() >= 1);
            if (_locking_set.size() == 1) {
                TxnManager * t = *_locking_set.begin();
                assert(t == _upgrading_txn);
                _lock_type = LOCK_EX;
                if (t->set_txn_ready(RCOK)) {
                    _wakeup_txn(t);
                }
            }
            goto unlock_and_return_ok;
        }
        if (_locking_set.empty())
            _lock_type = LOCK_NONE;
        while (!done) {
            std::set<WaitEntry>::reverse_iterator rit = _waiting_set.rbegin();
            if (rit != _waiting_set.rend() && !conflict_lock(rit->type, _lock_type)) {
                _lock_type = rit->type;
                _locking_set.insert(rit->txn);
                if (rit->txn->set_txn_ready(RCOK)) {
                    _wakeup_txn(rit->txn);
                }
                _waiting_set.erase( --rit.base() );
            } else
                done = true;
        }
        if (_locking_set.empty())
            assert(_waiting_set.empty());
    #elif CC_ALG == NO_WAIT || CC_ALG == CALVIN
        if (_locking_set.empty())
            _lock_type = LOCK_NONE;
    #elif CC_ALG == WOUND_WAIT
        bool done = (released == 1) ? false : true;
        if (_lock_type == LOCK_UPGRADING) {
            assert(_locking_set.size() >= 1);
            if (_locking_set.size() == 1) {
                TxnManager * t = *_locking_set.begin();
                assert(t == _upgrading_txn);
                _lock_type = LOCK_EX;
                if (t->set_txn_ready(RCOK)) {
                    _wakeup_txn(t);
                }
            }
            goto unlock_and_return_ok;
        }
        if (_locking_set.empty())
            _lock_type = LOCK_NONE;
        while (!done) {
            auto it = _waiting_set.begin();
            if (it != _waiting_set.end() && !conflict_lock(it->type, _lock_type)) {
                _lock_type = it->type;
                _locking_set.insert(it->txn);
                if (it->txn->set_txn_ready(RCOK)) {
                    _wakeup_txn(it->txn);
                }
                _waiting_set.erase(it);
            } else
                done = true;
        }
    #else
        assert(false);
    #endif

    unlock_and_return_ok:
        pthread_mutex_unlock( &_latch );
        return RCOK;
    } else {
        // The acquiring txn is an applier
        TxnManager * applier_wakeup_txn = NULL;
        pthread_mutex_lock( &_latch );
        if (_applier_lock_owner != txn) {
            _applier_waiting_set.erase(txn);
            pthread_mutex_unlock( &_latch );
            return RCOK;
        }

        if (_applier_waiting_set.size() > 0) {
            auto last_it = _applier_waiting_set.end();
            last_it--;
            TxnManager * next = *last_it;
            _applier_lock_owner = next;
            next->set_txn_ready(RCOK);
            applier_wakeup_txn = next;
            _applier_waiting_set.erase(next);
        } else {
            _applier_lock_owner = NULL;
        }
        if (applier_wakeup_txn && applier_wakeup_txn->is_txn_ready()) {
            uint32_t queue_id = glob_manager->txnid_to_server_thread(applier_wakeup_txn->get_txn_id());
            ready_queues[ queue_id ]->push(applier_wakeup_txn->get_txn_id());
        }
        pthread_mutex_unlock( &_latch );
        return RCOK;
    }

    assert(false);
}

bool Row_lock::conflict_lock(LockType l1, LockType l2)
{
    if (l1 == LOCK_UPGRADING || l2 == LOCK_UPGRADING)
        return true;
    if (l1 == LOCK_NONE || l2 == LOCK_NONE)
        return false;
    else if (l1 == LOCK_EX || l2 == LOCK_EX)
        return true;
    else
        return false;
}

bool
Row_lock::is_owner(TxnManager * txn)
{
    return _lock_type == LOCK_EX
        && _locking_set.size() == 1
        && (*_locking_set.begin()) == txn;
}

#endif
