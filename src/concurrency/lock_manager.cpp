//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::AddToQueueAndCheckOlder(LockRequestQueue *lrq, Transaction *txn, LockMode request_lock_mode) {
  auto lrq_iter = lrq->request_queue_.begin();
  // check if already in lrq and check if has older writer
  bool has_older_request_can_block_me = false;
  while (lrq_iter != lrq->request_queue_.end() &&
         !(lrq_iter->txn_id_ == txn->GetTransactionId() && lrq_iter->lock_mode_ == request_lock_mode)) {
    // older writer block newer reader&writer
    if (lrq_iter->lock_mode_ == LockMode::EXCLUSIVE) {
      has_older_request_can_block_me = true;
    }
    // older reader block newer writer
    if (lrq_iter->lock_mode_ == LockMode::SHARED && request_lock_mode == LockMode::EXCLUSIVE) {
      // if this lrq_iter is the same txn(can be upgrade but not use, should not be blocked)
      if (lrq_iter->txn_id_ != txn->GetTransactionId()) {
        has_older_request_can_block_me = true;
      }
    }
    ++lrq_iter;
  }
  // if not in queue, append to queue for LockShared and LockExclusive
  // not for LockUpgrade, already
  if (lrq_iter == lrq->request_queue_.end()) {
    lrq->request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), request_lock_mode));
  }
  return has_older_request_can_block_me;
}

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> guard(latch_);
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    // support reenterance
    return true;
  }
  LockRequestQueue &lrq = lock_table_[rid];
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
      return false;
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::REPEATABLE_READ:
      // older writer block
      while (AddToQueueAndCheckOlder(&lrq, txn, LockMode::SHARED)) {
        // TODO(jiyuanz): deadlock prevention, wound-wait
        lrq.cv_.wait(guard);
      }
      // update lrq
      auto lr = std::find_if(lrq.request_queue_.begin(), lrq.request_queue_.end(),
                             [txn](LockRequest lr) { return lr.txn_id_ == txn->GetTransactionId(); });
      lr->granted_ = true;
      // update txn's lockset
      txn->GetSharedLockSet()->emplace(rid);
      return true;
  }
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> guard(latch_);
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    // support reenterance
    return true;
  }
  LockRequestQueue &lrq = lock_table_[rid];
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::REPEATABLE_READ:
      while (AddToQueueAndCheckOlder(&lrq, txn, LockMode::EXCLUSIVE)) {
        lrq.cv_.wait(guard);
      }
      // update lrq
      auto lr = std::find_if(lrq.request_queue_.begin(), lrq.request_queue_.end(),
                             [txn](LockRequest lr) { return lr.txn_id_ == txn->GetTransactionId(); });
      lr->granted_ = true;
      // update txn's lockset
      txn->GetExclusiveLockSet()->emplace(rid);
      return true;
  }
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> guard(latch_);
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    // support reenterance
    return true;
  }
  LockRequestQueue &lrq = lock_table_[rid];
  switch (txn->GetIsolationLevel()) {
      // actually not possible for READ_UNCOMMITTED
    case IsolationLevel::READ_UNCOMMITTED:
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::REPEATABLE_READ:
      if (lrq.upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      // TODO(jiyuanz) just get first lr?
      auto lr = std::find_if(lrq.request_queue_.begin(), lrq.request_queue_.end(),
                             [txn](LockRequest lr) { return lr.txn_id_ == txn->GetTransactionId(); });
      // no shared lock request in queue
      if (lr == lrq.request_queue_.end() || lr->lock_mode_ != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      //
      if (!lr->granted_) {
        UNREACHABLE("This txn must be block when former shared_lock not granted");
      }
      // prepare upgrading
      lrq.upgrading_ = txn->GetTransactionId();
      lr->lock_mode_ = LockMode::EXCLUSIVE;
      // wait older reader (cannot have no older writer, because they are blocked when shared_lock has granted before )
      while (AddToQueueAndCheckOlder(&lrq, txn, LockMode::EXCLUSIVE)) {
        lrq.cv_.wait(guard);
      }
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->emplace(rid);
      return true;
  }
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> guard(latch_);
  LockRequestQueue &lrq = lock_table_[rid];
  if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  // TODO(jiyuanz) just get first? if one is read, then exclusive(not call upgrade)
  auto lrq_iter = lrq.request_queue_.begin();
  while (lrq_iter != lrq.request_queue_.end() && lrq_iter->txn_id_ != txn->GetTransactionId()) {
    ++lrq_iter;
  }
  if (lrq_iter == lrq.request_queue_.end() || !lrq_iter->granted_) {
    txn->SetState(TransactionState::ABORTED);
    // throw unlock on not lock
    return false;
  }
  if (lrq_iter->lock_mode_ == LockMode::SHARED) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
        txn->SetState(TransactionState::ABORTED);
        // throw unlockshared on READ_UNCOMMITTED
        return false;
      case IsolationLevel::REPEATABLE_READ:
        if (txn->GetState() == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }
      case IsolationLevel::READ_COMMITTED:
        txn->GetSharedLockSet()->erase(rid);
        lrq.request_queue_.erase(lrq_iter);
        lrq.cv_.notify_all();
        return true;
    }
  } else {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
        txn->SetState(TransactionState::ABORTED);
        return false;
      case IsolationLevel::READ_COMMITTED:
      case IsolationLevel::REPEATABLE_READ:
        if (txn->GetState() == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }
        txn->GetExclusiveLockSet()->erase(rid);
        lrq.request_queue_.erase(lrq_iter);
        lrq.cv_.notify_all();
        return true;
    }
  }
}

}  // namespace bustub
