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
#include "concurrency/transaction.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // 加锁
  std::unique_lock<std::mutex> lock(latch_);
  // 检查是否是在2PL的shrinking阶段加锁
  if (!CheckForLock(txn)) {
    return false;
  }
  // 对于隔离级别是READ_UNCOMMITTED，不需要sharedLock
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  // 如果这个tuple rid并没有被请求过，也就是说没有加入lock_table_中，则初始化
  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
  // 找到tuple rid对应的request请求队列，将这个txn加进去
  LockRequestQueue *lock_request_queue = &lock_table_.find(rid)->second;
  lock_request_queue->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);

  // 如果这个lock_request_queue卡在这里，且正在写入，说明这个tuple正在被一个exclusiveLock占用
  if (lock_request_queue->is_writing_) {
    // 检查死锁，如果有死锁，，然后将这个txn的状态置为aborted，然后将这个队列的共有属性is_writing或者sharing_count做一下处理
    DeadlockPrevent(txn, lock_request_queue);
    // 经过死锁处理过后，如果没有死锁，则需要等待unlock
    // 这个wait会一直卡在这里，除非第二个参数为true，也就是说要么是死锁要么是unlock后解除了iswriting属性
    lock_request_queue->cv_.wait(lock, [lock_request_queue, txn]() -> bool {
      return txn->GetState() == TransactionState::ABORTED || !lock_request_queue->is_writing_;
    });
  }
  // 如果是死锁过来的，则在这里处理，直接会throw exception deadlock
  CheckAborted(txn, lock_request_queue);
  // 如果走到这里，说明已经可以分配sharedlock
  txn->GetSharedLockSet()->emplace(rid);
  // 找到这个txn在这个lock_table_中，tuple rid对应的请求队列中的迭代器
  auto iter = GetIterator(&lock_request_queue->request_queue_, txn);
  // 将这个请求的isgranted属性置为true，代表已经分配
  iter->granted_ = true;
  // 存储txn id -> txn，方便找到txn，因为LockRequest中只记录了txn
  // id，后面在死锁检测中，只能拿到txnid，所以需要这个txnid直接拿到txn，然后将属性置为aborted
  id_to_txn_.insert({txn->GetTransactionId(), txn});
  // 这个tuple rid对应的请求队列的共有属性，sharing_count+1，代表这个tuple目前被多少sharedlock占用，
  // 因为只有为0的时候，才能安排exclusiveLock，所以需要统计sharing_count
  lock_request_queue->sharing_count_++;
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  if (!CheckForLock(txn)) {
    return false;
  }
  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
  LockRequestQueue *lock_request_queue = &lock_table_.find(rid)->second;
  lock_request_queue->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  // 如果这个queue is_writing是true代表已经被其他exclusiveLock占用，或者sharing_count>0，代表被其他sharedlock占用
  if (lock_request_queue->is_writing_ || lock_request_queue->sharing_count_ > 0) {
    DeadlockPrevent(txn, lock_request_queue);
    lock_request_queue->cv_.wait(lock, [lock_request_queue, txn]() -> bool {
      return txn->GetState() == TransactionState::ABORTED ||
             (!lock_request_queue->is_writing_ && lock_request_queue->sharing_count_ == 0);
    });
  }
  CheckAborted(txn, lock_request_queue);
  txn->GetExclusiveLockSet()->emplace(rid);
  auto iter = GetIterator(&lock_request_queue->request_queue_, txn);
  iter->granted_ = true;
  id_to_txn_.insert({txn->GetTransactionId(), txn});
  lock_request_queue->is_writing_ = true;
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  // 其实和CheckForLock原理一样，但这里的AbortReason不一样
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  LockRequestQueue *request_queue = &lock_table_.find(rid)->second;
  // 如果这个queue的公共属性upgrading为true，代表其他的request正在upgrading，则会冲突
  if (request_queue->upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  // 先从txn占用的sharedLock中删除，然后后面找到机会再占用exclusiveLock
  txn->GetSharedLockSet()->erase(rid);
  // 去掉后，再同步queue的公共属性，比如sharing_count，再重置这个txn的request的lock_mode和granted属性
  request_queue->sharing_count_--;
  auto iter = GetIterator(&request_queue->request_queue_, txn);
  iter->lock_mode_ = LockMode::EXCLUSIVE;
  iter->granted_ = false;

  // 这里就和要加exclusiveLock一样了，多了一点就是upgrading变为true
  if (request_queue->is_writing_ || request_queue->sharing_count_ > 0) {
    DeadlockPrevent(txn, request_queue);
    request_queue->upgrading_ = true;
    request_queue->cv_.wait(lock, [request_queue, txn]() -> bool {
      return txn->GetState() == TransactionState::ABORTED ||
             (!request_queue->is_writing_ && request_queue->sharing_count_ == 0);
    });
  }

  CheckAborted(txn, request_queue);

  txn->GetExclusiveLockSet()->emplace(rid);
  request_queue->upgrading_ = false;
  request_queue->is_writing_ = true;
  iter = GetIterator(&request_queue->request_queue_, txn);
  iter->granted_ = true;

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  LockRequestQueue *lock_request_queue = &lock_table_.find(rid)->second;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  auto iter = GetIterator(&lock_request_queue->request_queue_, txn);
  // 获取lock_mode，并对解锁后进行操作
  LockMode mode = iter->lock_mode_;
  lock_request_queue->request_queue_.erase(iter);
  // 判断这里是否要把2PL从Growing置为Shrinking
  // TODO 如果是可提交读，unlock的是sharedLock则不用置shrinking
  if (!(mode == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  // 解锁后，如果是shared，则维护sharing_count，如果是exclusive，则维护is_writing
  // 最后，要notify，让上面那些wait的函数进行判断然后可能会向下走
  if (mode == LockMode::SHARED) {
    if (--lock_request_queue->sharing_count_ == 0) {
      lock_request_queue->cv_.notify_all();
    }
  } else {
    lock_request_queue->is_writing_ = false;
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

// 检查是否是在shrinking阶段加锁，是的话，则abort txn 并抛出异常
bool LockManager::CheckForLock(Transaction *txn) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  return true;
}

// 通过给定的queue和txn，找到对应txn的request的位置
std::list<LockManager::LockRequest>::iterator LockManager::GetIterator(
    std::list<LockManager::LockRequest> *request_queue, Transaction *txn) {
  for (auto iter = request_queue->begin(); iter != request_queue->end(); iter++) {
    if (txn->GetTransactionId() == iter->txn_id_) {
      return iter;
    }
  }
  return request_queue->end();
}
// 在死锁检查后，检查是否有Aborted的txn，有的话则抛出异常
void LockManager::CheckAborted(Transaction *txn, LockRequestQueue *request_queue) {
  if (txn->GetState() == TransactionState::ABORTED) {
    auto iter = GetIterator(&request_queue->request_queue_, txn);
    request_queue->request_queue_.erase(iter);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
}

/**
 * @brief 死锁预防
 * 对于给定的txn和queue，我们去遍历queue中的request，如果有的request对应的txn_id比目前请求的txn id还要大
 * 根据实验要求 WOUND-WAIT思想 abort request对应的txn
 * 对于一个老事务要去拿新事务的锁在WAIT-DIE下，他会WAIT。（重点）在WOUND-WAIT下，他会让新事务回滚，直接抢了他的锁。重点）
 * 新事务要拿老事务的锁的话，在WAIT-DIE，会把自己回滚。在WOUND-WAIT下，会等老事务把锁还掉
 * @param txn
 * @param request_queue
 */
void LockManager::DeadlockPrevent(Transaction *txn, LockRequestQueue *request_queue) {
  for (const auto &request : request_queue->request_queue_) {
    if (request.granted_ && request.txn_id_ > txn->GetTransactionId()) {
      id_to_txn_[request.txn_id_]->SetState(TransactionState::ABORTED);
      if (request.lock_mode_ == LockMode::SHARED) {
        request_queue->sharing_count_--;
      } else {
        request_queue->is_writing_ = false;
      }
    }
  }
}

}  // namespace bustub
