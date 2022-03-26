//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct HashJoinKey {
  Value key_;

  bool operator==(const HashJoinKey &other) const { return key_.CompareEquals(other.key_) == CmpBool::CmpTrue; }
};
}  // namespace bustub

namespace std {

template <>
struct hash<bustub::HashJoinKey> {
  // 通过给定key，调用bustub::HashUtil::HashValue得到hash值
  std::size_t operator()(const bustub::HashJoinKey &key) const { return bustub::HashUtil::HashValue(&key.key_); }
};
}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** The left child executor to obtain value from */
  std::unique_ptr<AbstractExecutor> left_child_executor_;
  /** The right child executor to obtain value from */
  std::unique_ptr<AbstractExecutor> right_child_executor_;
  /** Hash table */
  std::unordered_map<HashJoinKey, std::vector<std::vector<Value>>> hash_table_;
  // assume hash table has directory and bucket
  // 若一个hash值，对应多个元组，因为next要一个一个取，所以缓存这个hash表给定key值对应的溢出列list
  // 然后通过bucket_index来一个一个读取
  std::vector<std::vector<Value>> bucket_list_;

  size_t bucket_index_;
};

}  // namespace bustub
