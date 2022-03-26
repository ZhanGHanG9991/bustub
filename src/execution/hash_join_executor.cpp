//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_child)),
      right_child_executor_(std::move(right_child)) {
  left_child_executor_->Init();
  right_child_executor_->Init();
  Tuple left_tuple;
  RID left_rid;
  while (left_child_executor_->Next(&left_tuple, &left_rid)) {
    Value key_value = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, plan_->GetLeftPlan()->OutputSchema());
    auto left_col_count = plan_->GetLeftPlan()->OutputSchema()->GetColumnCount();
    std::vector<Value> values;
    values.reserve(left_col_count);
    for (uint32_t i = 0; i < left_col_count; i++) {
      values.push_back(left_tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), i));
    }
    HashJoinKey key{key_value};
    if (hash_table_.count(key) > 0) {
      hash_table_[key].push_back(std::move(values));
    } else {
      // C++11新特性，{}可用来初始化vector
      // hash_table_[key] = {values};
      // 外面的{}是pair对，内部的{}是初始化vector
      hash_table_.insert({key, {values}});
      // pair与make_pair https://www.thinbug.com/q/41325199
      // hash_table_.insert(std::pair<HashJoinKey, std::vector<std::vector<Value>>>(key, {values}));
    }
  }
}

void HashJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
  bucket_index_ = 0;
  bucket_list_ = {};
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  bool right_over = true;
  if (bucket_index_ == bucket_list_.size()) {
    // 上一个bucket_list遍历完了，获取新的bucket_list
    while (right_child_executor_->Next(tuple, rid)) {
      Value key_value = plan_->RightJoinKeyExpression()->Evaluate(tuple, plan_->GetRightPlan()->OutputSchema());
      HashJoinKey key{key_value};
      if (hash_table_.count(key) > 0) {
        bucket_index_ = 0;
        bucket_list_ = hash_table_[key];
        right_over = false;
        break;
      }
    }
    if (right_over) {
      return false;
    }
  }
  std::vector<Value> values;
  values.reserve(plan_->OutputSchema()->GetColumnCount());
  for (auto column : plan_->OutputSchema()->GetColumns()) {
    auto column_expr = reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
    if (column_expr->GetTupleIdx() == 0) {
      values.push_back(bucket_list_[bucket_index_][column_expr->GetColIdx()]);
    } else {
      values.push_back(tuple->GetValue(plan_->GetRightPlan()->OutputSchema(), column_expr->GetColIdx()));
    }
  }
  *tuple = Tuple(values, plan_->OutputSchema());
  bucket_index_++;
  return true;
}
}  // namespace bustub
