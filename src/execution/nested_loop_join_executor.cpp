//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan_->Predicate() != nullptr) {
    predicate_ = plan_->Predicate();
  } else {
    is_alloc_ = true;
    predicate_ = new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
  }
}

NestedLoopJoinExecutor::~NestedLoopJoinExecutor() {
  if (is_alloc_) {
    delete predicate_;
  }
  predicate_ = nullptr;
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_is_selected = left_executor_->Next(&left_tuple, &left_rid);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  //   Tuple *left_tuple = nullptr, *right_tuple = nullptr;
  //   RID *left_rid = nullptr, *right_rid = nullptr;
  //   TODO 为什么这里用*left_tuple这些会报错啊？
  Tuple right_tuple;
  RID right_rid;
  while (left_is_selected) {
    // 进入循环说明左边已经拿到tuple，就可以遍历右表了
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto value = predicate_->EvaluateJoin(&left_tuple, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                                            plan_->GetRightPlan()->OutputSchema());
      if (value.GetAs<bool>()) {
        std::vector<Value> values;
        values.reserve(plan_->OutputSchema()->GetColumnCount());
        for (auto column : plan_->OutputSchema()->GetColumns()) {
          // 拿到OutputSchema中每一列的列表达式，从而可以得到这个列是左边还是右边的元组中的，从而在对应的元组中，找到元组中该列的index，拿到该列对应的值
          auto column_expr = reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
          if (column_expr->GetTupleIdx() == 0) {
            values.push_back(left_tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), column_expr->GetColIdx()));
          } else {
            values.push_back(right_tuple.GetValue(plan_->GetRightPlan()->OutputSchema(), column_expr->GetColIdx()));
          }
        }
        *tuple = Tuple(values, plan_->OutputSchema());
        // TODO 这里为什么用的是左元组的RID？？？
        *rid = left_tuple.GetRid();
        return true;
      }
    }
    // 走到这里说明，上一个while (right_executor_->Next(&right_tuple, &right_rid))循环没有join元组，也就是说没有return
    // true 那么说明右表已经遍历完了，可以Init，初始化右表的cursor
    right_executor_->Init();
    // 内循环结束，外循环next
    left_is_selected = left_executor_->Next(&left_tuple, &left_rid);
  }
  return false;
}

}  // namespace bustub
