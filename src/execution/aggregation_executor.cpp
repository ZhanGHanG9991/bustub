//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/aggregate_value_expression.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
}

void AggregationExecutor::Init() {
  child_->Init();
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    auto temp_iterator = aht_iterator_;
    ++aht_iterator_;
    if (plan_->GetHaving() != nullptr) {
      auto value =
          plan_->GetHaving()->EvaluateAggregate(temp_iterator.Key().group_bys_, temp_iterator.Val().aggregates_);
      if (!value.GetAs<bool>()) {
        continue;
      }
    }
    std::vector<Value> value;
    value.reserve(plan_->OutputSchema()->GetColumnCount());
    for (const auto &column : plan_->OutputSchema()->GetColumns()) {
      auto agg_expr = reinterpret_cast<const AggregateValueExpression *>(column.GetExpr());
      value.push_back(agg_expr->EvaluateAggregate(temp_iterator.Key().group_bys_, temp_iterator.Val().aggregates_));
    }
    *tuple = Tuple(value, plan_->OutputSchema());
    return true;
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
