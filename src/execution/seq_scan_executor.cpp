//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), cur_(nullptr, RID{}, nullptr), end_(nullptr, RID{}, nullptr) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->GetTableOid());
  // 这里只分配空间，输出的列的数量
  out_schema_idx_.reserve(plan_->OutputSchema()->GetColumnCount());

  for (uint32_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
    auto column_name = plan->OutputSchema()->GetColumn(i).GetName();
    out_schema_idx_.push_back(table_info_->schema_.GetColIdx(column_name));
  }

  if (plan_->GetPredicate() == nullptr) {
    is_alloc_ = true;
    predicate_ = new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
  } else {
    predicate_ = plan_->GetPredicate();
  }
}

SeqScanExecutor::~SeqScanExecutor() {
  if (is_alloc_) {
    delete predicate_;
  }
  predicate_ = nullptr;
}

void SeqScanExecutor::Init() {
  cur_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_info_->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // 符合条件的tuple不一定就是下一个，可能需要多探测几个
  while (cur_ != end_) {
    auto temp = cur_++;
    auto value = predicate_->Evaluate(&(*temp), &(table_info_->schema_));
    if (value.GetAs<bool>()) {
      std::vector<Value> values;
      values.reserve(out_schema_idx_.size());
      for (auto i : out_schema_idx_) {
        values.push_back(temp->GetValue(&table_info_->schema_, i));
      }
      *tuple = Tuple(values, plan_->OutputSchema());
      *rid = temp->GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
