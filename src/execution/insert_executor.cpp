//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
  insert_index_ = 0;
}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool is_inserted = false;
  if (plan_->IsRawInsert()) {
    if (insert_index_ == plan_->RawValues().size()) {
      // nothing
    } else {
      std::vector<Value> raw_value = plan_->RawValues()[insert_index_++];
      *tuple = Tuple(raw_value, &(table_info_->schema_));
      table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
      is_inserted = true;
    }
  } else {
    if (child_executor_->Next(tuple, rid)) {
      is_inserted = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    }
  }
  if (is_inserted) {
    for (auto index_info : index_infos_) {
      auto tuple_key =
          tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(tuple_key, *rid, exec_ctx_->GetTransaction());
    }
  }
  return is_inserted;
}

}  // namespace bustub
