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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  table_iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iter_ == table_heap_->End()) {
    return false;
  }
  const auto *predicate = plan_->GetPredicate();
  const auto *table_schema = &exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_;
  const auto *output_schema = plan_->OutputSchema();
  // skip the unsatisfied tuple
  while (predicate != nullptr && !predicate->Evaluate(&*table_iter_, table_schema).GetAs<bool>()) {
    ++table_iter_;
    if (table_iter_ == table_heap_->End()) {
      return false;
    }
  }
  // do projection
  std::vector<Value> values;
  for (size_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
    values.emplace_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->Evaluate(&*table_iter_, output_schema));
  }
  *tuple = Tuple(values, output_schema);
  *rid = table_iter_->GetRid();
  ++table_iter_;
  return true;
}
}  // namespace bustub
