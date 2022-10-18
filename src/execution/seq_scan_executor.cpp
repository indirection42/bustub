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
  const auto *predicate = plan_->GetPredicate();
  const auto *table_schema = &exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_;
  const auto *output_schema = plan_->OutputSchema();
  // get satisfied tuple
  for (; table_iter_ != table_heap_->End(); ++table_iter_) {
    switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
        // no shared lock
        break;
      case IsolationLevel::READ_COMMITTED:
      case IsolationLevel::REPEATABLE_READ:
        exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), table_iter_->GetRid());
    }
    if (predicate == nullptr || predicate->Evaluate(&*table_iter_, table_schema).GetAs<bool>()) {
      // READ_COMMITTED release ShareLock after use
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), table_iter_->GetRid());
      }
      break;
    }
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), table_iter_->GetRid());
    }
  }
  if (table_iter_ == table_heap_->End()) {
    return false;
  }
  // do projection
  std::vector<Value> values;
  values.reserve(output_schema->GetColumnCount());
  for (auto &col : output_schema->GetColumns()) {
    values.emplace_back(col.GetExpr()->Evaluate(&*table_iter_, table_schema));
  }
  *tuple = Tuple(values, output_schema);
  *rid = table_iter_->GetRid();
  ++table_iter_;
  return true;
}
}  // namespace bustub
