//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid()); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  child_executor_->Init();
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
      case IsolationLevel::READ_COMMITTED:
        exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), child_rid);
        break;
      case IsolationLevel::REPEATABLE_READ:
        exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), child_rid);
        break;
    }
    // TableWriteSet already get updated in TableHeap::MarkDelete
    // update table
    table_info_->table_->MarkDelete(child_rid, exec_ctx_->GetTransaction());
    // update indexes
    for (auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes((table_info_->name_))) {
      exec_ctx_->GetTransaction()->GetIndexWriteSet()->emplace_back(
          IndexWriteRecord(child_rid, table_info_->oid_, WType::DELETE, child_tuple, Tuple{}, index_info->index_oid_,
                           exec_ctx_->GetCatalog()));

      index_info->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          child_rid, exec_ctx_->GetTransaction());
    }
  }
  return false;
}

}  // namespace bustub
