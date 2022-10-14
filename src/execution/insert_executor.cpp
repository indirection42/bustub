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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
}
void InsertExecutor::InsertIntoTableAndUpdateIndex(Tuple tuple) {
  RID inserted_rid;
  if (!table_heap_->InsertTuple(tuple, &inserted_rid, exec_ctx_->GetTransaction())) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertExecutor: no enough space");
  }

  for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
    index_info->index_->InsertEntry(
        tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
        inserted_rid, exec_ctx_->GetTransaction());
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (plan_->IsRawInsert()) {
    for (const auto &values : plan_->RawValues()) {
      InsertIntoTableAndUpdateIndex(Tuple(values, &table_info_->schema_));
    }
  } else {
    child_executor_->Init();
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      InsertIntoTableAndUpdateIndex(tuple);
    }
  }
  return false;
}

}  // namespace bustub
