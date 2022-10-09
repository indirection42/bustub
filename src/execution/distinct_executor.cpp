//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() { child_executor_->Init(); }

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (child_executor_->Next(tuple, rid)) {
    auto key = MakeDistinctKey(tuple);
    if (distinct_keys_.find(key) == distinct_keys_.end()) {
      distinct_keys_.insert(key);
      return true;
    }
  }
  return false;
}

auto DistinctExecutor::MakeDistinctKey(const Tuple *tuple) -> DistinctKey {
  std::vector<Value> keys;
  for (size_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
    keys.emplace_back(tuple->GetValue(plan_->OutputSchema(), i));
  }
  return {keys};
}

}  // namespace bustub
