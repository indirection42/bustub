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

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  Tuple left_child_tuple;
  RID left_child_rid;
  while (left_child_->Next(&left_child_tuple, &left_child_rid)) {
    auto key = MakeJoinKey(&left_child_tuple, plan_->LeftJoinKeyExpression(), left_child_->GetOutputSchema());
    if (left_ht_.find(key) == left_ht_.end()) {
      left_ht_[key] = std::vector<Tuple>{};
    }
    left_ht_[key].emplace_back(left_child_tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_key_.keys_.empty() || matched_tuples_iter_ == left_ht_[cur_key_].end()) {
    while (right_child_->Next(&right_child_tuple_, &right_child_rid_)) {
      auto key = MakeJoinKey(&right_child_tuple_, plan_->RightJoinKeyExpression(), right_child_->GetOutputSchema());
      if (left_ht_.find(key) != left_ht_.end()) {
        cur_key_ = key;
        matched_tuples_iter_ = left_ht_[key].begin();
        break;
      }
    }
  }
  if (!cur_key_.keys_.empty() && matched_tuples_iter_ != left_ht_[cur_key_].end()) {
    std::vector<Value> vals;
    vals.reserve(GetOutputSchema()->GetColumnCount());
    for (auto &col : GetOutputSchema()->GetColumns()) {
      vals.emplace_back(col.GetExpr()->EvaluateJoin(&*matched_tuples_iter_, left_child_->GetOutputSchema(),
                                                    &right_child_tuple_, right_child_->GetOutputSchema()));
    }
    *tuple = Tuple(vals, GetOutputSchema());
    ++matched_tuples_iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
