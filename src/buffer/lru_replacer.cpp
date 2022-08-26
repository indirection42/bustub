//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock lck(latch_);
  if (cache_map_.empty()) {
    return false;
  }
  *frame_id = cache_list_.back();
  cache_map_.erase(cache_list_.back());
  cache_list_.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lck(latch_);
  // access it, directly remove it, no need to move it to the front
  if (cache_map_.find(frame_id) != cache_map_.end()) {
    cache_list_.erase(cache_map_[frame_id]);
    cache_map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lck(latch_);
  // old page
  if (cache_map_.find(frame_id) != cache_map_.end()) {
    // cache_list_.splice(cache_list_.begin(),cache_list_,cache_map_[frame_id]);
    return;
  }
  // new page
  // actually it's not sufficient to do remove the back
  // and it's would be processed out of the scope
  if (cache_map_.size() == capacity_) {
    cache_map_.erase(cache_list_.back());
    cache_list_.pop_back();
  }
  cache_list_.emplace_front(frame_id);
  cache_map_[frame_id] = cache_list_.begin();
}

size_t LRUReplacer::Size() {
  std::scoped_lock lck(latch_);
  return cache_map_.size();
}

}  // namespace bustub
