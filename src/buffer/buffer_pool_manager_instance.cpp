//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

#include "common/logger.h"
namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  std::unique_lock lck(latch_);
  // page in buffer pool
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto &page = pages_[page_table_[page_id]];
  // flush if page is dirty;
  if (page.is_dirty_) {
    // LOG_DEBUG("# Instance %d, Pages: %d, data(write_back):%s\n",instance_index_, page.page_id_,page.data_);
    disk_manager_->WritePage(page_id, page.data_);
    page.is_dirty_ = false;
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::unique_lock lck(latch_);
  for (auto [page_id, frame_id] : page_table_) {
    auto &page = pages_[frame_id];
    if (page.is_dirty_) {
      // LOG_DEBUG("# Instance %d, Pages: %d, data(write_back):%s\n",instance_index_, page.page_id_,page.data_);
      disk_manager_->WritePage(page_id, page.data_);
      page.is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::unique_lock lck(latch_);
  // Try to fetch
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    // LOG_DEBUG("# Instance %d, Get Frame: %d in free_list", instance_index_,frame_id);
  } else {
    if (!replacer_->Victim(&frame_id)) {
      *page_id = INVALID_PAGE_ID;
      return nullptr;
    }
    auto victim_page_id = pages_[frame_id].page_id_;
    // LOG_DEBUG("# Instance %d, Victim Frame: %d (Page %d) in replacer",instance_index_, frame_id, victim_page_id);
    page_table_.erase(victim_page_id);
  }
  // reset P's metadata, zero out memory, dirty is true(can be write-back later);
  // Allocate (if return nullptr, )
  page_id_t new_page_id = AllocatePage();
  *page_id = new_page_id;
  auto &page = pages_[frame_id];
  if (page.is_dirty_) {
    // LOG_DEBUG("# Instance %d, Old Page: %d, data(write-back): %s",instance_index_, page.page_id_,page.data_);
    disk_manager_->WritePage(page.page_id_, page.data_);
  }
  // Different with FetchPgImp(read from disk)
  page.ResetMemory();
  page.page_id_ = new_page_id;
  // LOG_DEBUG("# Instance %d, Pin new Page %d in Frame %d",instance_index_, page.page_id_,frame_id);
  // set pinned, since it a new page, no need to call replacer_->Pin()
  page.pin_count_ = 1;
  page.is_dirty_ = false;
  // Add P to the page table;
  // lck.lock();
  page_table_[new_page_id] = frame_id;
  return &page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::unique_lock lck(latch_);
  // in buffer bool, directly return
  if (page_table_.find(page_id) != page_table_.end()) {
    // LOG_DEBUG("# Instance %d, Page %d in buffer pool",instance_index_, page_id);
    frame_id_t frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }
  frame_id_t frame_id;
  // find in free_list first
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    // LOG_DEBUG("# Instance %d, Get Frame %d from free_list",instance_index_, frame_id);
  } else {
    // all pages in buffer pool are pinned
    if (!replacer_->Victim(&frame_id)) {
      return nullptr;
    }
    auto victim_page_id = pages_[frame_id].page_id_;
    // LOG_DEBUG("# Instance %d, Victim Frame %d(Page  %d) from replacer",instance_index_, frame_id,victim_page_id);
    page_table_.erase(victim_page_id);
  }
  auto &page = pages_[frame_id];
  // if R is dirty(from the replacer), write out
  if (page.is_dirty_) {
    //  LOG_DEBUG("#Instance %d, Page: %d, data(write_back): %s",instance_index_,page.page_id_,page.data_);
    disk_manager_->WritePage(page.page_id_, page.data_);
  }
  // Update P
  disk_manager_->ReadPage(page_id, page.data_);
  // LOG_DEBUG("# Instance %d, Page %d, data(read_from): %s",instance_index_,page_id,page.data_);
  page.page_id_ = page_id;
  // set pinned, since its new, no need to call replacer_.Pin()
  page.pin_count_ = 1;
  page.is_dirty_ = false;
  // update page_table_
  page_table_[page_id] = frame_id;
  return &page;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::unique_lock lck(latch_);
  // not in buffer pool, just deallocate
  if (page_table_.find(page_id) == page_table_.end()) {
    DeallocatePage(page_id);
    return true;
  }
  auto frame_id = page_table_[page_id];
  // lck.unlock();
  auto &page = pages_[frame_id];
  if (page.pin_count_ > 0) {
    return false;
  }
  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.is_dirty_ = false;
  // TODO(jiyuanz) check if race condition(delete,new,fetch)
  page_table_.erase(page_id);
  DeallocatePage(page_id);
  free_list_.emplace_back(frame_id);
  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::unique_lock lck(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  auto &page = pages_[frame_id];
  if (pages_[frame_id].pin_count_ <= 0) {
    // LOG_DEBUG("#Instance %d, Cannot unpin Page: %d",instance_index_, page_id);
    return false;
  }
  page.is_dirty_ |= is_dirty;
  page.pin_count_--;
  if (page.pin_count_ == 0) {
    // LOG_DEBUG("#Instance %d,  Unpin Page: %d in replacer",instance_index_, page_id);
    replacer_->Unpin(frame_id);
  }
  // LOG_DEBUG("#Instance %d, page_table size: %lu",instance_index_, page_table_.size());
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
