//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include <fstream>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  directory_page_id_ = INVALID_PAGE_ID;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> page_id_t {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  std::lock_guard<std::mutex> guard(dir_page_id_m_);
  if (directory_page_id_ == INVALID_PAGE_ID) {
    // create new directory_page
    Page *dir_page_raw = buffer_pool_manager_->NewPage(&directory_page_id_);
    assert(dir_page_raw != nullptr);
    auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(dir_page_raw->GetData());
    dir_page->SetPageId(directory_page_id_);
    // create new bucket_page
    page_id_t bucket_page_id;
    Page *bucket_page_raw = buffer_pool_manager_->NewPage(&bucket_page_id);
    assert(bucket_page_raw != nullptr);
    dir_page->SetBucketPageId(0, bucket_page_id);
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    return dir_page;
  }
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(page != nullptr);
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  return dir_page;
}

// ! Acctually not used in concurrent mode
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(page != nullptr);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->RLatch();
  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  bool succeed = bucket_page->GetValue(key, comparator_, result);
  page->RUnlatch();
  // auto bucket_page = FetchBucketPage(bucket_page_id);
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  table_latch_.RUnlock();
  return succeed;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->WLatch();
  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  // auto bucket_page = FetchBucketPage(bucket_page_id);
  if (!bucket_page->IsFull()) {
    bool succeed = bucket_page->Insert(key, value, comparator_);
    page->WUnlatch();
    if (succeed) {
      buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    } else {
      buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    }
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    table_latch_.RUnlock();
    return succeed;
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  table_latch_.RUnlock();
  // unpin operations left to SplitInsert
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // this bucket is full, should split
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto split_bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t split_bucket_page_id = dir_page->GetBucketPageId(split_bucket_idx);
  // if reach max_depth, return false
  if ((1 << dir_page->GetLocalDepth(split_bucket_idx)) == DIRECTORY_ARRAY_SIZE) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    assert(buffer_pool_manager_->UnpinPage(split_bucket_page_id, false));
    table_latch_.WUnlock();
    return false;
  }
  // if reach global_depth, increase it
  if (dir_page->GetLocalDepth(split_bucket_idx) == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }
  // split the local bucket
  dir_page->IncrLocalDepth(split_bucket_idx);
  // modify information of all slots in directory that points to the split bucket
  uint32_t stride = 1 << dir_page->GetLocalDepth(split_bucket_idx);
  for (int i = split_bucket_idx; i >= 0; i -= stride) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_idx));
  }
  for (uint32_t i = split_bucket_idx; i < dir_page->Size(); i += stride) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_idx));
  }

  auto buddy_bucket_idx = dir_page->GetSplitImageIndex(split_bucket_idx);
  page_id_t buddy_bucket_page_id;
  buffer_pool_manager_->NewPage(&buddy_bucket_page_id);
  assert(buffer_pool_manager_->UnpinPage(buddy_bucket_page_id, true));
  // modify information of all slots in directory that points to the buddy bucket
  for (int i = buddy_bucket_idx; i >= 0; i -= stride) {
    dir_page->SetBucketPageId(i, buddy_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_idx));
  }
  for (uint32_t i = buddy_bucket_idx; i < dir_page->Size(); i += stride) {
    dir_page->SetBucketPageId(i, buddy_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_idx));
  }
  // redistribute the key-value pair in old bucket
  Page *split_bucket_page_raw = buffer_pool_manager_->FetchPage(split_bucket_page_id);
  split_bucket_page_raw->WLatch();
  auto split_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(split_bucket_page_raw->GetData());
  Page *buddy_bucket_page_raw = buffer_pool_manager_->FetchPage(buddy_bucket_page_id);
  buddy_bucket_page_raw->WLatch();
  auto buddy_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buddy_bucket_page_raw->GetData());
  auto num_readable = split_bucket_page->NumReadable();
  for (size_t i = 0; i < num_readable; i++) {
    auto key_at_i = split_bucket_page->KeyAt(i);
    if (split_bucket_page_id != KeyToPageId(key_at_i, dir_page)) {
      assert(buddy_bucket_page->Insert(key_at_i, split_bucket_page->ValueAt(i), comparator_));
      split_bucket_page->RemoveAt(i);
    }
  }
  split_bucket_page_raw->WUnlatch();
  buddy_bucket_page_raw->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage(split_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(buddy_bucket_page_id, true));
  table_latch_.WUnlock();
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->WLatch();
  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  // HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  bool succeed = bucket_page->Remove(key, value, comparator_);
  page->WUnlatch();
  if (succeed) {
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  } else {
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  }
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  if (bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
  }
  return succeed;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  // check if local_depth is greater than 0;
  auto local_depth = dir_page->GetLocalDepth(bucket_idx);
  if (local_depth == 0) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }
  // check if local_depth is same as the buddy_bucket's
  auto buddy_bucket_idx = dir_page->GetSplitImageIndex(bucket_idx);
  if (local_depth != dir_page->GetLocalDepth(buddy_bucket_idx)) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }
  auto bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  Page *bucket_page_raw = buffer_pool_manager_->FetchPage(bucket_page_id);
  bucket_page_raw->WLatch();
  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page_raw->GetData());
  // auto bucket_page = FetchBucketPage(bucket_page_id);
  // check if empty now
  if (!bucket_page->IsEmpty()) {
    bucket_page_raw->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    table_latch_.WUnlock();
    return;
  }
  bucket_page_raw->WUnlatch();
  // delete the empty page
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->DeletePage(bucket_page_id));
  // modify the corresponding slots in dir_page
  page_id_t buddy_bucket_page_id = dir_page->GetBucketPageId(buddy_bucket_idx);
  auto stride = 1 << (local_depth - 1);
  for (int i = bucket_idx; i >= 0; i -= stride) {
    dir_page->SetBucketPageId(i, buddy_bucket_page_id);
    dir_page->SetLocalDepth(i, local_depth - 1);
  }
  for (uint32_t i = bucket_idx; i < dir_page->Size(); i += stride) {
    dir_page->SetBucketPageId(i, buddy_bucket_page_id);
    dir_page->SetLocalDepth(i, local_depth - 1);
  }
  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
