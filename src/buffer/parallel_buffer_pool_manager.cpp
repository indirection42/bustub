//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager):num_instances_(num_instances),pool_size_(num_instances*pool_size),start_index_(0) {
  // Allocate and create individual BufferPoolManagerInstances
  // vector should be initialized, but it seems not a initialization.
  // for (size_t i=0;i<num_instances;i++){
  //   buffer_pools_.emplace_back(BufferPoolManagerInstance(pool_size,num_instances,i,disk_manager,log_manager));
  // }
  buffer_pools_ = static_cast<BufferPoolManagerInstance*> (operator new[](sizeof(BufferPoolManagerInstance)*num_instances));
  // placement new
  for (size_t i=0;i<num_instances;i++){
    new(buffer_pools_+i) BufferPoolManagerInstance(pool_size,num_instances,i,disk_manager,log_manager
    );
  }
}

// Update destructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager(){
  for (size_t i=0;i<num_instances_;i++){
    buffer_pools_[i].~BufferPoolManagerInstance();
  }
  operator delete[](buffer_pools_);
}

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return pool_size_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return &buffer_pools_[page_id % num_instances_];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool=GetBufferPoolManager(page_id);
  return buffer_pool->FetchPage(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool=GetBufferPoolManager(page_id); 
  return buffer_pool->UnpinPage(page_id,is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool=GetBufferPoolManager(page_id); 
  return buffer_pool->FlushPage(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  size_t i=start_index_;
  Page *p=nullptr;
  do{
    p=buffer_pools_[i].NewPage(page_id);
    i=(i+1)%num_instances_;
  }while(p==nullptr&&i!=start_index_);
  start_index_=(start_index_+1)%num_instances_;
  return p;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool=GetBufferPoolManager(page_id); 
  return buffer_pool->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i=0;i<num_instances_;i++){
    buffer_pools_[i].FlushAllPages();
  }
}

}  // namespace bustub
