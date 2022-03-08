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

LRUReplacer::LRUReplacer(size_t num_pages) { max_size = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  latch.lock();
  if (l.empty()) {
    return false;
  }
  frame_id_t id = l.back();
  *frame_id = id;
  l.pop_back();
  mmap.erase(id);
  latch.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch.lock();
  if (mmap.find(frame_id) != mmap.end()) {
    l.erase(mmap[frame_id]);
    mmap.erase(frame_id);
  }
  latch.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch.lock();
  if (mmap.find(frame_id) != mmap.end()) {
    latch.unlock();
    return;
  } else {
    l.push_front(frame_id);
    mmap[frame_id] = l.begin();
  }
  latch.unlock();
}

size_t LRUReplacer::Size() { return mmap.size(); }

}  // namespace bustub
