#pragma once
#include <stddef.h>
#include <stdint.h>
#include "iterator.h"

namespace leveldb {

struct block_contents;
class comparator;

class block {
 public:
  explicit block(const block_contents& contents);

  ~block();

  size_t size() const { return size_; }
  iterator* new_iterator(const comparator* comparator);
  block(const block&) = delete;
  void operator=(const block&);

 private:
  uint32_t num_restarts() const;

  const char* data_;
  size_t size_;
  uint32_t restart_offset_;     // Offset in data_ of restart array
  bool owned_;                  // Block owns data_[]

  class Iter;
};

}
