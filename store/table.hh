#pragma once
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "utils/bytes.hh"
#include "store/file_reader.hh"
#include <memory>
namespace store {

struct sstable_options {
    size_t _sstable_buffer_size = 4096;
    lw_shared_ptr<block_cache> _block_cache;
    lw_shared_ptr<table_cache> _table_cache;
};
// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class sstable {
 public:
  static future<lw_shared_ptr<table>> open(bytes fname, const sstable_options& options);

  ~sstable();

  uint64_t approximate_offset_of(const slice& key) const;

 private:
  struct rep;
  std::unique_ptr<rep> rep_;

  explicit sstable(std::unique_ptr<rep> rep) { rep_ = rep; }

  // No copying allowed
  sstable(const table&) = delete;
  void operator=(const table&) = delete;
};
}
