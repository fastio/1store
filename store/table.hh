#pragma once
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "utils/bytes.hh"
#include "store/file_reader.hh"
namespace store {

class block_cache;
class table_cache;
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
  rep* rep_;

  explicit sstable(sep* rep) { rep_ = rep; }

  // No copying allowed
  sstable(const table&) = delete;
  void operator=(const table&) = delete;
};

class file_random_access_reader;
struct block_handle;
extern future<temporary_buffer<char>> read_block(lw_shared_ptr<file_random_access_reader> r, block_handle index);
}
