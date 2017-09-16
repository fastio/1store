#pragma once
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "utils/bytes.hh"
#include "store/file_reader.hh"
#include <memory>
namespace store {

struct sstable_options {
    size_t _sstable_buffer_size = 4096;
    lw_shared_ptr<sstable_cache> _sstable_cache;
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

class sstable_cache : public base_cache<managed_bytes, lw_shared_ptr<sstable>> {
public:
    sstable_cache()
        : redis::base_cache<managed_bytes, lw_shared_ptr<sstable>> (redis::global_cache_tracker<managed_bytes, lw_shared_ptr<block>>())
    {
    }
    sstable_cache(sstable_cache&&) = delete;
    sstable_cache(const sstable_cache&) = delete;
    sstable_cache& operator = (const sstable_cache&) = delete;
    sstable_cache& operator = (sstable_cache&&) = delete;
};
}
