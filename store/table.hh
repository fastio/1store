#pragma once
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "utils/bytes.hh"
#include "utils/managed_bytes.hh"
#include "store/file_reader.hh"
#include "lru_cache.hh"
#include <memory>
namespace store {

class sstable_cache;
struct sstable_options {
    size_t _sstable_buffer_size = 4096;
};
// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class sstable {
    struct rep;
public:
    explicit sstable(std::unique_ptr<rep> rep);
    explicit sstable() = default;
    sstable(sstable&&) = default;
    sstable& operator = (sstable&&) = default;
    ~sstable();
    uint64_t approximate_offset_of(const bytes_view& key) const;
private:
    std::unique_ptr<rep> rep_;

    // No copying allowed
    sstable(const sstable&) = delete;
    void operator=(const sstable&) = delete;
};

future<lw_shared_ptr<sstable>> open_sstable(bytes fname, const sstable_options& options);

class sstable_cache : public redis::base_cache<managed_bytes, lw_shared_ptr<sstable>> {
public:
    sstable_cache()
        : redis::base_cache<managed_bytes, lw_shared_ptr<sstable>> (redis::global_cache_tracker<managed_bytes, lw_shared_ptr<sstable>>())
    {
    }
    sstable_cache(sstable_cache&&) = delete;
    sstable_cache(const sstable_cache&) = delete;
    sstable_cache& operator = (const sstable_cache&) = delete;
    sstable_cache& operator = (sstable_cache&&) = delete;
};
}
