#pragma once
#include <vector>
#include "core/shared_ptr.hh"
#include "store/table.hh"
#include "store/memtable.hh"
#include "store/log_writer.hh"
#include "partition.hh"
#include "core/sharded.hh"
namespace store {

struct sstable_meta {
    bytes _smallest_key {};
    bytes _largest_key {};
    bytes _file_name {};
    lw_shared_ptr<sstable> _sstable = { nullptr };
    sstable_meta()
    {
    }
};

class level_manifest_wrapper {
    unsigned int _source_shard;
    std::vector<std::vector<lw_shared_ptr<sstable_meta>>> _sstable_metas;
public:
    level_manifest_wrapper() = default;
    level_manifest_wrapper(const level_manifest_wrapper&) = default;
    level_manifest_wrapper(level_manifest_wrapper&& o)
        : _source_shard(std::move(o._source_shard))
        , _sstable_metas(std::move(o._sstable_metas))
    {
    }
    level_manifest_wrapper& operator = (level_manifest_wrapper&& o) {
        if (&o != this) {
            _source_shard = std::move(o._source_shard);
            _sstable_metas = std::move(o._sstable_metas);
        }
        return *this;
    }
};

class write_options;
class read_options;
class column_family final {
    static constexpr int MAX_LEVELS = 9;
    std::vector<std::vector<lw_shared_ptr<sstable_meta>>> _sstables;
    lw_shared_ptr<memtable> _active_memtable;
    std::vector<lw_shared_ptr<memtable>> _immutable_memtables;
    bytes _sstable_dir_name;
    bytes _column_family_name;
    sstable_options _sstable_opt;
public:
    column_family();
    ~column_family();
    future<> write(const write_options& opt, redis::decorated_key&& key, partition&& p);
    future<lw_shared_ptr<partition>> read(const read_options& opt, const redis::decorated_key& key) const;
    bool apply_new_sstables(foreign_ptr<level_manifest_wrapper> news);
private:
    future<lw_shared_ptr<partition>> try_read_from_sstable(lw_shared_ptr<sstable> stable, bytes_view key) const;
    void maybe_flush();
    int in_range(lw_shared_ptr<sstable_meta> m, const bytes_view& key) const;
    std::vector<lw_shared_ptr<sstable_meta>> filter_file_meta(bytes_view key) const;
    std::vector<lw_shared_ptr<sstable_meta>> filter_file_meta_from_level_zero(bytes_view key) const;
    lw_shared_ptr<partition> merge_multi_targets(std::vector<lw_shared_ptr<partition>>&& ps) const;
};

}
