#pragma once
#include <vector>
#include "core/shared_ptr.hh"
#include "store/table.hh"
#include "store/memtable.hh"
#include "store/log_writer.hh"
#include "partition.hh"
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

class column_family;
class version {
    column_family& owner_;
    lw_shared_ptr<memtable> _memtable;
    std::vector<std::vector<lw_shared_ptr<sstable_meta>>> _sstable_metas;
    static constexpr int MAX_LEVELS = 9;
public:
    explicit version(column_family& o)
        : owner_(o)
        , _memtable(make_lw_shared<memtable>())
    {
        _sstable_metas.resize(MAX_LEVELS);
    }

    version(version&& o)
        : owner_(o.owner_)
        , _memtable(std::move(o._memtable))
        , _sstable_metas(std::move(o._sstable_metas))
    {
    }

    ~version()
    {
    }

    lw_shared_ptr<memtable> release_memtable() {
        auto result = _memtable;
        _memtable = {};
        return result;
    }
private:
    std::vector<lw_shared_ptr<sstable_meta>> filter_file_meta(bytes_view key);
    int in_range(lw_shared_ptr<sstable_meta> m, const bytes_view& key) const;
    friend class column_family;
};

class write_options;
class read_options;
class column_family final {
    version _current;
    std::vector<lw_shared_ptr<memtable>> _immutable_memtables;
    bytes _sstable_dir_name;
    bytes _column_family_name;
public:
    column_family();
    ~column_family();
    future<> write(const write_options& opt, redis::decorated_key&& key, partition&& p);
    future<lw_shared_ptr<partition>> read(const read_options& opt, redis::decorated_key&& key);
    bool apply_new_sstables(foreign_ptr<level_manifest_wrapper> news);
private:
    future<lw_shared_ptr<partition>> try_read_from_sstable(lw_shared_ptr<sstable> stable, lw_shared_ptr<partition> target, bytes_view key);
    void maybe_flush(version& v);
};

}
