#pragma once
#include <vector>
#include "core/shared_ptr.hh"
namespace store {

class memtable;
class version_manager;
class partition;
class sstable;

struct sstable_meta {
    bytes _smallest_key {};
    bytes _largest_key {};
    bytes _file_name {};
    lw_shared_ptr<sstable> _sstable = { nullptr };
    sstable_meta()
    {
    }
};

class version {
    column_family& owner_;
    lw_shared_ptr<memtable> _memtable;
    std::vector<std::vector<lw_shared_ptr<sstable_meta>> _sstable_metas;
    static constexpr int MAX_LEVELS = 9;
public:
    explicit version(column_family& o)
        : owner_(o)
        , _memtable(make_lw_shared<memtable>())
    {
        _sstables.resize(MAX_LEVELS);
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

    lw_shared_ptr<memtable> memtable() {
        return _memtable;
    }
private:
    std::vector<lw_shared_ptr<sstable_meta>> filter_file_meta(bytes_view key);
    int in_range(lw_shared_ptr<sstable_meta> m, const bytes_view& key) const;
};


class column_family final {
    version _current;
    std::vector<lw_shared_ptr<memtable>> _immutable_memtables
    version_manager& _version_manager;
    bytes _sstable_dir_name;
    bytes _column_family_name;
public:
    column_family(version_manager& vm);
    ~column_family();
    bool apply(redis::decorated_key key, redis::partition data);
    future<> write(const write_options& opt, bytes_view key, partition value);
    future<lw_shared_ptr<partition>> read(const read_options& opt, bytes_view key);
    bool apply_new_version(foregin
private:
    future<lw_shared_ptr<partition>> try_read_from_sstable(lw_shared_ptr<sstable> stable, lw_shared_ptr<partition> target, bytes_view key);
};

}
