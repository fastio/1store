#pragma once
#include <vector>
#include "core/shared_ptr.hh"
#include "memtable.hh"
#include "partition.hh"
#include "core/sharded.hh"
namespace store {

struct sstable_holder {
    bytes _smallest_key {};
    bytes _largest_key {};
    bytes _file_name {};
    sstable_holder()
    {
    }
};

class column_family final {
    static constexpr int MAX_LEVELS = 9;
    std::vector<std::vector<lw_shared_ptr<sstable_holder>>> _sstables;
    lw_shared_ptr<memtable> _active_memtable;
    std::vector<lw_shared_ptr<memtable>> _immutable_memtables;
    bytes _sstable_dir_name;
    bytes _column_family_name;
    bool _has_commitlog;
public:
    future<> apply(cache_entry& e);
    column_family(bytes name, bool with_commitlog);
    ~column_family();
};

}
