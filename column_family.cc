#include "column_family.hh"
#include "store/reader.hh"
namespace store {

column_family::column_family(bytes name, bool with_commitlog)
    : _sstables()
    , _active_memtable(nullptr)
    , _immutable_memtables()
    , _sstable_dir_name(name)
    , _has_commitlog(with_commitlog)
{
}

column_family::~column_family()
{
}

future<> column_family::apply(cache_entry& e)
{
    return make_ready_future<>();
}
}
