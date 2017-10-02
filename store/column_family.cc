#include "column_family.hh"
namespace store {

int version::in_range(lw_shared_ptr<sstable_meta> m, bytes_view key)
{
    // return 1, iff key > largest key of the sstable;
    // return -1, iff key < smallest key of the sstable
    // return 0, otherwise.
    assert(m);
    if (key > m->_largest_key) return 1;
    else if (key < m->_smallest_key) return -1;
    return 0;
}
std::vector<lw_shared_ptr<sstable_meta>> version::filter_file_meta(bytes_view key)
{
    std::vector<lw_shared_ptr<sstable_meta>> result;
    for (size_t i = 0; i < MAX_LEVELS; ++i) {
        auto& sstable_metas = _sstables_metas[i];
        auto files_number = sstable_metas.size();
        if (i == 0) {
            // iterate every files in the level 0.
            for (size_t k = 0; k < files_number; ++k) {
                if (in_range(sstable_metas[k], key) == 0) {
                    result.push_back(sstable_metas[k]);
                    break;
                }
            }
        }
        else {
            // binary search to lookup the target files.
            size_t begin = 0, end = sstable_metas.size();
            for (; begin < end; ) {
                auto m = (begin + end) >> 1;
                auto c = in_range(sstable_metas[m], key);
                if (c == 0) {
                    result.push_back(sstable_metas[m]);
                    break;
                }
                else if (c > 0) {
                    begin = m + 1;
                }
                else {
                    end = m - 1;
                }
            }
        }
    }
    return std::move(result);
}

future<lw_shared_ptr<partition>> column_faily::try_read_from_sstable(lw_shared_ptr<sstable> stable, lw_shared_ptr<partition> target, bytes_view key)
{
    assert(stable);
    auto sstable_reader = make_sstable_reader(sstable);
    return sstable_reader->seek(key).then([this, target, sstable_reader = std::move(sstable_reader)] {
        if (!sstable_reader->eof()) {
            target.replace_if_newer(sstable_reader->curret());
        }
        return make_ready_future<lw_shared_ptr<partition>> { target };
    });
}

future<lw_shared_ptr<partition>> column_faily::read(const read_options& opt, bytes_view key)
{
    auto target = make_empty_partition();

    // 1. read partition from the memtable;
    auto memtable_reader = make_memtable_reader(_current.memtable());
    memtable_reader->seek(key);
    // if the target partition was not in the memtable, the memtable_reader's eof will return true.
    if (memtable_reader->eof() == false) {
        // we read the target partition from the memtable, good news, return it to caller.
        return make_ready_future<lw_shared_ptr<partition>> { memtable_reader->current() }; 
    }

    // 2. read partition from the immemtables;
    // looking up the target partition from the immemtables will not blocks the thread.
    for (size_t i = 0; i < _immutable_memtables.size(); ++i) {
        auto immemtable_reader = make_memtable_reader(_immutable_memtables[i]);
        immemtable_reader->seek(key);
        if (immemtable_reader->eof() == false) {
            // we read the target partition from the memtable, good news, return it to caller.
            return make_ready_future<lw_shared_ptr<partition>> { immemtable_reader->current() }; 
        }
    }
    // 3. read partition from the level 0 files.
    auto level_zero_sstables = _current.filter_sstable_meta_from_level_zero(key);
    if (!level_zero_sstables.empty()) {
        // if we meet the same key, return the newer one, which local version is bigger.
        // because the sstables in the level zero were overlaped each other.
        return parallel_for_each(level_zero_sstables.begin(), level_zero_sstables.end(), [this, target, key] (auto& sstable_meta) {
            // we have opened the sstable last time, try to read target from the sstable.
            if (sstable_meta->_sstable) {
                return try_read_from_sstable(sstable_meta->_sstable, target, key);
            }
            // open the target sstable, then try to read the partition.
            return open_sstable(sstable_meta).then([this, target, key] (auto sstable) {
                return try_read_from_sstable(sstable, target, key);
            });
        }).then([this, target] {
            if (target->empty()) {
     // 4. read partition from other level files.
                auto& target_sstables = _current.filter_sstable_meta(key);
                if (target_sstables.empty()) {
                    return do_with(std::move(target_sstables), std::move(target_sstables.begin()), [this, target, key] (auto& sstables, auto& iter) {
                        return do_until ([target, &sstables, &iter] () { target->empty() == false || iter == sstables.end(); }, [this, target, &iter, &sstables, key] {
                            auto& sstable_meta = *iter++;
                            if (sstable_meta->_sstable) {
                                return try_read_from_sstable(sstable_meta->_sstable, target, key);
                            }
                            // open the target sstable, then try to read the partition.
                            return open_sstable(sstable_meta).then([this, target, key] (auto sstable) {
                                return try_read_from_sstable(sstable, target, key);
                            });
                        }).then([this, target] {
                            return make_ready_future<lw_shared_ptr<partition>> { target };
                        });
                    });
                }
            }
            return make_ready_future<lw_shared_ptr<partition>> { target };
        });
    }
    // oh, the key was not exists.
    assert(target->empty()); 
    return make_ready_future<lw_shared_ptr<partition>> { target };
}
