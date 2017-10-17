#include "column_family.hh"
#include "store/reader.hh"
namespace store {

int column_family::in_range(lw_shared_ptr<sstable_meta> m, const bytes_view& key) const
{
    // return 1, iff key > largest key of the sstable;
    // return -1, iff key < smallest key of the sstable
    // return 0, otherwise.
    assert(m);
    if (key > m->_largest_key) return 1;
    else if (key < m->_smallest_key) return -1;
    return 0;
}

std::vector<lw_shared_ptr<sstable_meta>> column_family::filter_file_meta_from_level_zero(bytes_view key) const
{
    return std::vector<lw_shared_ptr<sstable_meta>> {};
}

std::vector<lw_shared_ptr<sstable_meta>> column_family::filter_file_meta(bytes_view key) const
{
    std::vector<lw_shared_ptr<sstable_meta>> result;
    for (size_t i = 0; i < MAX_LEVELS; ++i) {
        auto& sstable_metas = _sstables[i];
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

lw_shared_ptr<partition> column_family::merge_multi_targets(std::vector<lw_shared_ptr<partition>>&& ps) const
{
    return make_lw_shared<partition> (nullptr);
}

future<lw_shared_ptr<partition>> column_family::try_read_from_sstable(lw_shared_ptr<sstable> stable, bytes_view key) const
{
    assert(stable);
    auto sstable_reader = make_sstable_reader(stable);
    return sstable_reader->seek(key).then([this, sstable_reader = std::move(sstable_reader)] {
        if (!sstable_reader->eof()) {
             return make_ready_future<lw_shared_ptr<partition>> ( make_lw_shared<partition>(sstable_reader->current()) );
        }
        return make_ready_future<lw_shared_ptr<partition>>( nullptr );
    });
}

void column_family::maybe_flush()
{
}


future<> column_family::write(const write_options& opt, redis::decorated_key&& key, partition&& p)
{
    _active_memtable->insert(std::move(key), std::move(p));
    maybe_flush();
    return make_ready_future<>();
}

future<lw_shared_ptr<partition>> column_family::read(const read_options& opt, const redis::decorated_key& key) const
{
    auto target = make_lw_shared<partition>(make_null_partition());

    // 1. read partition from the memtable;
    auto memtable_reader = make_memtable_reader(_active_memtable);
    memtable_reader->seek(key.key_view());
    // if the target partition was not in the memtable, the memtable_reader's eof will return true.
    if (memtable_reader->eof() == false) {
        // we read the target partition from the memtable, good news, return it to caller.
        return make_ready_future<lw_shared_ptr<partition>> ( make_lw_shared<partition>(memtable_reader->current()) ); 
    }

    // 2. read partition from the immemtables;
    // looking up the target partition from the immemtables will not blocks the thread.
    for (size_t i = 0; i < _immutable_memtables.size(); ++i) {
        auto immemtable_reader = make_memtable_reader(_immutable_memtables[i]);
        immemtable_reader->seek(key.key_view());
        if (immemtable_reader->eof() == false) {
            // we read the target partition from the memtable, good news, return it to caller.
            return make_ready_future<lw_shared_ptr<partition>> ( make_lw_shared<partition>(immemtable_reader->current()) ); 
        }
    }
    // 3. read partition from the level 0 files.
    auto&& level_zero_sstables = filter_file_meta_from_level_zero(key.key_view());
    if (!level_zero_sstables.empty()) {
        // if we meet the same key, return the newer one, which local version is bigger.
        // because the sstables in the level zero were overlaped each other.
        return do_with( std::vector<lw_shared_ptr<partition>> {}, [this, target, keyv = key.key_view(), sstables = std::move(level_zero_sstables)] (auto& target_container) { 
            return parallel_for_each(sstables.begin(), sstables.end(), [this, target, keyv, &target_container] (auto& sstable_meta) {
                // we have opened the sstable last time, try to read target from the sstable.
                if (sstable_meta->_sstable) {
                    return this->try_read_from_sstable(sstable_meta->_sstable, keyv).then([&target_container] (auto p) {
                        target_container.emplace_back(std::move(p));
                    });
                }
                else {
                    // open the target sstable, then try to read the partition.
                    return open_sstable(sstable_meta->_file_name, _sstable_opt).then([this, keyv, sstable_meta, &target_container] (auto nsstable) {
                        sstable_meta->_sstable = nsstable;
                        return this->try_read_from_sstable(sstable_meta->_sstable, keyv).then([&target_container] (auto p) {
                            target_container.emplace_back(std::move(p));
                        });
                    });
                }
            }).then([this, target, keyv, &target_container] {
                auto p =  this->merge_multi_targets(std::move(target_container));
                return make_ready_future<lw_shared_ptr<partition>> (std::move(p));
            });
        }).then([this, target, keyv = key.key_view()] (auto t) {
            if (t->empty()) {
     // 4. rpartition from other level files.
                auto&& target_sstables = this->filter_file_meta(keyv);
                if (!target_sstables.empty()) {
                    struct lookup_partition_state {
                        std::vector<lw_shared_ptr<sstable_meta>> _sstables;
                        std::vector<lw_shared_ptr<sstable_meta>>::iterator _next;
                        lw_shared_ptr<partition> _p;
                        lookup_partition_state(std::vector<lw_shared_ptr<sstable_meta>>&& sstables)
                           : _sstables(std::move(sstables))
                           , _next(_sstables.begin())
                           , _p(nullptr)
                           {
                           }
                    };
                    return do_with(lookup_partition_state { std::move(target_sstables) }, [this, keyv] (auto& state) {
                        return repeat([this, keyv, &state] {
                            if (state._next == state._sstables.end()) {
                                return make_ready_future<stop_iteration>(stop_iteration::yes);
                            }
                            auto sm = *(state._next++);
                            if (sm->_sstable) {
                                return this->try_read_from_sstable(sm->_sstable, keyv).then([&state] (auto p) {
                                    if (p) state._p = std::move(p);
                                    return make_ready_future<stop_iteration>(!p ? stop_iteration::no : stop_iteration::yes);
                                });
                            }
                            // open the target sstable, then try to read the partition.
                            return open_sstable(sm->_file_name, _sstable_opt).then([this, keyv, sm, &state] (auto nsstable) {
                                assert(!sm->_sstable);
                                sm->_sstable = nsstable;
                                return this->try_read_from_sstable(nsstable, keyv).then([&state] (auto p) {
                                    if (p) state._p = std::move(p);
                                    return make_ready_future<stop_iteration>(!p ? stop_iteration::no : stop_iteration::yes);
                                });
                            });
                        }).then([this, &state] {
                            return make_ready_future<lw_shared_ptr<partition>> ( std::move(state._p) );
                        });
                    });
                }
            }
            return make_ready_future<lw_shared_ptr<partition>> ( t );
        });
    }
    // oh, the key was not exists.
    assert(target->empty()); 
    return make_ready_future<lw_shared_ptr<partition>> ( target );
}

column_family::~column_family()
{
}
}
