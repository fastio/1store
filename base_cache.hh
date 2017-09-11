#pragma once

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>

#include "core/memory.hh"
#include "core/thread.hh"

#include "utils/logalloc.hh"
#include "utils/phased_barrier.hh"
#include "utils/histogram.hh"
#include "utils/estimated_histogram.hh"
#include "core/metrics_registration.hh"
#include "core/metrics.hh"
#include "seastarx.hh"
namespace bi = boost::intrusive;

namespace redis {
template<typename Key, typename Value>
class base_cache;

template<typename Key, typename Value>
class cache_tracker;

template<typename Key, typename Value>
class cache_entry {
    using cache_type = base_cache<Key, Value>;
    using lru_link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;
    using cache_link_type = bi::set_member_hook<bi::link_mode<bi::auto_unlink>>;

    Key _key;
    Value _value;
    lru_link_type _lru_link;
    cache_link_type _cache_link;
    friend class size_calculator;
public:
    friend class base_cache<Key, Value>;
    friend class cache_tracker<Key, Value>;

    cache_entry()
        : _key()
        , _value()
    {
    }

    cache_entry(const Key& key, const Value& value)
        : _key(key)
        , _value(value)
    {
    }

    cache_entry(cache_entry&& o) noexcept
        : _key(std::move(o._key))
        , _value(std::move(o._value))
        , _lru_link()
        , _cache_link()
    {
        using cache_tracker_type = typename cache_type::cache_tracker_type;
        if (o._lru_link.is_linked()) {
            auto prev = o._lru_link.prev_;
            o._lru_link.unlink();
            cache_tracker_type::lru_type::node_algorithms::link_after(prev, _lru_link.this_ptr());
        }
        {
            using container_type = typename cache_type::container_type;
            container_type::node_algorithms::replace_node(o._cache_link.this_ptr(), _cache_link.this_ptr());
            container_type::node_algorithms::init(o._cache_link.this_ptr());
        }
    }
    cache_entry& operator = (cache_entry&& o) noexcept
    {
        if (this != &o) {
            _key = std::move(o._key);
            _value = std::move(o._value);
            _lru_link = {};
            _cache_link = {};

            using cache_tracker_type = typename cache_type::cache_tracker_type;
            if (o._lru_link.is_linked()) {
                auto prev = o._lru_link.prev_;
                o._lru_link.unlink();
                cache_tracker_type::lru_type::node_algorithms::link_after(prev, _lru_link.this_ptr());
            }
            {
                using container_type = typename cache_type::container_type;
                container_type::node_algorithms::replace_node(o._cache_link.this_ptr(), _cache_link.this_ptr());
                container_type::node_algorithms::init(o._cache_link.this_ptr());
            }

        }
        return *this;
    }

    cache_entry(const cache_entry&) = delete;
    cache_entry& operator = (const cache_entry&) = delete;

    bool is_evictable() { return _lru_link.is_linked(); }
    const Key& key() const { return _key; }
    const Value& value() const { return _value; }
    Value& value() { return _value; }

    struct compare {
        bool operator()(const Key& a, const cache_entry& b) const {
            return a < b.key();
        }

        bool operator()(const cache_entry& a, const Key& b) const {
            return a.key() < b;
        }

        bool operator()(const cache_entry& a, const cache_entry& b) const {
            return a.key() < b.key();
        }
    };

    //friend std::ostream& operator<<(std::ostream&, cache_entry&);
};

// Tracks accesses and performs eviction of cache entries.
template<typename Key, typename Value>
class cache_tracker final {
public:
    using cache_entry_type = cache_entry<Key, Value>;
    using lru_type = bi::list<cache_entry_type,
        bi::member_hook<cache_entry_type, typename cache_entry_type::lru_link_type, &cache_entry<Key, Value>::_lru_link>,
        bi::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.
    using this_type = base_cache<Key, Value>;    
public:
    friend class base_cache<Key, Value>;
    struct stats {
        uint64_t row_hits;
        uint64_t row_misses;
        uint64_t reads;
        uint64_t reads_done;

        uint64_t active_reads() const {
            return reads_done - reads;
        }
    };
private:
    stats _stats{};
    seastar::metrics::metric_groups _metrics;
    logalloc::region _region;
    lru_type _lru;
public:
    cache_tracker() {
        setup_metrics();
        _region.make_evictable([this] {
            return with_allocator(_region.allocator(), [this] {
                // Removing a partition may require reading large keys when we rebalance
                // the rbtree, so linearize anything we read
                return with_linearized_managed_bytes([&] {
                    try {
                        auto evict_last = [this](lru_type& lru) {
                            //cache_entry_type& ce = lru.back();
                            //auto it = this_type::container_type::s_iterator_to(ce);
                            //clear_continuity(*std::next(it));
                            lru.pop_back_and_dispose(current_deleter<cache_entry_type>());
                        };
                        if (_lru.empty()) {
                            return memory::reclaiming_result::reclaimed_nothing;
                        }
                        evict_last(_lru);
                        //--_stats.partitions;
                        //++_stats.partition_evictions;
                        //++_stats.modification_count;
                        return memory::reclaiming_result::reclaimed_something;
                    } catch (std::bad_alloc&) {
                        // Bad luck, linearization during partition removal caused us to
                        // fail.  Drop the entire cache so we can make forward progress.
                        clear();
                        return memory::reclaiming_result::reclaimed_something;
                    }
                });
            });
        });
    }
    ~cache_tracker() {
        clear();
    }

    void setup_metrics() {
        namespace sm = seastar::metrics;
        _metrics.add_group("cache", {
            sm::make_gauge("bytes_used", sm::description("current bytes used by the cache out of the total size of memory"), [this] { return _region.occupancy().used_space(); }),
            sm::make_gauge("bytes_total", sm::description("total size of memory for the cache"), [this] { return _region.occupancy().total_space(); }),
        });
    }
    void clear() {
        with_allocator(_region.allocator(), [this] {
            auto clear = [this] (lru_type& lru) {
                while (!lru.empty()) {
                    cache_entry_type& ce = lru.back();
                    auto it = this_type::container_type::s_iterator_to(ce);
                    while (it->is_evictable()) {
                        cache_entry_type& to_remove = *it;
                        ++it;
                        to_remove._lru_link.unlink();
                        current_deleter<cache_entry_type>()(&to_remove);
                    }
                    //clear_continuity(*it);
                }
            };
            clear(_lru);
        });
        //_stats.partition_removals += _stats.partitions;
        //_stats.partitions = 0;
        //++_stats.modification_count;
    }

    void on_erase() {
    }

    void touch(cache_entry_type& e) {
        auto move_to_front = [this] (lru_type& lru, cache_entry_type& e) {
            lru.erase(lru.iterator_to(e));
            lru.push_front(e);
        };
        move_to_front(_lru, e);
    }
    void insert(cache_entry_type& e) {
        //++_stats.partition_insertions;
        //++_stats.partitions;
        //++_stats.modification_count;
        _lru.push_front(e);
    }
    allocation_strategy& allocator() { return _region.allocator(); }
    logalloc::region& region() { return _region; }
    const logalloc::region& region() const { return _region; }
    const stats& get_stats() const { return _stats; }
};

// Returns a reference to shard-wide cache_tracker.
template<typename Key, typename Value>
cache_tracker<Key, Value>& global_cache_tracker() {
    static thread_local cache_tracker<Key, Value> inst;
    return inst;
}

//
// A data source which wraps another data source such that data obtained from the underlying data source
// is cached in-memory in order to serve queries faster.
//
// Cache populates itself automatically during misses.
//
// Cache represents a snapshot of the underlying mutation source. When the
// underlying mutation source changes, cache needs to be explicitly synchronized
// to the latest snapshot. This is done by calling update() or invalidate().
//
template<typename Key, typename Value>
class base_cache {
public:
    using cache_entry_type = cache_entry<Key, Value>;
    using phase_type = utils::phased_barrier::phase_type;
    using container_type = bi::set<cache_entry_type,
        bi::member_hook<cache_entry_type, typename cache_entry_type::cache_link_type, &cache_entry_type::_cache_link>,
        bi::constant_time_size<false>, // we need this to have bi::auto_unlink on hooks
        bi::compare<typename cache_entry_type::compare>>;
    using cache_tracker_type = cache_tracker<Key, Value>;
public:
    struct stats {
        utils::timed_rate_moving_average hits;
        utils::timed_rate_moving_average misses;
        utils::timed_rate_moving_average reads_with_misses;
        utils::timed_rate_moving_average reads_with_no_misses;
    };
private:
    cache_tracker_type& _tracker;
    stats _stats{};
    container_type _cache;

    // There can be at most one update in progress.
    seastar::semaphore _update_sem = {1};

    logalloc::allocating_section _update_section;
    logalloc::allocating_section _populate_section;
    logalloc::allocating_section _read_section;

    void upgrade_entry(cache_entry_type&);
    void invalidate_locked(const Key& key);
    void invalidate_unwrapped(const Key& key);
    static thread_local seastar::thread_scheduling_group _update_thread_scheduling_group;

    // Must be run under reclaim lock
    cache_entry_type& find_or_create_entry(const Key& key, const Value& value);

    // Ensures that partition entry for given key exists in cache and returns a reference to it.
    // Prepares the entry for reading. "phase" must match the current phase of the entry.
    //
    // Since currently every entry has to have a complete tombstone, it has to be provided here.
    // The entry which is returned will have the tombstone applied to it.
    //
    // Must be run under reclaim lock

    typename container_type::iterator true_end() {
        return std::prev(_cache.end());
    }

    template <typename Updater>
    future<> do_update(const Key& m, Updater func);
public:
    ~base_cache() {
        with_allocator(_tracker.allocator(), [this] {
            _cache.clear_and_dispose([this, deleter = current_deleter<cache_entry_type>()] (auto&& p) mutable {
                _tracker.on_erase();
                deleter(p);
            });
        });
    }
    base_cache(cache_tracker_type& tracker)
        : _tracker(tracker)
    {
    }
    base_cache(base_cache&&) = default;
    base_cache(const base_cache&) = delete;
    base_cache& operator=(base_cache&&) = default;
public:
    // Populate cache from given mutation, which must be fully continuous.
    // Intended to be used only in tests.
    // Can only be called prior to any reads.
    void populate(const Key& key, const Value& value) {
        _populate_section(_tracker.region(), [&] {
             auto entry = current_allocator().construct<cache_entry_type>(key, value);
             upgrade_entry(*entry);
             _tracker.insert(*entry);
        });
    }

    cache_entry_type& find_or_create(const Key& key, const Value& value) {
        auto it = _cache.find(key);
        if (it != _cache.end()) {
            return *it;
        }
        auto entry = current_allocator().construct<cache_entry_type>(key, value);
        _tracker.insert(*entry);
        return _cache.insert(it, *entry);
    }

    typename container_type::iterator end() const {
        return _cache.end();
    }

    typename container_type::iterator find(const Key& key) {
        return _cache.find(key);
    }
    // Moves given partition to the front of LRU if present in cache.
    void touch(const Key&);

    future<> invalidate(const Key&);

    // Evicts entries from given key in cache.
    //
    void evict(const Key& key) {
        auto target = _cache.find(key);
        if (target != _cache.end()) {
            with_allocator(_tracker.allocator(), [this, target] {
                auto it = _cache.erase_and_dispose(target, [this, deleter = current_deleter<cache_entry_type>()] (auto&& p) mutable {
                    _tracker.on_erase();
                    deleter(p);
                });
                assert(it != _cache.end());
            });
        }
    }

    void clear_now() noexcept {
        with_allocator(_tracker.allocator(), [this] {
            auto it = _cache.erase_and_dispose(_cache.begin(), std::prev(_cache.end()), [this, deleter = current_deleter<cache_entry_type>()] (auto&& p) mutable {
                _tracker.on_erase();
                deleter(p);
            });
        });
    }

    size_t size() const {
        return _cache.size();
    }
    const cache_tracker_type& get_cache_tracker() const {
        return _tracker;
    }

    //friend std::ostream& operator<<(std::ostream&, base_cache&);

    //friend class cache_tracker_type;
};
}
