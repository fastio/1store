/*
* Pedis is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* You may obtain a copy of the License at
*
*     http://www.gnu.org/licenses
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*  Copyright (c) 2016-2026, Peng Jian, pstack@163.com. All rights reserved.
*
*/
#pragma once
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include "utils/managed_ref.hh"
#include "utils/managed_bytes.hh"
#include "utils/allocation_strategy.hh"
#include "utils/logalloc.hh"
#include "structures/list_lsa.hh"
#include "structures/dict_lsa.hh"
#include "structures/sset_lsa.hh"
#include "structures/hll.hh"
#include "core/timer-set.hh"
#include "util/log.hh"
#include "keys.hh"
#include "utils/bytes.hh"
#include "seastarx.hh"
#include "column_family.hh"
#include "types.hh"
namespace bi = boost::intrusive;
namespace redis {
using clock_type = lowres_clock;
static constexpr clock_type::time_point never_expire_timepoint = clock_type::time_point(clock_type::duration::min());
class cache;
struct expiration {
    using time_point = clock_type::time_point;
    using duration   = time_point::duration;
    time_point _time = never_expire_timepoint;

    expiration() {}

    expiration(long s) {
        using namespace std::chrono;
        static_assert(sizeof(clock_type::duration::rep) >= 8, "clock_type::duration::rep must be at least 8 bytes wide");

        if (s == 0U) {
            return; // means never expire.
        } else {
            _time = clock_type::now() + milliseconds(s);
        }
    }

    inline const bool ever_expires() const {
        return _time != never_expire_timepoint;
    }

    inline const time_point to_time_point() const {
        return _time;
    }

    inline void set_never_expired() {
        _time = never_expire_timepoint;
    }
};

class cache_entry
{
protected:
    friend class cache;
    using list_link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;
    using hook_type = boost::intrusive::unordered_set_member_hook<>;
    hook_type _cache_link;
    data_type _type;
    managed_ref<managed_bytes> _key;
    size_t _key_hash;
    union storage {
        double _float_number;
        int64_t _integer_number;
        managed_ref<managed_bytes> _bytes;
        managed_ref<list_lsa> _list;
        managed_ref<dict_lsa> _dict;
        managed_ref<sset_lsa> _sset;
        storage() {}
        ~storage() {}
    } _u;
    bi::list_member_hook<> _timer_link;
    expiration _expiry;
    bool _dirty { true };
    clock_type::time_point _last_touched;
    list_link_type _dirty_link;
    list_link_type _lru_link;
public:
    using time_point = expiration::time_point;
    using duration = expiration::duration;
    cache_entry(const bytes& key, size_t hash, data_type type) noexcept
        : _cache_link()
        , _type(type)
        , _key_hash(hash)
        , _dirty_link()
        , _lru_link()
    {
        _key = make_managed<managed_bytes>(bytes_view{key.data(), key.size()});
    }

    cache_entry(const bytes& key, size_t hash, double data) noexcept
        : cache_entry(key, hash, data_type::numeric)
    {
        _u._float_number = data;
    }

    cache_entry(const bytes key, size_t hash, int64_t data) noexcept
        : cache_entry(key, hash, data_type::int64)
    {
        _u._integer_number = data;
    }

    cache_entry(const bytes key, size_t hash, size_t origin_size) noexcept
        : cache_entry(key, hash, data_type::bytes)
    {
        //_u._bytes = make_managed<managed_bytes>(origin_size, 0);
    }

    cache_entry(const bytes& key, size_t hash, const bytes& data) noexcept
        : cache_entry(key, hash, data_type::bytes)
    {
        _u._bytes = make_managed<managed_bytes>(bytes_view{data.data(), data.size()});
    }
    struct list_initializer {};
    cache_entry(const bytes& key, size_t hash, list_initializer) noexcept
        : cache_entry(key, hash, data_type::list)
    {
        _u._list = make_managed<list_lsa>();
    }

    struct dict_initializer {};
    cache_entry(const bytes& key, size_t hash, dict_initializer) noexcept
        : cache_entry(key, hash, data_type::dict)
    {
        _u._dict = make_managed<dict_lsa>();
    }

    struct set_initializer {};
    cache_entry(const bytes& key, size_t hash, set_initializer) noexcept
        : cache_entry(key, hash, data_type::set)
    {
        _u._dict = make_managed<dict_lsa>();
    }

    struct sset_initializer {};
    cache_entry(const bytes& key, size_t hash, sset_initializer) noexcept
        : cache_entry(key, hash, data_type::sset)
    {
        _u._sset = make_managed<sset_lsa>();
    }

    struct hll_initializer {};
    cache_entry(const bytes& key, size_t hash, hll_initializer) noexcept
        : cache_entry(key, hash, data_type::hll)
    {
        //_u._bytes = make_managed<managed_bytes>(HLL_BYTES_SIZE, 0);
    }

    cache_entry(cache_entry&& o) noexcept;

    virtual ~cache_entry()
    {
        switch (_type) {
            case data_type::numeric:
            case data_type::int64:
                break;
            case data_type::bytes:
            case data_type::hll:
                _u._bytes.~managed_ref<managed_bytes>();
                break;
            case data_type::list:
                _u._list.~managed_ref<list_lsa>();
                break;
            case data_type::dict:
            case data_type::set:
                _u._dict.~managed_ref<dict_lsa>();
                break;
            case data_type::sset:
                _u._sset.~managed_ref<sset_lsa>();
                break;
            default:
                break;
        }
    }

    const bytes type_name() const
    {
        return {};
    }
    friend inline bool operator == (const cache_entry &l, const cache_entry &r) {
        return (l._key_hash == r._key_hash) && (*(l._key) == *(r._key));
    }

    friend inline std::size_t hash_value(const cache_entry& e) {
        return e._key_hash;
    }

    struct compare {
    public:
        inline bool operator () (const cache_entry& l, const cache_entry& r) const {
            const auto& lk = *(l._key);
            const auto& rk = *(r._key);
            return (l.key_hash() == r.key_hash()) && (lk == rk);
        }
        inline bool operator () (const redis_key& k, const cache_entry& e) const {
            return (k.hash() == e.key_hash()) && (k.size() == e.key_size()) && (memcmp(k.data(), e.key_data(), k.size()) == 0);
        }
        inline bool operator () (const cache_entry& e, const redis_key& k) const {
            return (k.hash() == e.key_hash()) && (k.size() == e.key_size()) && (memcmp(k.data(), e.key_data(), k.size()) == 0);
        }
    };
public:
    inline const clock_type::time_point get_timeout() const
    {
        return _expiry.to_time_point();
    }

    inline const bool ever_expires() const
    {
        return _expiry.ever_expires();
    }

    inline void set_never_expired()
    {
        return _expiry.set_never_expired();
    }

    inline void set_expiry(const expiration& expiry)
    {
        _expiry = expiry;
    }

    inline const size_t time_of_live() const
    {
        auto dur = get_timeout() - clock_type::now();
        return static_cast<size_t>(std::chrono::duration_cast<std::chrono::milliseconds>(dur).count());
    }

    inline bool cancel() const
    {
        return false;
    }

    inline size_t key_hash() const
    {
        return _key_hash;
    }

    inline size_t key_size() const
    {
        return _key->size();
    }

    inline const bytes_view key() const
    {
        return { _key->data(), _key->size() };
    }

    inline const char* key_data() const
    {
        return _key->data();
    }
    inline size_t value_bytes_size() const
    {
        return _u._bytes->size();
    }
    inline const char* value_bytes_data() const
    {
        return _u._bytes->data();
    }
    inline data_type type() const
    {
        return _type;
    }
    inline bool type_of_float() const
    {
        return _type == data_type::numeric;
    }
    inline bool type_of_integer() const
    {
        return _type == data_type::int64;
    }
    inline bool type_of_bytes() const
    {
        return _type == data_type::bytes;
    }
    inline bool type_of_list() const
    {
        return _type == data_type::list;
    }
    inline bool type_of_map() const {
        return _type == data_type::dict;
    }
    inline bool type_of_set() const {
        return _type == data_type::set;
    }
    inline bool type_of_sset() const {
        return _type == data_type::sset;
    }
    inline bool type_of_hll() const {
        return _type == data_type::hll;
    }
    inline int64_t value_integer() const
    {
        return _u._integer_number;
    }
    inline void value_integer_incr(int64_t step)
    {
        _u._integer_number += step;
    }

    inline double value_float() const
    {
        return _u._float_number;
    }

    inline void value_float_incr(double step)
    {
        _u._float_number += step;
    }
    inline managed_bytes& value_bytes() {
        return *(_u._bytes);
    }
    inline const managed_bytes& value_bytes() const {
        return *(_u._bytes);
    }
    inline list_lsa& value_list() {
        return *(_u._list);
    }
    inline const list_lsa& value_list() const {
        return *(_u._list);
    }
    inline dict_lsa& value_map() {
        return *(_u._dict);
    }
    inline const dict_lsa& value_map() const {
        return *(_u._dict);
    }
    inline dict_lsa& value_set() {
        return *(_u._dict);
    }
    inline const dict_lsa& value_set() const {
        return *(_u._dict);
    }
    inline sset_lsa& value_sset() {
        return *(_u._sset);
    }
    inline const sset_lsa& value_sset() const {
        return *(_u._sset);
    }
};

static constexpr const size_t DEFAULT_INITIAL_SIZE = 1 << 20;

class cache {
    using cache_type = boost::intrusive::unordered_set<cache_entry,
        boost::intrusive::member_hook<cache_entry, cache_entry::hook_type, &cache_entry::_cache_link>,
        boost::intrusive::power_2_buckets<true>,
        boost::intrusive::constant_time_size<true>>;
    using cache_iterator = typename cache_type::iterator;
    using const_cache_iterator = typename cache_type::const_iterator;
    using lru_list_type = bi::list<cache_entry,
        bi::member_hook<cache_entry, typename cache_entry::list_link_type, &cache_entry::_lru_link>,
        bi::constant_time_size<false>>;
    using dirty_list_type = bi::list<cache_entry,
        bi::member_hook<cache_entry, typename cache_entry::list_link_type, &cache_entry::_dirty_link>,
        bi::constant_time_size<false>>;
    static constexpr size_t initial_bucket_count = DEFAULT_INITIAL_SIZE;
    static constexpr float load_factor = 0.75f;
    size_t _resize_up_threshold = load_factor * initial_bucket_count;
    cache_type::bucket_type* _buckets;
    cache_type _store;
    seastar::timer_set<cache_entry, &cache_entry::_timer_link> _alive;
    timer<clock_type> _timer;
    clock_type::duration _wc_to_clock_type_delta;
    allocation_strategy* alloc;
    using expired_entry_releaser_type = std::function<void(cache_entry& e)>;
    expired_entry_releaser_type _expired_entry_releaser;

    // Evict cache_entry from lru, make the cache not to be too large.
    // Of cause, flush it before evicted from the lru list.
    lru_list_type _lru;

    // Every modified cache_entry is dirty before flushed to memory table.
    dirty_list_type _dirty;
public:
    cache ()
        : _buckets(new cache_type::bucket_type[initial_bucket_count])
        , _store(cache_type::bucket_traits(_buckets, initial_bucket_count))
        , _lru()
        , _dirty()
    {
        _timer.set_callback([this] { erase_expired_entries(); });
    }
    ~cache ()
    {
    }

    bool should_flush_dirty_entry() const { return false; }

    future<> flush_dirty_entry(store::column_family& cf);

    inline size_t expiring_size() const
    {
        return _alive.size();
    }


    void set_expired_entry_releaser(expired_entry_releaser_type&& releaser)
    {
        _alive.clear();
        _expired_entry_releaser = std::move(releaser);
    }

    void flush_all()
    {
        for (auto it = _store.begin(); it != _store.end(); ++it) {
            if (it->ever_expires()) {
                _alive.remove(*it);
            }
        }
        _store.erase_and_dispose(_store.begin(), _store.end(), current_deleter<cache_entry>());
    }

    inline bool erase(const redis_key& key)
    {
        static auto hash_fn = [] (const redis_key& k) -> size_t { return k.hash(); };
        auto it = _store.find(key, hash_fn, cache_entry::compare());
        if (it != _store.end()) {
            if (it->ever_expires()) {
                _alive.remove(*it);
            }
            _store.erase_and_dispose(it, current_deleter<cache_entry>());
            return true;
        }
        return false;
    }

    inline bool erase(cache_entry& e)
    {
        auto it = _store.iterator_to(e);
        _store.erase_and_dispose(it, current_deleter<cache_entry>());
        return true;
    }

    inline bool replace(cache_entry* entry)
    {
        bool res = true;
        if (entry) {
            static auto hash_fn = [] (const cache_entry& e) -> size_t { return e.key_hash(); };
            auto it = _store.find(*entry, hash_fn, cache_entry::compare());
            if (it != _store.end()) {
                if (it->ever_expires()) {
                    _alive.remove(*it);
                }
                _store.erase_and_dispose(it, current_deleter<cache_entry>());
                res = false;
            }
        }
        insert(entry);
        return res;
    }

    inline bool replace(cache_entry* entry, long expired)
    {
        bool res = true;
        if (entry) {
            static auto hash_fn = [] (const cache_entry& e) -> size_t { return e.key_hash(); };
            auto it = _store.find(*entry, hash_fn, cache_entry::compare());
            if (it != _store.end()) {
                if (it->ever_expires()) {
                    _alive.remove(*it);
                }
                _store.erase_and_dispose(it, current_deleter<cache_entry>());
                res = false;
            }
        }
        insert(entry);
        return res;
    }

    // return value: true if the entry was inserted, otherwise false.
    inline bool insert_if(cache_entry* entry, long expired, bool nx, bool xx)
    {
        if (!entry) {
            return false;
        }
        static auto hash_fn = [] (const cache_entry& e) -> size_t {
            return e.key_hash();
        };
        auto it = _store.find(*entry, hash_fn, cache_entry::compare());
        if (it != _store.end() && (xx || (!xx && !nx))) {
            if (it->ever_expires()) {
                _alive.remove(*it);
            }
            _store.erase_and_dispose(it, current_deleter<cache_entry>());
        }
        bool should_insert = (xx && it != _store.end()) || (nx && it == _store.end()) || (!nx && !xx);
        if (should_insert) {
            if (expired > 0) {
                auto expiry = expiration(expired);
                entry->set_expiry(expiry);
                if (_alive.insert(*entry)) {
                    _timer.rearm(entry->get_timeout());
                }
            }
            _store.insert(*entry);
            maybe_rehash();
            return true;
        }
        return false;
    }

    inline void insert(cache_entry* entry)
    {
        auto& etnry_reference = *entry;
        _store.insert(etnry_reference);
        // maybe cache will be rehashed.
        maybe_rehash();
    }

    cache_entry* find(const redis_key& rk)
    {
        static auto hash_fn = [] (const redis_key& k) -> size_t { return k.hash(); };
        auto it = _store.find(rk, hash_fn, cache_entry::compare());
        if (it != _store.end()) {
            return &(*it);
        }
        return nullptr;
    }

    template <typename Func>
    inline std::result_of_t<Func(const cache_entry* e)> run_with_entry(const redis_key& rk, Func&& func) const {
        static auto hash_fn = [] (const redis_key& k) -> size_t { return k.hash(); };
        auto it = _store.find(rk, hash_fn, cache_entry::compare());
        if (it != _store.end()) {
            const auto& e = *it;
            return func(&e);
        }
        else {
            return func(nullptr);
        }
    }

    template <typename Func>
    inline std::result_of_t<Func(cache_entry* e)> run_with_entry(const redis_key& rk, Func&& func) {
        static auto hash_fn = [] (const redis_key& k) -> size_t { return k.hash(); };
        auto it = _store.find(rk, hash_fn, cache_entry::compare());
        if (it != _store.end()) {
            auto& e = *it;
            return func(&e);
        }
        else {
            return func(nullptr);
        }
    }


    inline bool exists(const redis_key& rk)
    {
        static auto hash_fn = [] (const redis_key& k) -> size_t { return k.hash(); };
        auto it = _store.find(rk, hash_fn, cache_entry::compare());
        return it != _store.end();
    }

    void maybe_rehash()
    {
        if (_store.size() >= _resize_up_threshold) {
            auto new_size = _store.bucket_count() * 2;
            auto old_buckets = _buckets;
            try {
                _buckets = new cache_type::bucket_type[new_size];
            } catch (const std::bad_alloc& e) {
                return;
            }
            _store.rehash(typename cache_type::bucket_traits(_buckets, new_size));
            delete[] old_buckets;
            _resize_up_threshold = _store.bucket_count() * load_factor;
        }
    }

    inline size_t size() const
    {
        return _store.size();
    }

    inline bool empty() const
    {
        return _store.empty();
    }

    bool expire(const redis_key& rk, long expired)
    {
        bool result = false;
        static auto hash_fn = [] (const redis_key& k) -> size_t { return k.hash(); };
        auto it = _store.find(rk, hash_fn, cache_entry::compare());
        if (it != _store.end()) {
            auto expiry = expiration(expired);
            it->set_expiry(expiry);
            auto& ref = *it;
            if (_alive.insert(ref)) {
                _timer.rearm(it->get_timeout());
                result = true;
            }
        }
        return result;
    }

    void erase_expired_entries()
    {
        assert(_expired_entry_releaser);

        auto expired_entries = _alive.expire(clock_type::now());
        while (!expired_entries.empty()) {
            auto entry = &*expired_entries.begin();
            expired_entries.pop_front();
            _expired_entry_releaser(*entry);
        }
        _timer.arm(_alive.get_next_timeout());
    }

    bool never_expired(const redis_key& rk)
    {
        bool result = false;
        static auto hash_fn = [] (const redis_key& k) -> size_t { return k.hash(); };
        auto it = _store.find(rk, hash_fn, cache_entry::compare());
        if (it != _store.end() && it->ever_expires()) {
            it->set_never_expired();
            auto& ref = *it;
            _alive.remove(ref);
            result = true;
        }
        return result;
    }
};
}
