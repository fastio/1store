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
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include "common.hh"
#include "utils/bytes.hh"
#include "utils/managed_ref.hh"
#include "utils/managed_bytes.hh"
#include "utils/allocation_strategy.hh"
#include "utils/logalloc.hh"
#include "list_lsa.hh"
#include "core/sstring.hh"
namespace redis {
namespace bi = boost::intrusive;
struct cache_entry_key
{
    managed_bytes _key;
    size_t  _hash;
    cache_entry_key(const sstring& key) noexcept : _key(), _hash(std::hash<managed_bytes>()(_key))
    {
        auto entry = current_allocator().construct<managed_bytes>(bytes_view{reinterpret_cast<const signed char*>(key.data()), key.size()});
        _key = std::move(*entry);
    }
    cache_entry_key(const sstring& key, size_t hash) noexcept : _key(), _hash(hash)
    {
        auto entry = current_allocator().construct<managed_bytes>(bytes_view{reinterpret_cast<const signed char*>(key.data()), key.size()});
        _key = std::move(*entry);
    }
    cache_entry_key(const cache_entry_key& o) noexcept : _key(o._key), _hash(o._hash)
    {
    }
    cache_entry_key(cache_entry_key&& o) noexcept : _key(std::move(o._key)), _hash(std::move(o._hash))
    {
    }
};

class cache;
// cache_entry should be allocated by LSA.
class cache_entry
{
protected:
    friend class cache;
    using time_point = expiration::time_point;
    using duration = expiration::duration;
    using hook_type = boost::intrusive::unordered_set_member_hook<>;
    hook_type _cache_link;
    enum class entry_type {
        ENTRY_FLOAT = 0,
        ENTRY_INT64 = 1,
        ENTRY_BYTES = 2,
        ENTRY_LIST  = 3,
    };
    entry_type _type;
    cache_entry_key _key;
    union storage {
        double _float_number;
        int64_t _integer_number;
        managed_bytes _bytes;
        managed_ref<list_lsa> _list;
        storage() {}
        ~storage() {}
    } _storage;
    inline const cache_entry_key& key() const { return _key; }
public:
    cache_entry(const sstring& key, size_t hash, entry_type type) noexcept
        : _cache_link()
        , _type(type)
        , _key(key, hash)
    {
    }
    cache_entry(const sstring& key, size_t hash, double data) noexcept
        : cache_entry(key, hash, entry_type::ENTRY_FLOAT)
    {
        _storage._float_number = data;
    }
    cache_entry(const std::string key, size_t hash, int64_t data) noexcept
        : cache_entry(key, hash, entry_type::ENTRY_INT64)
    {
        _storage._integer_number = data;
    }
    cache_entry(const sstring& key, size_t hash, const sstring& data) noexcept
        : cache_entry(key, hash, entry_type::ENTRY_BYTES)
    {
        auto entry = current_allocator().construct<managed_bytes>(bytes_view{reinterpret_cast<const signed char*>(data.data()), data.size()});
        _storage._bytes = std::move(*entry);
    }
    struct list_initializer {};
    cache_entry(const sstring& key, size_t hash, list_initializer) noexcept
        : cache_entry(key, hash, entry_type::ENTRY_LIST)
    {
        _storage._list = make_managed<list_lsa>();
    }
    cache_entry(cache_entry&& o) noexcept
        : _cache_link(std::move(o._cache_link))
        , _type(o._type)
        , _key(std::move(_key))
    {
        switch (_type) {
            case entry_type::ENTRY_FLOAT:
                _storage._float_number = std::move(o._storage._float_number);
                break;
            case entry_type::ENTRY_INT64:
                _storage._integer_number = std::move(o._storage._integer_number);
                break;
            case entry_type::ENTRY_BYTES:
                _storage._bytes = std::move(o._storage._bytes);
                break;
            case entry_type::ENTRY_LIST:
                _storage._list = std::move(o._storage._list);
                break;
        }
    }
    virtual ~cache_entry()
    {
    }

    friend inline bool operator == (const cache_entry &l, const cache_entry &r) {
        const auto& lk = l.key();
        const auto& rk = r.key();
        return (lk._hash == rk._hash) && (lk._key == rk._key);
    }

    friend inline std::size_t hash_value(const cache_entry& e) {
        return e.key()._hash;
    }

    struct compare {
    private:
        inline bool compare_key (const cache_entry_key& l, const cache_entry_key& r) const {
            return (l._hash == r._hash) && (l._key == r._key);
        }

    public:
        bool operator () (const cache_entry& l, const cache_entry& r) const {
            const auto& lk = l._key;
            const auto& rk = r._key;
            return compare_key(lk, rk);
        }
        bool operator () (const cache_entry_key& k, const cache_entry& e) const {
            const auto& ek = e._key;
            return compare_key(k, ek);
        }
        bool operator () (const cache_entry& e, const cache_entry_key& k) const {
            const auto& ek = e._key;
            return compare_key(k, ek);
        }
    };
};

static constexpr const size_t DEFAULT_INITIAL_SIZE = 1 << 20;

class cache {
    using cache_type = boost::intrusive::unordered_set<cache_entry,
        boost::intrusive::member_hook<cache_entry, cache_entry::hook_type, &cache_entry::_cache_link>,
        boost::intrusive::power_2_buckets<true>,
        boost::intrusive::constant_time_size<true>>;
    using cache_iterator = typename cache_type::iterator;
    using const_cache_iterator = typename cache_type::const_iterator;
    static constexpr size_t initial_bucket_count = DEFAULT_INITIAL_SIZE;
    static constexpr float load_factor = 0.75f;
    size_t _resize_up_threshold = load_factor * initial_bucket_count;
    cache_type::bucket_type* _buckets;
    cache_type _store;
public:
    cache ()
        : _buckets(new cache_type::bucket_type[initial_bucket_count])
        , _store(cache_type::bucket_traits(_buckets, initial_bucket_count))
    {
    }
    ~cache ()
    {
        flush_all();
    }
    
    void flush_all()
    {
        _store.erase_and_dispose(_store.begin(), _store.end(), [this] (const cache_entry* it) {
            //release memory
        });
    }

    inline void erase(const cache_entry_key& key)
    {
        static auto hash_fn = [] (const cache_entry_key& k) -> size_t { return k._hash; };
        auto it = _store.find(key, hash_fn, cache_entry::compare());
        if (it != _store.end()) {
            _store.erase(it);
        } 
    }

    inline cache_iterator find(const cache_entry_key& key)
    {
        static auto hash_fn = [] (const cache_entry_key& k) -> size_t { return k._hash; };
        return _store.find(key, hash_fn, cache_entry::compare());
    }

    inline void replace(cache_entry* entry)
    {
        erase(entry->key());
        insert(entry);
    }

    inline void insert(cache_entry* entry)
    {
        auto& etnry_reference = *entry;
        _store.insert(etnry_reference);
        // maybe cache will be rehashed.
        maybe_rehash();
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
};
}
