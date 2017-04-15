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
#include <boost/intrusive/set.hpp>
#include "utils/allocation_strategy.hh"
#include "utils/managed_ref.hh"
#include "utils/managed_bytes.hh"
#include "utils/bytes.hh"
#include "utils/allocation_strategy.hh"
#include "utils/logalloc.hh"
#include "common.hh"
#include "core/sstring.hh"
namespace redis {

class dict_lsa;
struct dict_entry
{
    friend class dict_lsa;
    using hook_type = boost::intrusive::set_member_hook<>;
    hook_type _link;
    managed_bytes _key;
    size_t _key_hash;
    union storage {
        managed_bytes _data;
        double _float;
        int64_t _integer;
        storage() {}
        ~storage() {}
    } _u;
    enum class entry_type
    {
        BYTES   = 0,
        FLOAT   = 1,
        INTEGER = 2,
    };
    entry_type _type;

    dict_entry(const sstring& key, const sstring& val) noexcept
        : _link()
        , _key(std::move(*(current_allocator().construct<managed_bytes>(bytes_view {reinterpret_cast<const signed char*>(key.data()), key.size()}))))
        , _key_hash(std::hash<managed_bytes>()(_key))
        , _type(entry_type::BYTES)
    {
        _u._data = std::move(*(current_allocator().construct<managed_bytes>(bytes_view {reinterpret_cast<const signed char*>(val.data()), val.size()})));
    }

    dict_entry(const sstring& key) noexcept
        : _link()
        , _key(std::move(*(current_allocator().construct<managed_bytes>(bytes_view {reinterpret_cast<const signed char*>(key.data()), key.size()}))))
        , _key_hash(std::hash<managed_bytes>()(_key))
        , _type(entry_type::BYTES)
    {
    }

    dict_entry(const sstring& key, double data) noexcept
        : _link()
        , _key(std::move(*(current_allocator().construct<managed_bytes>(bytes_view {reinterpret_cast<const signed char*>(key.data()), key.size()}))))
        , _key_hash(std::hash<managed_bytes>()(_key))
        , _type(entry_type::FLOAT)
    {
        _u._float = data;
    }

    dict_entry(const sstring& key, int64_t data) noexcept
        : _link()
        , _key(std::move(*(current_allocator().construct<managed_bytes>(bytes_view {reinterpret_cast<const signed char*>(key.data()), key.size()}))))
        , _key_hash(std::hash<managed_bytes>()(_key))
        , _type(entry_type::INTEGER)
    {
        _u._integer = data;
    }

    dict_entry(dict_entry&& o) noexcept
        : _link(std::move(o._link))
        , _key(std::move(o._key))
        , _key_hash(std::move(o._key_hash))
        , _type(std::move(o._type))
    {
        switch (_type) {
            case entry_type::BYTES:
                 _u._data = std::move(o._u._data);
                 break;
            case entry_type::FLOAT:
                 _u._float = o._u._float;
                 break;
            case entry_type::INTEGER:
                 _u._integer = o._u._integer;
                 break;
        }
    }

    struct compare {
        inline bool compare_impl(const char* d1, size_t s1, const char* d2, size_t s2) const noexcept {
            const int len = std::min(s1, s2);
            auto r = memcmp(d1, d2, len);
            if (r == 0) {
                if (s1 < s2) return true;
                else if (s1 > s2) return false;
            }
            return r < 0;
        }
        inline bool operator () (const dict_entry& l, const dict_entry& r) const noexcept {
            return compare_impl(l.key_data(), l.key_size(), r.key_data(), r.key_size());
        }
        inline bool operator () (const sstring& k, const dict_entry& e) const noexcept {
            return compare_impl(k.data(), k.size(), e.key_data(), e.key_size());
        }
        inline bool operator () (const dict_entry& e, const sstring& k) const noexcept {
            return compare_impl(e.key_data(), e.key_size(), k.data(), k.size());
        }
    };

    bool type_of_bytes() const {
        return _type == entry_type::BYTES;
    }

    bool type_of_integer() const {
        return _type == entry_type::INTEGER;
    }

    bool type_of_float() const {
        return _type == entry_type::FLOAT;
    }

    const managed_bytes& key() const {
        return _key;
    }
    const managed_bytes& value() const {
        return _u._data;
    }
    managed_bytes& value() {
        return _u._data;
    }
    inline const char* key_data() const
    {
        return reinterpret_cast<const char*>(_key.data());
    }
    inline size_t key_size() const
    {
        return _key.size();
    }
    inline size_t value_bytes_size() const
    {
        return _u._data.size();
    }
    inline const char* value_bytes_data() const
    {
        return reinterpret_cast<const char*>(_u._data.data());
    }
    inline const double value_float() const {
        return _u._float;
    }
    inline double value_float_incr(double delta) {
        return _u._float += delta;
    }
    inline const int64_t value_integer() const {
        return _u._integer;
    }
    inline int64_t value_integer_incr(int64_t delta) {
        return _u._integer += delta;
    }
};

class dict_lsa final {
    using dict_type = boost::intrusive::set<dict_entry,
        boost::intrusive::member_hook<dict_entry, dict_entry::hook_type, &dict_entry::_link>,
        boost::intrusive::compare<dict_entry::compare>>;
    using dict_iterator = typename dict_type::iterator;
    using const_dict_iterator = typename dict_type::const_iterator;
    dict_type _dict;
public:
    dict_lsa () noexcept : _dict()
    {
    }

    dict_lsa (dict_lsa&& o) noexcept : _dict(std::move(o._dict))
    {
    }

    ~dict_lsa ()
    {
        flush_all();
    }

    void flush_all()
    {
        _dict.erase_and_dispose(_dict.begin(), _dict.end(), current_deleter<dict_entry>());
    }

    bool insert(dict_entry* e)
    {
        assert(e != nullptr);
        auto r = _dict.insert(*e);
        return r.second;
    }

    template <typename Func>
    inline std::result_of_t<Func(const dict_entry* e)> with_entry_run(const sstring& k, Func&& func) const {
        auto it = _dict.find(k, dict_entry::compare());
        if (it != _dict.end()) {
            const auto& e = *it;
            return func(&e);
        }
        else {
            return func(nullptr);
        }
    }

    template <typename Func>
    inline std::result_of_t<Func(dict_entry* e)> with_entry_run(const sstring& k, Func&& func) {
        auto it = _dict.find(k, dict_entry::compare());
        if (it != _dict.end()) {
            auto& e = *it;
            return func(&e);
        }
        else {
            return func(nullptr);
        }
    }

    inline bool erase(const sstring& key)
    {
        auto it = _dict.find(key, dict_entry::compare());
        if (it != _dict.end()) {
            _dict.erase_and_dispose(it, current_deleter<dict_entry>());
            return true;
        }
        return false;
    }

    inline bool empty() const {
        return _dict.empty();
    }

    inline size_t size() const {
        return _dict.size();
    }

    inline void clear() {
        flush_all();
    }

    inline bool exists(const sstring& key) const
    {
        return _dict.find(key, dict_entry::compare()) != _dict.end();
    }

    inline const dict_entry* begin() const
    {
        if (_dict.begin() != _dict.end()) {
            const auto& e = *(_dict.begin());
            return &e;
        }
        return nullptr;
    }
    void fetch(const std::vector<sstring>& keys, std::vector<const dict_entry*>& entries) const {
        for (const auto& key : keys) {
            auto it = _dict.find(key, dict_entry::compare());
            if (it != _dict.end()) {
                const auto& e = *it;
                entries.push_back(&e);
            }
            else {
                entries.push_back(nullptr);
            }
        }
    }

    void fetch(std::vector<const dict_entry*>& entries) const {
        for (auto it = _dict.begin(); it != _dict.end(); ++it) {
            const auto& e = *it;
            entries.push_back(&e);
        }
    }
};
}
