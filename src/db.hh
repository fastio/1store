/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef _DB_HH
#define _DB_HH
#include "core/shared_ptr.hh"
#include "core/future.hh"
#include <sstream>
#include <iostream>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include "core/shared_ptr.hh"
#include "core/sharded.hh"
#include "base.hh"
#include "dict.hh"
#include "list.hh"
namespace redis {

namespace stdx = std::experimental;
using item_ptr = foreign_ptr<boost::intrusive_ptr<item>>;

struct remote_origin_tag {
    template <typename T>
    static inline
    T move_if_local(T& ref) {
        return ref;
    }
};

struct local_origin_tag {
    template <typename T>
    static inline
    T move_if_local(T& ref) {
        return std::move(ref);
    }
};


class db {
private:
    dict* _store;
    seastar::timer_set<item, &item::_timer_link> _alive;
    timer<clock_type> _timer;
    clock_type::duration _wc_to_clock_type_delta;
public:
    db();
    ~db();

    /** COUNTER API **/
    template <typename origin = local_origin_tag>
    uint64_t counter_by(sstring& key, size_t key_hash, uint64_t step, bool incr)
    {
        auto it = _store->fetch_raw(key, key_hash);
        if (it != nullptr) {
            if (it->type() != REDIS_RAW_UINT64) {
                return REDIS_ERR;
            }
            return incr ? it->incr(step) : it->decr(step);
        } else {
            const size_t item_size = item::item_size_for_uint64();
            auto new_item = local_slab().create(item_size, step);
            intrusive_ptr_add_ref(new_item);
            if (_store->set(key, key_hash, new_item) != REDIS_OK)
                return REDIS_ERR;
            return step;
        }
    }

    /** STRING API **/
    template <typename origin = local_origin_tag>
    int set(sstring& key, size_t key_hash, sstring& val, long expire, uint32_t flag)
    {
        _store->remove(key, key_hash);
        const size_t item_size = item::item_size_for_row_string(val);
        auto new_item = local_slab().create(item_size, origin::move_if_local(val), expire);
        intrusive_ptr_add_ref(new_item);
        return _store->set(key, key_hash, new_item);
    }

    template <typename origin = local_origin_tag>
    int append(sstring& key, size_t key_hash, sstring& val)
    {
        size_t current_size = -1;
        auto it = _store->fetch_raw(key, key_hash);
        if (it != nullptr) {
            auto exist_val = it->value();
            current_size = exist_val.size() + val.size();
            const size_t item_size = item::item_size_for_row_string_append(val, exist_val);
            auto new_item = local_slab().create(item_size,
                    origin::move_if_local(exist_val),
                    origin::move_if_local(val), 0);
            intrusive_ptr_add_ref(new_item);
            intrusive_ptr_release(it);
            if (_store->replace(key, key_hash, new_item) != 0)
                return -1;
        }
        else {
            current_size = val.size();
            const size_t item_size = item::item_size_for_row_string(val);
            auto new_item = local_slab().create(item_size, origin::move_if_local(val), 0);
            intrusive_ptr_add_ref(new_item);
            if (_store->set(key, key_hash, new_item) != 0)
                return -1;
        }
        return current_size;
    }

    template <typename origin = local_origin_tag>
    int del(const sstring& key, size_t key_hash)
    {
        return _store->remove(key, key_hash) == REDIS_OK ? 1 : 0;
    }

    template <typename origin = local_origin_tag>
    int exists(const sstring& key, size_t key_hash)
    {
        return _store->exists(key, key_hash);
    }

    template <typename origin = local_origin_tag>
    item_ptr get(const sstring& key, size_t key_hash)
    {
        return _store->fetch(key, key_hash);
    }

    template <typename origin = local_origin_tag>
    int strlen(const sstring& key, size_t key_hash)
    {
        auto i = _store->fetch(key, key_hash);
        if (i) {
            return i->value_size();
        }
        return 0;
    }

    template<typename origin = local_origin_tag>
    int expire(const sstring& key, size_t key_hash, long expired)
    {
      auto it = _store->fetch_raw(key, key_hash);
      if (it == nullptr) {
        return REDIS_ERR;
      }
      auto exp = expiration(_cache.get_wc_to_clock_type_delta(), expired);
      it->update_expired_point(exp);
      return REDIS_OK;
    }

    /** LIST API **/
    inline list* fetch_list(const sstring& key, size_t key_hash)
    {
        auto it = _store->fetch_raw(key, key_hash);
        if (it != nullptr && it->type() == REDIS_LIST) {
            return static_cast<list*>(it->ptr());
        }
        return nullptr;
    }
    template<typename origin = local_origin_tag>
    int push(const sstring& key, size_t key_hash, sstring& value, bool force, bool left)
    {
        list* l = fetch_list(key, key_hash);
        if (l == nullptr) {
            if (!force) {
                return -1;
            }
            const size_t list_size = item::item_size_for_list();
            l = new list();
            auto list_item = local_slab().create(list_size, l, REDIS_LIST, 0);
            intrusive_ptr_add_ref(list_item);
            if (_store->set(key, key_hash, list_item) != 0) {
                return -1;
            }
        }
        const size_t item_size = item::item_size_for_row_string(value);
        auto new_item = local_slab().create(item_size, origin::move_if_local(value), 0);
        intrusive_ptr_add_ref(new_item);
        return (left ? l->add_head(new_item) : l->add_tail(new_item)) == 0 ? 1 : 0;
    }

    template<typename origin = local_origin_tag>
    item_ptr pop(const sstring& key, size_t key_hash, bool left)
    {
        list* l = fetch_list(key, key_hash);
        if (l != nullptr) {
            auto it = left ? l->pop_head() : l->pop_tail();
            if (l->length() == 0) {
                _store->remove(key, key_hash);
            }
            return it;
        } 
        return nullptr;
    }

    template<typename origin = local_origin_tag>
    int llen(const sstring& key, size_t key_hash)
    {
        list* l = fetch_list(key, key_hash);
        if (l != nullptr) {
            return l->length();
        } 
        return 0;
    }

    template<typename origin = local_origin_tag>
    item_ptr lindex(const sstring& key, size_t key_hash, int idx)
    {
        list* l = fetch_list(key, key_hash);
        if (l != nullptr) {
            return l->index(idx);
        } 
        return nullptr;
    }
  
    template<typename origin = local_origin_tag>
    int linsert(const sstring& key, size_t key_hash, sstring& pivot, sstring& value, bool after)
    {
        list* l = fetch_list(key, key_hash);
        if (l != nullptr) {
            const size_t item_size = item::item_size_for_row_string(value);
            auto new_item = local_slab().create(item_size, origin::move_if_local(value), 0);
            intrusive_ptr_add_ref(new_item);
            return (after ? l->insert_after(pivot, new_item) : l->insert_before(pivot, new_item)) == 0 ? 1 : 0;
        } 
        return 0;
    }

    template<typename origin = local_origin_tag>
    std::vector<item_ptr> lrange(const sstring& key, size_t key_hash, int start, int end)
    {
        list* l = fetch_list(key, key_hash);
        std::vector<item_ptr> result;
        if (l != nullptr) {
            return l->range(start, end);
        } 
        return std::move(result); 
    }
    template<typename origin = local_origin_tag>
    int lset(const sstring& key, size_t key_hash, int idx, sstring& value)
    {
        list* l = fetch_list(key, key_hash);
        if (l != nullptr) {
            const size_t item_size = item::item_size_for_row_string(value);
            auto new_item = local_slab().create(item_size, origin::move_if_local(value), 0);
            intrusive_ptr_add_ref(new_item);
            if (l->set(idx, new_item) == REDIS_OK)
                return 1;
            else {
                intrusive_ptr_release(new_item);
            }
        } 
        return 0;
    }

    template<typename origin = local_origin_tag>
    int lrem(const sstring& key, size_t key_hash, int count, sstring& value)
    {
        list* l = fetch_list(key, key_hash);
        if (l != nullptr) {
            return l->trem(count, value);
        }
        return 0;
    }

    template<typename origin = local_origin_tag>
    int ltrim(const sstring& key, size_t key_hash, int start, int end)
    {
        list* l = fetch_list(key, key_hash);
        if (l != nullptr) {
            return l->trim(start, end);
        }
        return 0;
    }

    future<> stop() { return make_ready_future<>(); }
};
}
#endif

