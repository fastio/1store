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
 *
 *  Copyright (c) 2016-2026, Peng Jian, pstack@163.com. All rights reserved.
 *
 */
#pragma once
#include "storage.hh"
#include "base.hh"
#include "dict.hh"
namespace redis {
using item_ptr = foreign_ptr<lw_shared_ptr<item>>;
class dict_storage : public storage {
public:
    dict_storage(const sstring& name, dict* store) : storage(name, store)
    {
    }
    virtual ~dict_storage()
    {
    }
    struct stats {
        uint64_t dict_count_ = 0;
        uint64_t dict_node_count_ = 0;
    };
    // HSET
    template<typename origin = local_origin_tag>
    int hset(const sstring& key, sstring& field, sstring& value)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d == nullptr) {
            d = new dict();
            auto dict_item = item::create(key, d, REDIS_DICT);
            if (_store->set(rk, dict_item) != 0) {
                return -1;
            }
        }
        auto field_hash = std::hash<sstring>()(field);
        redis_key field_key{std::ref(field), field_hash};
        auto new_item = item::create(origin::move_if_local(field_key), origin::move_if_local(value));
        return d->replace(field_key, new_item);
    }

    // HMSET
    template<typename origin = local_origin_tag>
    int hmset(const sstring& key, std::unordered_map<sstring, sstring>& kv)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d == nullptr) {
            d = new dict();
            auto dict_item = item::create(key, d, REDIS_DICT);
            if (_store->set(rk, dict_item) != 0) {
                return -1;
            }
        }
        for (auto p = kv.begin(); p != kv.end(); p++) {
            sstring field = p->first;
            auto hash = std::hash<sstring>()(field);
            sstring& value = p->second;
            redis_key field_key{std::ref(field), std::move(hash)};
            auto new_item = item::create(origin::move_if_local(field_key), origin::move_if_local(value));
            if (d->replace(field_key, new_item) != -1) {
                return -1;
            }
        }
        return 0;
    }

    // HGET
    item_ptr hget(const sstring& key, sstring& field)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            auto hash = std::hash<sstring>()(field);
            redis_key field_key{std::ref(field), hash};
            return d->fetch(field_key);
        }
        return nullptr;
    }

    // HDEL
    int hdel(const sstring& key, sstring& field)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            auto hash = std::hash<sstring>()(field);
            redis_key field_key{std::ref(field), hash};
            auto result = d->remove(field_key);
            if (d->size() == 0) {
                _store->remove(rk);
            }
            return result;
        }
        return 0;
    }

    // HEXISTS
    int hexists(const sstring& key, sstring& field)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            auto hash = std::hash<sstring>()(field);
            redis_key field_key{std::ref(field), hash};
            return d->exists(field_key);
        }
        return REDIS_ERR;
    }

    // HSTRLEN
    int hstrlen(const sstring& key, sstring& field)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            auto hash = std::hash<sstring>()(field);
            redis_key field_key{std::ref(field), hash};
            auto it = d->fetch(field_key);
            if (it && it->type() == REDIS_RAW_STRING) {
                return static_cast<int>(it->value_size());
            }
        }
        return 0;
    }

    // HLEN
    int hlen(const sstring& key)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            return d->size();
        }
        return 0;
    }

    // HINCRBY
    template<typename origin = local_origin_tag>
    std::pair<int64_t, int> hincrby(const sstring& key, sstring& field, int64_t delta)
    {
        using result_type = std::pair<int64_t, int>;
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d == nullptr) {
            d = new dict();
            auto dict_item = item::create(key, d, REDIS_DICT);
            if (_store->set(rk, dict_item) != REDIS_OK) {
                return result_type {0, REDIS_ERR};
            }
        }
        auto hash = std::hash<sstring>()(field);
        redis_key field_key{std::ref(field), hash};
        auto it = d->fetch(field_key);
        auto new_value = delta;
        if (!it) {
            if (it->type() == REDIS_RAW_INT64) {
                return result_type {0, REDIS_WRONG_TYPE};
            }
            new_value += it->int64(); 
        }
        auto new_item = item::create(origin::move_if_local(field_key), static_cast<int64_t>(new_value));
        if (d->replace(field_key, new_item) == REDIS_OK) {
            return result_type {new_value, REDIS_OK};
        }
        return result_type {0, REDIS_ERR}; 
    }

    // HINCRBYFLOAT
    template<typename origin = local_origin_tag>
    std::pair<double, int> hincrbyfloat(const sstring& key, sstring& field, double delta)
    {
        using result_type = std::pair<double, int>;
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d == nullptr) {
            d = new dict();
            auto dict_item = item::create(key, d, REDIS_DICT);
            if (_store->set(rk, dict_item) != REDIS_OK) {
                return result_type {0, REDIS_ERR};
            }
        }
        auto hash = std::hash<sstring>()(field);
        redis_key field_key{std::ref(field), hash};
        auto it = d->fetch(field_key);
        auto new_value = delta;
        if (!it) {
            if (it->type() != REDIS_RAW_DOUBLE) {
                return result_type {0, REDIS_WRONG_TYPE};
            }
            new_value += it->Double();
        }
        auto new_item = item::create(origin::move_if_local(field_key), new_value);
        if (d->replace(field_key, new_item) == REDIS_OK) {
            return result_type {new_value, REDIS_OK};
        }
        return result_type {0, REDIS_ERR}; 
    }

    // HGETALL
    std::vector<item_ptr> hgetall(const sstring& key)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            return d->fetch();
        }
        std::vector<item_ptr> empty;
        return std::move(empty);
    }

    // HMGET
    std::vector<item_ptr> hmget(const sstring& key, std::unordered_set<sstring>& keys)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            return d->fetch(keys);
        }
        std::vector<item_ptr> empty {};
        return std::move(empty);
    }

protected:
    stats _stats;
    inline dict* fetch_dict(const redis_key& key)
    {
        auto it = _store->fetch_raw(key);
        if (it && it->type() == REDIS_DICT) {
            return it->dict_ptr();
        }
        return nullptr;
    }
};
}
