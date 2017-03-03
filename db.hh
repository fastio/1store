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
#include "sorted_set.hh"
#include "redis_timer_set.hh"
namespace redis {

namespace stdx = std::experimental;
using item_ptr = foreign_ptr<lw_shared_ptr<item>>;

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

class database {
public:
    database();
    ~database();

    template  <typename origin = local_origin_tag> inline
    int set(redis_key&& rk, sstring&& val, long expire, uint32_t flag)
    {
        _store->remove(rk);
        auto new_item = item::create(rk, origin::move_if_local(val));
        return _store->set(rk, new_item);
    }

    template  <typename origin = local_origin_tag> inline
    std::pair<int64_t, int> counter_by(redis_key&& rk, int64_t step, bool incr)
    {
        using result_type = std::pair<int64_t, int>;
        auto it = _store->fetch_raw(rk);
        int64_t new_value = incr ? step : -step;
        if (it) {
            if (it->type() != REDIS_RAW_UINT64) {
                return result_type {0, REDIS_WRONG_TYPE};
            }
            new_value += it->int64();
        }
        auto new_item = item::create(rk, new_value);
        if (_store->replace(rk, new_item) == REDIS_OK) {
            return result_type {new_value, REDIS_OK};
        }
        else {
            return result_type {0, REDIS_ERR};
        }
    }

    template  <typename origin = local_origin_tag> inline
    std::pair<size_t, int> append(redis_key&& rk, sstring&& val)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        if (it && it->type() != REDIS_RAW_STRING) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        size_t current_size = -1;
        if (it) {
            auto exist_val = it->value();
            current_size = exist_val.size() + val.size();
            auto new_item = item::create(rk,
                    origin::move_if_local(exist_val),
                    origin::move_if_local(val));
            if (_store->replace(rk, new_item) != REDIS_OK) {
                return result_type {0, REDIS_ERR};
            }
        }
        else {
            current_size = val.size();
            auto new_item = item::create(rk, origin::move_if_local(val));
            if (_store->set(rk, new_item) != REDIS_OK) {
                return result_type {0, REDIS_ERR};
            }
        }
        return result_type {current_size, REDIS_OK};
    }

    int del(redis_key&& key);

    int exists(redis_key&& key);

    item_ptr get(redis_key&& key);

    int strlen(redis_key&& key);

    int expire(redis_key&& rk, long expired)
    {
        auto expiry = expiration(expired);
        auto it = _store->fetch_raw(rk);
        if (it) {
            it->set_expiry(expiry);
            if (_alive.insert(it)) {
                _timer.rearm(it->get_timeout());
                return REDIS_OK;
            }
            it->set_never_expired();
        }
        return REDIS_ERR;
    }

    inline int persist(redis_key&& rk)
    {
        auto it = _store->fetch_raw(rk);
        if (it) {
            it->set_never_expired();
            return REDIS_OK;
        }
        return REDIS_ERR;
    }


    template <typename origin = local_origin_tag> inline
    std::pair<size_t, int> push(redis_key&& rk, sstring&& value, bool force, bool left)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        if (it && it->type() != REDIS_LIST) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        list* _list = nullptr;
        if (!it) {
            if (!force) {
                return result_type {0, REDIS_ERR};
            }
            _list = new list();
            auto list_item = item::create(rk, _list, REDIS_LIST);
            if (_store->set(rk, list_item) != REDIS_OK) {
                return result_type {0, REDIS_ERR};
            }
        }
        else {
            _list = it->list_ptr();
        }
        auto new_item = item::create(origin::move_if_local(value));
        if ((left ? _list->add_head(new_item) : _list->add_tail(new_item)) != REDIS_OK) {
            return result_type {0, REDIS_ERR};
        }
        return result_type {_list->size(), REDIS_OK};
    }

    std::pair<item_ptr, int> pop(redis_key&& rk, bool left)
    {
        using result_type = std::pair<item_ptr, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {nullptr, REDIS_ERR};
        }
        else if(it->type() != REDIS_LIST) {
            return result_type {nullptr, REDIS_WRONG_TYPE};
        }
        list* _list = it->list_ptr();
        auto mit = left ? _list->pop_head() : _list->pop_tail();
        if (_list->size() == 0) {
            _store->remove(rk);
        }
        return result_type {std::move(mit), REDIS_OK};
    }

    std::pair<size_t, int> llen(redis_key&& rk) 
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {0, REDIS_ERR};
        }
        else if (it->type() != REDIS_LIST) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        list* _list = it->list_ptr();
        return result_type {_list->size(), REDIS_OK};
    }

    std::pair<item_ptr, int> lindex(redis_key&& rk, int idx)
    {
        using result_type = std::pair<item_ptr, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {nullptr, REDIS_ERR};
        }
        else if (it->type() != REDIS_LIST) {
            return result_type {nullptr, REDIS_WRONG_TYPE};
        }
        list* _list = it->list_ptr();
        return result_type {std::move(_list->index(idx)), REDIS_OK};
    }

    template <typename origin = local_origin_tag> inline
    int linsert(redis_key&& rk, sstring&& pivot, sstring&& value, bool after)
    {
        auto it = _store->fetch_raw(rk);
        if (it) {
            if (it->type() != REDIS_LIST) {
                return REDIS_WRONG_TYPE;
            }
            list* _list = it->list_ptr();
            auto new_item = item::create(origin::move_if_local(value));
            return (after ? _list->insert_after(pivot, new_item) : _list->insert_before(pivot, new_item));
        }
        return REDIS_ERR;
    }

    std::pair<std::vector<item_ptr>, int> lrange(redis_key&& rk, int start, int end)
    {
        using result_type = std::pair<std::vector<item_ptr>, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {std::move(std::vector<item_ptr>()), REDIS_ERR};
        }
        else if (it->type() != REDIS_LIST) {
            return result_type {std::move(std::vector<item_ptr>()), REDIS_WRONG_TYPE};
        }
        list* _list = it->list_ptr();
        return result_type {std::move(_list->range(start, end)), REDIS_OK};
    }

    template <typename origin = local_origin_tag> inline
    int lset(redis_key&& rk, int idx, sstring&& value)
    {
        auto it = _store->fetch_raw(rk);
        if (it && it->type() != REDIS_LIST) {
            return REDIS_WRONG_TYPE;
        }
        if (it) {
            list* _list = it->list_ptr();
            auto new_item = item::create(origin::move_if_local(value));
            if (_list->set(idx, new_item) == REDIS_OK) {
                return REDIS_OK;
            }
        } 
        return REDIS_ERR;
    }

    std::pair<size_t, int> lrem(redis_key&& rk, int count, sstring&& value)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {0, REDIS_ERR};
        }
        else if (it->type() != REDIS_LIST) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        list* _list = it->list_ptr();
        auto result = _list->trem(count, value);
        if (_list->size() == 0) {
            _store->remove(rk);
        }
        return result_type {result, REDIS_OK};
    }

    int ltrim(redis_key&& rk, int start, int end)
    {
        auto it = _store->fetch_raw(rk);
        if (it) {
            if (it->type() != REDIS_LIST) {
                return REDIS_WRONG_TYPE;
            }
            list* _list = it->list_ptr();
            return _list->trim(start, end);
        }
        return REDIS_OK;
    }


    template <typename origin = local_origin_tag> inline
    int hset(redis_key&& rk, sstring&& field, sstring&& value)
    {
        auto it = _store->fetch_raw(rk);
        if (it && it->type() != REDIS_DICT) {
            return REDIS_WRONG_TYPE;
        }
        dict* _dict = nullptr;
        if (!it) {
            _dict = new dict();
            auto dict_item = item::create(rk, _dict, REDIS_DICT);
            if (_store->set(rk, dict_item) != REDIS_OK) {
                return REDIS_ERR;
            }
        }
        else {
            _dict = it->dict_ptr();
        }
        redis_key field_key {std::move(field)};
        auto new_item = item::create(field_key, origin::move_if_local(value));
        return _dict->replace(field_key, new_item);
    }

    template <typename origin = local_origin_tag> inline
    int hmset(redis_key&& rk, std::unordered_map<sstring, sstring>&& kv)
    {
        auto it = _store->fetch_raw(rk);
        if (it && it->type() != REDIS_DICT) {
            return REDIS_WRONG_TYPE;
        }
        dict* _dict = nullptr;
        if (!it) {
            _dict = new dict();
            auto dict_item = item::create(rk, _dict, REDIS_DICT);
            if (_store->set(rk, dict_item) != REDIS_OK) {
                return REDIS_ERR;
            }
        }
        else {
            _dict = it->dict_ptr();
        }
        for (auto p = kv.begin(); p != kv.end(); p++) {
            sstring field = p->first;
            sstring& value = p->second;
            redis_key field_key {std::move(field)};
            auto new_item = item::create(field_key, origin::move_if_local(value));
            _dict->replace(field_key, new_item);
        }
        return REDIS_OK;
    }

    std::pair<item_ptr, int> hget(redis_key&& rk, sstring&& field)
    {
        using result_type = std::pair<item_ptr, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {nullptr, REDIS_ERR};
        }
        else if (it->type() != REDIS_DICT) {
            return result_type {nullptr, REDIS_WRONG_TYPE};
        }
        auto _dict = it->dict_ptr();
        redis_key field_key {std::move(field)};
        return result_type {_dict->fetch(field_key), REDIS_OK};
    }

    int hdel(redis_key&& rk, sstring&& field)
    {
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return REDIS_ERR;
        }
        else if (it->type() != REDIS_DICT) {
            return REDIS_WRONG_TYPE;
        }
        auto _dict = it->dict_ptr();
        redis_key field_key {std::move(field)};
        auto result = _dict->remove(field_key);
        if (_dict->size() == 0) {
            _store->remove(rk);
        }
        return result;
    }

    int hexists(redis_key&& rk, sstring&& field)
    {
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return REDIS_ERR;
        }
        else if (it->type() != REDIS_DICT) {
            return REDIS_WRONG_TYPE;
        }
        auto _dict = it->dict_ptr();
        redis_key field_key {std::move(field)};
        return _dict->exists(field_key);
    }

    std::pair<size_t, int> hstrlen(redis_key&& rk, sstring&& field)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);

        if (!it) {
            return result_type {0, REDIS_ERR};
        }
        else if (it->type() != REDIS_DICT) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        auto _dict = it->dict_ptr();
        redis_key field_key {std::move(field)};
        auto mit = _dict->fetch(field_key);
        return result_type {mit->value_size(), REDIS_OK};
    }

    std::pair<size_t, int> hlen(redis_key&& rk)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);

        if (!it) {
            return result_type {0, REDIS_ERR};
        }
        else if (it->type() != REDIS_DICT) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        auto _dict = it->dict_ptr();
        return result_type {_dict->size(), REDIS_OK};
    }

    template <typename origin = local_origin_tag> inline
    std::pair<size_t, int> hincrby(redis_key&& rk, sstring&& field, size_t delta)
    {
        using result_type = std::pair<int64_t, int>;
        auto it = _store->fetch_raw(rk);
        if (it && it->type() != REDIS_DICT) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        dict* _dict = nullptr;
        if (!it) {
            _dict = new dict();
            auto dict_item = item::create(rk, _dict, REDIS_DICT);
            if (_store->set(rk, dict_item) != REDIS_OK) {
                return result_type {0, REDIS_ERR};
            }
        }
        else {
            _dict = it->dict_ptr();
        }
        redis_key field_key{std::move(field)};
        auto mit = _dict->fetch(field_key);
        auto new_value = delta;
        if (!mit) {
            new_value += mit->int64(); 
        }
        auto new_item = item::create(field_key, static_cast<int64_t>(new_value));
        if (_dict->replace(field_key, new_item) == REDIS_OK) {
            return result_type {new_value, REDIS_OK};
        }
        return result_type {0, REDIS_ERR}; 
    }

    template <typename origin = local_origin_tag> inline
    std::pair<double, int> hincrbyfloat(redis_key&& rk, sstring&& field, double delta)
    {
        using result_type = std::pair<double, int>;
        auto it = _store->fetch_raw(rk);
        if (it && it->type() != REDIS_DICT) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        dict* _dict = nullptr;
        if (!it) {
            _dict = new dict();
            auto dict_item = item::create(rk, _dict, REDIS_DICT);
            if (_store->set(rk, dict_item) != REDIS_OK) {
                return result_type {0, REDIS_ERR};
            }
        }
        redis_key field_key {std::move(field)};
        auto mit = _dict->fetch(field_key);
        auto new_value = delta;
        if (!mit) {
            new_value += mit->Double();
        }
        auto new_item = item::create(field_key, new_value);
        if (_dict->replace(field_key, new_item) == REDIS_OK) {
            return result_type {new_value, REDIS_OK};
        }
        return result_type {0, REDIS_ERR}; 
    }

    std::pair<std::vector<item_ptr>, int> hgetall(redis_key&& rk)
    {
        using result_type = std::pair<std::vector<item_ptr>, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {std::move(std::vector<item_ptr>()), REDIS_ERR};
        }
        else if (it->type() != REDIS_DICT) {
            return result_type {std::move(std::vector<item_ptr>()), REDIS_WRONG_TYPE};
        }
        auto dict = it->dict_ptr();
        return result_type {std::move(dict->fetch()), REDIS_OK};
    }

    std::pair<std::vector<item_ptr>, int> hmget(redis_key&& rk, std::unordered_set<sstring>&& keys)
    {
        using result_type = std::pair<std::vector<item_ptr>, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {std::move(std::vector<item_ptr>()), REDIS_ERR};
        }
        else if (it->type() != REDIS_DICT) {
            return result_type {std::move(std::vector<item_ptr>()), REDIS_WRONG_TYPE};
        }
        auto dict = it->dict_ptr();
        return result_type{std::move(dict->fetch(keys)), REDIS_OK};
    }

    template <typename origin = local_origin_tag> inline
    std::pair<size_t, int> sadds(redis_key&& rk, std::vector<sstring>&& members)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        if (it && it->type() != REDIS_SET) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        dict* _set = nullptr;
        if (!it) {
            _set = new dict();
            auto dict_item = item::create(rk, _set, REDIS_SET);
            if (_store->set(rk, dict_item) != REDIS_OK) {
                return result_type {0, REDIS_ERR};
            }
        }
        else {
            _set = it->dict_ptr();
        }
        int count = 0;
        for (sstring& member : members) {
            redis_key member_data {std::move(member)};
            auto new_item = item::create(member_data);
            if (_set->replace(member_data, new_item) == REDIS_OK) {
                count++;
            }
        }
        return result_type {count, REDIS_OK};
    }

    template<typename origin = local_origin_tag>
    int sadd(redis_key&& rk, sstring&& member)
    {
        auto it = _store->fetch_raw(rk);
        if(it && it->type() != REDIS_SET) {
            return REDIS_WRONG_TYPE;
        }

        dict* _set = nullptr;
        if (!it) {
            auto _set = new dict();
            auto dict_item = item::create(rk, _set, REDIS_SET);
            if (_store->set(rk, dict_item) != REDIS_OK) {
                return REDIS_ERR;
            }
        }
        else {
            _set = it->dict_ptr();
        }
        redis_key member_data {std::move(member)};
        auto new_item = item::create(member_data);
        if (_set->replace(member_data, new_item)) {
            return REDIS_OK;
        }
        return REDIS_ERR;
    }

    std::pair<size_t, int> scard(redis_key&& rk)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {0, REDIS_ERR};
        }
        else if(it->type() != REDIS_SET) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        auto set = it->dict_ptr();
        return result_type {set->size(), REDIS_OK};
    }

    int sismember(redis_key&& rk, sstring&& member)
    {
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return REDIS_ERR;
        }
        else if(it->type() != REDIS_SET) {
            return REDIS_WRONG_TYPE;
        }
        auto _set = it->dict_ptr();
        redis_key member_data {std::move(member)};
        return _set->exists(member_data);
    }

    std::pair<std::vector<item_ptr>, int> smembers(redis_key&& rk)
    {
        using result_type = std::pair<std::vector<item_ptr>, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {std::move(std::vector<item_ptr>()), REDIS_ERR};
        }
        if (it->type() == REDIS_SET) {
            return result_type {std::move(std::vector<item_ptr>()), REDIS_WRONG_TYPE};
        }
        auto _set = it->dict_ptr();
        return result_type {std::move(_set->fetch()), REDIS_OK};
    }

    std::pair<item_ptr, int> spop(redis_key&& rk)
    {
        using result_type = std::pair<item_ptr, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {nullptr, REDIS_ERR};
        }
        if (it->type() == REDIS_SET) {
            return result_type {nullptr, REDIS_WRONG_TYPE};
        }
        auto _set = it->dict_ptr();
        return result_type {std::move(_set->random_fetch_and_remove()), REDIS_OK};
    }

    int srem(redis_key&& rk, sstring&& member)
    {
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return REDIS_ERR;
        }
        if (it->type() == REDIS_SET) {
            return REDIS_WRONG_TYPE;
        }
        auto _set = it->dict_ptr();
        redis_key member_data {std::move(member)};
        return _set->remove(member_data);
    }

    std::pair<size_t, int> srems(redis_key&& rk, std::vector<sstring>&& members)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {0, REDIS_ERR};
        }
        if (it->type() == REDIS_SET) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        auto _set = it->dict_ptr();
        int count = 0;
        if (_set != nullptr) {
            for (sstring& member : members) {
                redis_key member_data {std::move(member)};
                if (_set->remove(member_data) == REDIS_OK) {
                    count++;
                }
            }
            if (_set->size() == 0) {
                _store->remove(rk);
            }
        }
        return result_type {count, REDIS_OK};
    }

    int type(redis_key&& rk);

    long pttl(redis_key&& rk)
    {
        auto item = get(std::move(rk));
        if (item) {
            if (item->ever_expires() == false) {
                return -1;
            }
            auto duration = item->get_timeout() - clock_type::now();
            return static_cast<long>(std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());
        }
        else {
            return -2;
        }
    }

    long ttl(redis_key&& rk)
    {
        auto ret = pttl(std::move(rk));
        if (ret > 0) {
            return ret / 1000;
        }
        return ret;
    }

    template <typename origin = local_origin_tag> inline
    std::pair<size_t, int> zadds(redis_key&& rk, std::unordered_map<sstring, double>&& members, int flags)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        sorted_set* zset = nullptr; 
        if (!it) {
            zset = new sorted_set();
            auto zset_item = item::create(rk, zset, REDIS_ZSET);
            if (_store->set(rk, zset_item) != 0) {
                return result_type {0, REDIS_ERR};
            }
        }
        else {
            if (it->type() == REDIS_ZSET) {
                zset = it->zset_ptr();
            }
            else {
                return result_type {0, REDIS_WRONG_TYPE};
            }
        }
        size_t count = 0;
        for (auto& entry : members) {
            sstring key = entry.first;
            double  score = entry.second;
            redis_key member_data {std::move(key)};
            auto de = zset->fetch(member_data);
            if (de) {
                if (flags & ZADD_NX || score == 0) {
                    continue;
                }
                score += de->Double();
                auto new_item = item::create(member_data, score);
                if (zset->replace(member_data, new_item) == REDIS_OK) {
                    count++;
                }
            }
            else if (!(flags & ZADD_XX)) {
                auto new_item = item::create(member_data, score);
                if (zset->insert(member_data, new_item) == REDIS_OK) {
                    count++;
                }
            }
        }
        return result_type {count, REDIS_OK};
    }

    std::pair<size_t, int> zcard(redis_key&& rk)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {0, REDIS_ERR};
        }
        if (it->type() != REDIS_ZSET) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        auto zset = it->zset_ptr();
        return result_type {zset->size(), REDIS_OK};
    }
    std::pair<size_t, int> zrem(redis_key&& rk, std::vector<sstring>&& members)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {0, REDIS_ERR};
        }
        if (it->type() != REDIS_ZSET) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        auto zset = it->zset_ptr();
        size_t count = 0;
        for (size_t i = 0; i < members.size(); ++i) {
            redis_key m{std::move(members[i])};
            if (zset->remove(m) == REDIS_OK) {
                count++;
            }
        }
        if (zset->size() == 0) {
            _store->remove(rk);
        }
        return result_type {count, REDIS_OK};
    }
    std::pair<size_t, int> zcount(redis_key&& rk, double min, double max)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {0, REDIS_ERR};
        }
        if (it->type() != REDIS_ZSET) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        auto zset = it->zset_ptr();
        return result_type {zset->count(min, max), REDIS_OK};
    }
    template <typename origin = local_origin_tag> inline
    std::pair<double, int> zincrby(redis_key&& rk, sstring&& member, double delta)
    {
        using result_type = std::pair<double, int>;
        auto it = _store->fetch_raw(rk);
        if (it && it->type() != REDIS_ZSET) {
            return result_type{0, REDIS_WRONG_TYPE};
        }
        sorted_set* zset = nullptr;
        if (!it) {
            zset = new sorted_set();
            auto zset_item = item::create(rk, zset, REDIS_ZSET);
            if (_store->set(rk, zset_item) != REDIS_OK) {
                return result_type{0, REDIS_ERR};
            }
        }
        else {
            zset = it->zset_ptr();
        }
        redis_key mk {std::move(member)};
        double new_value = delta;
        auto exists_member = zset->fetch(mk);
        if (exists_member) {
            new_value += exists_member->Double();
        }
        auto new_member = item::create(mk, new_value);
        if (zset->replace(mk, new_member) == REDIS_OK) {
            return result_type {new_value, REDIS_OK};
        }
        else {
            return result_type {0, REDIS_ERR}; 
        }
    }
    std::pair<std::vector<item_ptr>, int> zrange(redis_key&& rk, long begin, long end, bool reverse)
    {
        using result_type = std::pair<std::vector<item_ptr>, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {std::move(std::vector<item_ptr>()), REDIS_OK};
        }
        if (it->type() != REDIS_ZSET) {
            return result_type {std::move(std::vector<item_ptr>()), REDIS_WRONG_TYPE};
        }
        auto zset = it->zset_ptr();
        return result_type {std::move(zset->range_by_rank(begin, end, reverse)), REDIS_OK};
    }
    std::pair<std::vector<item_ptr>, int> zrangebyscore(redis_key&& rk, double min, double max, bool reverse)
    {
        using result_type = std::pair<std::vector<item_ptr>, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {std::move(std::vector<item_ptr>()), REDIS_OK};
        }
        if (it->type() != REDIS_ZSET) {
            return result_type {std::move(std::vector<item_ptr>()), REDIS_WRONG_TYPE};
        }
        auto zset = it->zset_ptr();
        return result_type {std::move(zset->range_by_score(min, max, reverse)), REDIS_OK};
    }

    std::pair<size_t, int> zrank(redis_key&& rk, sstring&& member, bool reverse)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
           return result_type {0, REDIS_ERR};
        }
        else if (it->type() != REDIS_ZSET) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        auto zset = it->zset_ptr();
        redis_key m{std::move(member)};
        return result_type {zset->rank(m, reverse), REDIS_OK};
    }

    std::pair<double, int> zscore(redis_key&& rk, sstring&& member)
    {
        using result_type = std::pair<double, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {0, REDIS_ERR};
        }
        if (it->type() != REDIS_ZSET) {
            return result_type{0, REDIS_WRONG_TYPE};
        }
        auto zset = it->zset_ptr();
        redis_key mk {std::move(member)};
        auto value = zset->fetch(mk);
        if (!value) {
            return result_type{0, REDIS_ERR};
        }
        return result_type{value->Double(), REDIS_OK};
    }

    std::pair<size_t, int> zremrangebyscore(redis_key&& rk, double min, double max)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {0, REDIS_OK};
        }
        if (it && it->type() != REDIS_ZSET) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        auto zset = it->zset_ptr();
        return result_type {zset->remove_range_by_score(min, max), REDIS_OK};
    }
    std::pair<size_t, int> zremrangebyrank(redis_key&& rk, size_t begin, size_t end)
    {
        using result_type = std::pair<size_t, int>;
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return result_type {0, REDIS_OK};
        }
        if (it && it->type() != REDIS_ZSET) {
            return result_type {0, REDIS_WRONG_TYPE};
        }
        auto zset = it->zset_ptr();
        return result_type {zset->remove_range_by_rank(begin, end), REDIS_OK};
    }
    int select(int index)
    {
        _store = &_data_storages[index];
        return REDIS_OK;
    }
    future<> stop() { return make_ready_future<>(); }
private:
    void expired_items()
    {
        using namespace std::chrono;
        auto exp = _alive.expire(clock_type::now());
        while (!exp.empty()) {
            auto it = *exp.begin();
            exp.pop_front();
            // release expired item
            if (it && it->ever_expires()) {
                _store->remove(it);
            }
        }
        _timer.arm(_alive.get_next_timeout());
    }
private:
    static const int DEFAULT_DB_COUNT = 32;
    dict* _store = nullptr;
    dict  _data_storages[DEFAULT_DB_COUNT];
    timer_set<item> _alive;
    timer<clock_type> _timer;
};
}
