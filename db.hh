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
#include "misc_storage.hh"
#include "list_storage.hh"
#include "dict_storage.hh"
#include "set_storage.hh"
#include "sorted_set_storage.hh"
#include "redis_timer_set.hh"
namespace redis {

namespace stdx = std::experimental;
using item_ptr = foreign_ptr<lw_shared_ptr<item>>;

class db {
public:
    db();
    ~db();

    template  <typename origin = local_origin_tag> inline
    int set(const redis_key& key, sstring& val, long expire, uint32_t flag)
    {
        return _misc_storage.set<origin>(key, val, expire, flag);
    }

    template  <typename origin = local_origin_tag> inline
    std::pair<int64_t, int> counter_by(sstring& key, int64_t step, bool incr)
    {
        return _misc_storage.counter_by<origin>(key, step, incr);
    }

    template  <typename origin = local_origin_tag> inline
    int append(sstring& key, sstring& val)
    {
        return _misc_storage.append<origin>(key, val);
    }

    inline int del(sstring& key)
    {
        return _misc_storage.del(key);
    }

    inline int exists(sstring& key)
    {
        return _misc_storage.exists(key);
    }

    inline item_ptr get(sstring& key)
    {
        return _misc_storage.get(key);
    }

    inline int strlen(sstring& key)
    {
        return _misc_storage.strlen(key);
    }

    inline int expire(sstring& key, long expired)
    {
        auto expiry = expiration(expired);
        auto item = _misc_storage.fetch_item(key);
        if (item) {
            item->set_expiry(expiry);
            if (_alive.insert(item)) {
                _timer.rearm(item->get_timeout());
                return 1;
            }
            item->set_never_expired();
        }
        return 0;
    }

    inline int persist(sstring& key)
    {
        auto item = _misc_storage.fetch_item(key);
        if (item) {
            item->set_never_expired();
            return 1;
        }
        return 0;
    }


    template <typename origin = local_origin_tag> inline
    int push(sstring& key, sstring& value, bool force, bool left)
    {
        return _list_storage.push<origin>(key, value, force, left);
    }

    inline item_ptr pop(sstring& key, bool left)
    {
        return _list_storage.pop(key, left);
    }

    inline int llen(sstring& key)
    {
        return _list_storage.llen(key);
    }

    inline item_ptr lindex(sstring& key, int idx)
    {
        return _list_storage.lindex(key, idx);
    }

    template <typename origin = local_origin_tag> inline
    int linsert(sstring& key, sstring& pivot, sstring& value, bool after)
    {
        return _list_storage.linsert<origin>(key, pivot, value, after);
    }

    inline std::vector<item_ptr> lrange(sstring& key, int start, int end)
    {
        return _list_storage.lrange(key, start, end);
    }

    template <typename origin = local_origin_tag> inline
    int lset(sstring& key, int idx, sstring& value)
    {
        return _list_storage.lset<origin>(key, idx, value);
    }

    int lrem(sstring& key, int count, sstring& value)
    {
        return _list_storage.lrem(key, count, value);
    }

    int ltrim(sstring& key, int start, int end)
    {
        return _list_storage.ltrim(key, start, end);
    }


    template <typename origin = local_origin_tag> inline
    int hset(sstring& key, sstring& field, sstring& value)
    {
        return _dict_storage.hset<origin>(key, field, value);
    }

    template <typename origin = local_origin_tag> inline
    int hmset(sstring& key, std::unordered_map<sstring, sstring>& kv)
    {
        return _dict_storage.hmset<origin>(key, kv);
    }

    inline item_ptr hget(sstring& key, sstring& field)
    {
        return _dict_storage.hget(key, field);
    }

    inline int hdel(sstring& key, sstring& field)
    {
        return _dict_storage.hdel(key, field);
    }

    inline int hexists(sstring& key, sstring& field)
    {
        return _dict_storage.hexists(key, field);
    }

    inline int hstrlen(sstring& key, sstring& field)
    {
        return _dict_storage.hstrlen(key, field);
    }

    inline int hlen(sstring& key)
    {
        return _dict_storage.hlen(key);
    }

    template <typename origin = local_origin_tag> inline
    std::pair<size_t, int> hincrby(sstring& key, sstring& field, size_t delta)
    {
        return _dict_storage.hincrby<origin>(key, field, delta);
    }

    template <typename origin = local_origin_tag> inline
    std::pair<double, int> hincrbyfloat(sstring& key, sstring& field, double delta)
    {
        return _dict_storage.hincrbyfloat<origin>(key, field, delta);
    }

    inline std::vector<item_ptr> hgetall(sstring& key)
    {
        return _dict_storage.hgetall(key);
    }

    inline std::vector<item_ptr> hmget(sstring& key, std::unordered_set<sstring>& keys)
    {
        return _dict_storage.hmget(key, keys);
    }

    template <typename origin = local_origin_tag> inline
    int sadds(sstring& key, std::vector<sstring>&& members)
    {
        return _set_storage.sadds(key, std::move(members));
    }

    inline int scard(sstring& key)
    {
        return _set_storage.scard(key );
    }

    inline int sismember(sstring& key, sstring&& member)
    {
        return _set_storage.sismember(key, std::move(member));
    }

    inline std::vector<item_ptr> smembers(sstring& key)
    {
        return _set_storage.smembers(key);
    }

    inline int srems(sstring& key, std::vector<sstring>&& members)
    {
        return _set_storage.srems(key, std::move(members));
    }

    inline int srem(sstring& key, sstring& member)
    {
        return _set_storage.srem(key, member);
    }

    template <typename origin = local_origin_tag> inline
    int sadd(sstring& key, sstring& member)
    {
        return _set_storage.sadd(key, member);
    }

    int type(sstring& key)
    {
        return _misc_storage.type(key);
    }

    long pttl(sstring& key)
    {
        auto item = _misc_storage.get(key);
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

    long ttl(sstring& key)
    {
        auto ret = pttl(key);
        if (ret > 0) {
            return ret / 1000;
        }
        return ret;
    }

    template <typename origin = local_origin_tag> inline
    std::pair<size_t, int> zadds(sstring& key, std::unordered_map<sstring, double>&& members, int flags)
    {
        return _zset_storage.zadds<origin>(key, std::move(members), flags);
    }

    size_t zcard(sstring& key)
    {
        return _zset_storage.zcard(key);
    }
    size_t zcount(sstring& key, double min, double max)
    {
        return _zset_storage.zcount(key, min, max);
    }
    template <typename origin = local_origin_tag> inline
    std::pair<double, bool> zincrby(sstring& key, sstring&& member, double delta)
    {
        return _zset_storage.zincrby<origin>(key, std::move(member), delta);
    }
    std::vector<item_ptr> zrange(const sstring& key, long begin, long end, bool reverse)
    {
        return _zset_storage.zrange(key, begin, end, reverse);
    }
    std::vector<item_ptr> zrangebyscore(sstring& key, double min, double max, bool reverse)
    {
        return _zset_storage.zrangebyscore(key, min, max, reverse);
    }
    size_t zrank(sstring& key, sstring&& member, bool reverse)
    {
        return _zset_storage.zrank(key, std::move(member), reverse);
    }
    size_t zrem(sstring& key, std::vector<sstring>&& members)
    {
        return _zset_storage.zrem(key, std::move(members));
    }
    std::pair<double, bool> zscore(sstring& key, sstring&& member)
    {
        return _zset_storage.zscore(key, std::move(member));
    }
    std::pair<size_t, int> zremrangebyscore(sstring& key, double min, double max)
    {
        return _zset_storage.zremrangebyscore(key, min, max);
    }
    std::pair<size_t, int> zremrangebyrank(sstring& key, size_t begin, size_t end)
    {
        return _zset_storage.zremrangebyrank(key, begin, end);
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
    dict* _store;
    timer_set<item> _alive;
    timer<clock_type> _timer;
    misc_storage _misc_storage;
    list_storage _list_storage;
    dict_storage _dict_storage;
    set_storage  _set_storage;
    sorted_set_storage  _zset_storage;
};
}
