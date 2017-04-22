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
#include "core/shared_ptr.hh"
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "core/sharded.hh"
#include "core/temporary_buffer.hh"
#include <sstream>
#include <iostream>
#include "common.hh"
#include "redis_timer_set.hh"
#include "geo.hh"
#include "bitmap.hh"
#include <tuple>
#include "cache.hh"
#include "reply_builder.hh"
#include  <experimental/vector>
namespace stdx = std::experimental;
namespace redis {
//using item_ptr = foreign_ptr<lw_shared_ptr<item>>;
using scattered_message_ptr = foreign_ptr<lw_shared_ptr<scattered_message<char>>>;
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


class database final : private logalloc::region {
public:
    database();
    ~database();

    bool set(const redis_key& rk, sstring& val, long expire, uint32_t flag);

    future<scattered_message_ptr> counter_by(const redis_key& rk, int64_t step, bool incr);
    future<scattered_message_ptr> append(const redis_key& rk, sstring& val);

    bool del(const redis_key& key);

    bool exists(const redis_key& key);

    future<scattered_message_ptr> get(const redis_key& key);
    future<scattered_message_ptr> strlen(const redis_key& key);

    int expire(const redis_key& rk, long expired);
    int persist(const redis_key& rk);

    // [LIST]
    future<scattered_message_ptr> push(const redis_key& rk, sstring& value, bool force, bool left);
    future<scattered_message_ptr> push_multi(const redis_key& rk, std::vector<sstring>& value, bool force, bool left);
    future<scattered_message_ptr> pop(const redis_key& rk, bool left);
    future<scattered_message_ptr> llen(const redis_key& rk);
    future<scattered_message_ptr> lindex(const redis_key& rk, long idx);
    future<scattered_message_ptr> linsert(const redis_key& rk, sstring& pivot, sstring& value, bool after);
    future<scattered_message_ptr> lrange(const redis_key& rk, long start, long end);
    future<scattered_message_ptr> lset(const redis_key& rk, long idx, sstring& value);
    future<scattered_message_ptr> lrem(const redis_key& rk, long count, sstring& value);
    future<scattered_message_ptr> ltrim(const redis_key& rk, long start, long end);

    // [HASHMAP]
    future<scattered_message_ptr> hset(const redis_key& rk, sstring& field, sstring& value);
    future<scattered_message_ptr> hmset(const redis_key& rk, std::unordered_map<sstring, sstring>& kv);
    future<scattered_message_ptr> hget(const redis_key& rk, sstring& field);
    future<scattered_message_ptr> hdel(const redis_key& rk, sstring& field);
    future<scattered_message_ptr> hdel_multi(const redis_key& rk, std::vector<sstring>& fields);
    future<scattered_message_ptr> hexists(const redis_key& rk, sstring& field);
    future<scattered_message_ptr> hstrlen(const redis_key& rk, sstring& field);
    future<scattered_message_ptr> hlen(const redis_key& rk);
    future<scattered_message_ptr> hincrby(const redis_key& rk, sstring& field, int64_t delta);
    future<scattered_message_ptr> hincrbyfloat(const redis_key& rk, sstring& field, double delta);
    future<scattered_message_ptr> hgetall(const redis_key& rk);
    future<scattered_message_ptr> hgetall_values(const redis_key& rk);
    future<scattered_message_ptr> hgetall_keys(const redis_key& rk);
    future<scattered_message_ptr> hmget(const redis_key& rk, std::vector<sstring>& keys);

    // [SET]
    future<scattered_message_ptr> sadds(const redis_key& rk, std::vector<sstring>& members);
    bool sadds_direct(const redis_key& rk, std::vector<sstring>& members);
    bool sadd_direct(const redis_key& rk, sstring& member);
    future<scattered_message_ptr> sadd(const redis_key& rk, sstring& member);
    future<scattered_message_ptr> scard(const redis_key& rk);
    future<scattered_message_ptr> sismember(const redis_key& rk, sstring& member);
    future<scattered_message_ptr> smembers(const redis_key& rk);
    future<scattered_message_ptr> spop(const redis_key& rk, size_t count);
    future<scattered_message_ptr> srem(const redis_key& rk, sstring& member);
    bool srem_direct(const redis_key& rk, sstring& member);
    future<scattered_message_ptr> srems(const redis_key& rk, std::vector<sstring>& members);
    future<foreign_ptr<lw_shared_ptr<std::vector<sstring>>>> smembers_direct(const redis_key& rk);
    future<scattered_message_ptr> srandmember(const redis_key& rk, size_t count);
    int type(const redis_key& rk);
    long pttl(const redis_key& rk);
    long ttl(const redis_key& rk);
    template <typename origin = local_origin_tag> inline
    std::pair<size_t, int> zadds(const redis_key& rk, std::unordered_map<sstring, double>& members, int flags)
    {
        using result_type = std::pair<size_t, int>;
/*
        auto it = _store->fetch_raw(rk);
        sorted_set* zset = nullptr;
        if (!it) {
            zset = new sorted_set();
            auto zset_item = item::create(rk, zset);
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
*/
        return result_type {0, REDIS_OK};
    }

    std::pair<size_t, int> zcard(const redis_key& rk);
    std::pair<size_t, int> zrem(const redis_key& rk, std::vector<sstring>& members);
    std::pair<size_t, int> zcount(const redis_key& rk, double min, double max);
    template <typename origin = local_origin_tag> inline
    std::pair<double, int> zincrby(const redis_key& rk, sstring& member, double delta)
    {
        using result_type = std::pair<double, int>;
        /*
        auto it = _store->fetch_raw(rk);
        if (it && it->type() != REDIS_ZSET) {
            return result_type{0, REDIS_WRONG_TYPE};
        }
        sorted_set* zset = nullptr;
        if (!it) {
            zset = new sorted_set();
            auto zset_item = item::create(rk, zset);
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
        */
            return result_type{0, REDIS_WRONG_TYPE};
    }
    /*
    //std::pair<std::vector<item_ptr>, int> zrange(const redis_key& rk, long begin, long end, bool reverse);
    //std::pair<std::vector<item_ptr>, int> zrangebyscore(const redis_key& rk, double min, double max, bool reverse);
    std::pair<size_t, int> zrank(const redis_key& rk, sstring& member, bool reverse);
    std::pair<double, int> zscore(const redis_key& rk, sstring& member);
    std::pair<size_t, int> zremrangebyscore(const redis_key& rk, double min, double max);
    std::pair<size_t, int> zremrangebyrank(const redis_key& rk, size_t begin, size_t end);
    int select(int index);

    // [GEO]
    std::pair<double, int> geodist(const redis_key& rk, sstring& lpos, sstring& rpos, int flag);
    std::pair<std::vector<sstring>, int> geohash(const redis_key& rk, std::vector<sstring>& members);
    std::pair<std::vector<std::tuple<double, double, bool>>, int> geopos(const redis_key& rk, std::vector<sstring>& members);
    // [key, dist, score, longitude, latitude]
    using georadius_result_type = std::pair<std::vector<std::tuple<sstring, double, double, double, double>>, int>;
    georadius_result_type georadius_coord(const redis_key& rk, double longtitude, double latitude, double radius, size_t count, int flag);
    georadius_result_type georadius_member(const redis_key& rk, sstring& pos, double radius, size_t count, int flag);

    // [BITMAP]
    std::pair<bool, int> setbit(const redis_key& rk, size_t offset, bool value);
    std::pair<bool, int> getbit(const redis_key& rk, size_t offset);
    std::pair<size_t, int> bitcount(const redis_key& rk, long start, long end);
    std::pair<size_t, int> bitop(const redis_key& rk, int flags, std::vector<sstring>& keys);
    std::pair<size_t, int> bitpos(const redis_key& rk, bool bit, long start, long end);
*/
    future<> stop();
private:
/*
    georadius_result_type georadius(sorted_set* zset, double longtitude, double latitude, double radius, size_t count, int flag);
*/
    void expired_items();
    static inline long alignment_index_base_on(size_t size, long index)
    {
        if (index < 0) {
            index += size;
        }
        return index;
    }

    template<bool Key, bool Value>
    future<scattered_message_ptr> hgetall_impl(const redis_key& rk)
    {
        return _cache_store.with_entry_run(rk, [this] (const cache_entry* e) {
            if (!e) {
                return reply_builder::build(msg_err);
            }
            if (e->type_of_map() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& map = e->value_map();
            std::vector<const dict_entry*> entries;
            map.fetch(entries);
            return reply_builder::build<Key, Value>(entries);
        });
    }
private:
    static const int DEFAULT_DB_COUNT = 32;
    cache _cache_store;
};
}
