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
#include "core/metrics_registration.hh"
#include <sstream>
#include <iostream>
#include "common.hh"
#include "redis_timer_set.hh"
#include "geo.hh"
#include "bits_operation.hh"
#include <tuple>
#include "cache.hh"
#include "reply_builder.hh"
#include  <experimental/vector>
namespace stdx = std::experimental;
namespace redis {
using scattered_message_ptr = foreign_ptr<lw_shared_ptr<scattered_message<char>>>;
class sset_lsa;
class database final : private logalloc::region {
public:
    database();
    ~database();

    future<scattered_message_ptr> set(const redis_key& rk, sstring& val, long expire, uint32_t flag);
    bool set_direct(const redis_key& rk, sstring& val, long expire, uint32_t flag);

    future<scattered_message_ptr> counter_by(const redis_key& rk, int64_t step, bool incr);
    future<scattered_message_ptr> append(const redis_key& rk, sstring& val);

    future<scattered_message_ptr> del(const redis_key& key);
    bool del_direct(const redis_key& key);

    future<scattered_message_ptr> exists(const redis_key& key);
    bool exists_direct(const redis_key& key);

    future<scattered_message_ptr> get(const redis_key& key);
    future<foreign_ptr<lw_shared_ptr<sstring>>> get_direct(const redis_key& rk);
    future<scattered_message_ptr> strlen(const redis_key& key);

    future<scattered_message_ptr> expire(const redis_key& rk, long expired);
    future<scattered_message_ptr> persist(const redis_key& rk);
    future<scattered_message_ptr> type(const redis_key& rk);
    future<scattered_message_ptr> pttl(const redis_key& rk);
    future<scattered_message_ptr> ttl(const redis_key& rk);
    bool select(size_t index);

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


    // [SORTED SET]
    future<scattered_message_ptr> zadds(const redis_key& rk, std::unordered_map<sstring, double>& members, int flags);
    bool zadds_direct(const redis_key& rk, std::unordered_map<sstring, double>& members, int flags);
    future<scattered_message_ptr> zcard(const redis_key& rk);
    future<scattered_message_ptr> zrem(const redis_key& rk, std::vector<sstring>& members);
    future<scattered_message_ptr> zcount(const redis_key& rk, double min, double max);
    future<scattered_message_ptr> zincrby(const redis_key& rk, sstring& member, double delta);
    future<scattered_message_ptr> zrange(const redis_key& rk, long begin, long end, bool reverse, bool with_score);
    future<foreign_ptr<lw_shared_ptr<std::vector<std::pair<sstring, double>>>>> zrange_direct(const redis_key& rk, long begin, long end);
    future<scattered_message_ptr> zrangebyscore(const redis_key& rk, double min, double max, bool reverse, bool with_score);
    future<scattered_message_ptr> zrank(const redis_key& rk, sstring& member, bool reverse);
    future<scattered_message_ptr> zscore(const redis_key& rk, sstring& member);
    future<scattered_message_ptr> zremrangebyscore(const redis_key& rk, double min, double max);
    future<scattered_message_ptr> zremrangebyrank(const redis_key& rk, size_t begin, size_t end);

    // [GEO]
    future<scattered_message_ptr> geodist(const redis_key& rk, sstring& lpos, sstring& rpos, int flag);
    future<scattered_message_ptr> geohash(const redis_key& rk, std::vector<sstring>& members);
    future<scattered_message_ptr> geopos(const redis_key& rk, std::vector<sstring>& members);
    using georadius_result_type = std::pair<std::vector<std::tuple<sstring, double, double, double, double>>, int>;
    future<foreign_ptr<lw_shared_ptr<georadius_result_type>>> georadius_coord_direct(const redis_key& rk, double longtitude, double latitude, double radius, size_t count, int flag);
    future<foreign_ptr<lw_shared_ptr<georadius_result_type>>> georadius_member_direct(const redis_key& rk, sstring& pos, double radius, size_t count, int flag);

    // [BITMAP]
    future<scattered_message_ptr> setbit(const redis_key& rk, size_t offset, bool value);
    future<scattered_message_ptr> getbit(const redis_key& rk, size_t offset);
    future<scattered_message_ptr> bitcount(const redis_key& rk, long start, long end);
    future<scattered_message_ptr> bitop(const redis_key& rk, int flags, std::vector<sstring>& keys);
    future<scattered_message_ptr> bitpos(const redis_key& rk, bool bit, long start, long end);

    // [HLL]
    future<scattered_message_ptr> pfadd(const redis_key& rk, std::vector<sstring>& keys);
    future<scattered_message_ptr> pfcount(const redis_key& rk);
    future<scattered_message_ptr> pfmerge(const redis_key& rk, uint8_t* merged_sources, size_t size);
    future<foreign_ptr<lw_shared_ptr<sstring>>> get_hll_direct(const redis_key& rk);

    future<> stop();
private:
    future<foreign_ptr<lw_shared_ptr<georadius_result_type>>> georadius(const sset_lsa&, double longtitude, double latitude, double radius, size_t count, int flag);
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
        return current_store().with_entry_run(rk, [this] (const cache_entry* e) {
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
    cache _cache_stores[DEFAULT_DB_COUNT];
    size_t current_store_index = 0;
    inline cache& current_store() { return _cache_stores[current_store_index]; }
    seastar::metrics::metric_groups _metrics;
    struct db_stats {
        uint64_t _get = 0;
        uint64_t _get_hit = 0;
        uint64_t _set = 0;
        uint64_t _del = 0;

        uint64_t _total_entries = 0;
        uint64_t _total_string_entries = 0;
        uint64_t _total_dict_entries = 0;
        uint64_t _total_set_entries = 0;
        uint64_t _total_sorted_set_entries = 0;
        uint64_t _total_geo_entries = 0;

        uint64_t _expired_entries_pending = 0;
    };
    db_stats _stats;
    void setup_metrics();
};
}
