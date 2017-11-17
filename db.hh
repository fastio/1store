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
#include "core/semaphore.hh"
#include <sstream>
#include <iostream>
#include "structures/geo.hh"
#include "structures/bits_operation.hh"
#include <tuple>
#include "cache.hh"
#include "keys.hh"
#include "reply_builder.hh"
#include  <experimental/vector>
#include "commit_log.hh"
namespace stdx = std::experimental;
namespace redis {

enum {
    FLAG_SET_NO = 1 << 0,
    FLAG_SET_EX = 1 << 1,
    FLAG_SET_PX = 1 << 2,
    FLAG_SET_NX = 1 << 3,
    FLAG_SET_XX = 1 << 4,
};

using scattered_message_ptr = foreign_ptr<lw_shared_ptr<scattered_message<char>>>;
class sset_lsa;
class database final : private logalloc::region {
public:
    database();
    ~database();

    future<> initialize();

    future<scattered_message_ptr> set(const redis_key& rk, bytes& val, long expire, uint32_t flag);
    bool set_direct(const redis_key& rk, bytes& val, long expire, uint32_t flag);

    future<scattered_message_ptr> counter_by(const redis_key& rk, int64_t step, bool incr);
    future<scattered_message_ptr> append(const redis_key& rk, bytes& val);

    future<scattered_message_ptr> del(const redis_key& key);
    bool del_direct(const redis_key& key);

    future<scattered_message_ptr> exists(const redis_key& key);
    bool exists_direct(const redis_key& key);

    future<scattered_message_ptr> get(const redis_key& key);
    future<foreign_ptr<lw_shared_ptr<bytes>>> get_direct(const redis_key& rk);
    future<scattered_message_ptr> strlen(const redis_key& key);

    future<scattered_message_ptr> expire(const redis_key& rk, long expired);
    future<scattered_message_ptr> persist(const redis_key& rk);
    future<scattered_message_ptr> type(const redis_key& rk);
    future<scattered_message_ptr> pttl(const redis_key& rk);
    future<scattered_message_ptr> ttl(const redis_key& rk);
    bool select(size_t index);

    // [LIST]
    future<scattered_message_ptr> push(const redis_key& rk, bytes& value, bool force, bool left);
    future<scattered_message_ptr> push_multi(const redis_key& rk, std::vector<bytes>& value, bool force, bool left);
    future<scattered_message_ptr> pop(const redis_key& rk, bool left);
    future<scattered_message_ptr> llen(const redis_key& rk);
    future<scattered_message_ptr> lindex(const redis_key& rk, long idx);
    future<scattered_message_ptr> linsert(const redis_key& rk, bytes& pivot, bytes& value, bool after);
    future<scattered_message_ptr> lrange(const redis_key& rk, long start, long end);
    future<scattered_message_ptr> lset(const redis_key& rk, long idx, bytes& value);
    future<scattered_message_ptr> lrem(const redis_key& rk, long count, bytes& value);
    future<scattered_message_ptr> ltrim(const redis_key& rk, long start, long end);

    // [HASHMAP]
    future<scattered_message_ptr> hset(const redis_key& rk, bytes& field, bytes& value);
    future<scattered_message_ptr> hmset(const redis_key& rk, std::unordered_map<bytes, bytes>& kv);
    future<scattered_message_ptr> hget(const redis_key& rk, bytes& field);
    future<scattered_message_ptr> hdel(const redis_key& rk, bytes& field);
    future<scattered_message_ptr> hdel_multi(const redis_key& rk, std::vector<bytes>& fields);
    future<scattered_message_ptr> hexists(const redis_key& rk, bytes& field);
    future<scattered_message_ptr> hstrlen(const redis_key& rk, bytes& field);
    future<scattered_message_ptr> hlen(const redis_key& rk);
    future<scattered_message_ptr> hincrby(const redis_key& rk, bytes& field, int64_t delta);
    future<scattered_message_ptr> hincrbyfloat(const redis_key& rk, bytes& field, double delta);
    future<scattered_message_ptr> hgetall(const redis_key& rk);
    future<scattered_message_ptr> hgetall_values(const redis_key& rk);
    future<scattered_message_ptr> hgetall_keys(const redis_key& rk);
    future<scattered_message_ptr> hmget(const redis_key& rk, std::vector<bytes>& keys);

    // [SET]
    future<scattered_message_ptr> sadds(const redis_key& rk, std::vector<bytes>& members);
    bool sadds_direct(const redis_key& rk, std::vector<bytes>& members);
    bool sadd_direct(const redis_key& rk, bytes& member);
    future<scattered_message_ptr> sadd(const redis_key& rk, bytes& member);
    future<scattered_message_ptr> scard(const redis_key& rk);
    future<scattered_message_ptr> sismember(const redis_key& rk, bytes& member);
    future<scattered_message_ptr> smembers(const redis_key& rk);
    future<scattered_message_ptr> spop(const redis_key& rk, size_t count);
    future<scattered_message_ptr> srem(const redis_key& rk, bytes& member);
    bool srem_direct(const redis_key& rk, bytes& member);
    future<scattered_message_ptr> srems(const redis_key& rk, std::vector<bytes>& members);
    future<foreign_ptr<lw_shared_ptr<std::vector<bytes>>>> smembers_direct(const redis_key& rk);
    future<scattered_message_ptr> srandmember(const redis_key& rk, size_t count);


    // [SORTED SET]
    future<scattered_message_ptr> zadds(const redis_key& rk, std::unordered_map<bytes, double>& members, int flags);
    bool zadds_direct(const redis_key& rk, std::unordered_map<bytes, double>& members, int flags);
    future<scattered_message_ptr> zcard(const redis_key& rk);
    future<scattered_message_ptr> zrem(const redis_key& rk, std::vector<bytes>& members);
    future<scattered_message_ptr> zcount(const redis_key& rk, double min, double max);
    future<scattered_message_ptr> zincrby(const redis_key& rk, bytes& member, double delta);
    future<scattered_message_ptr> zrange(const redis_key& rk, long begin, long end, bool reverse, bool with_score);
    future<foreign_ptr<lw_shared_ptr<std::vector<std::pair<bytes, double>>>>> zrange_direct(const redis_key& rk, long begin, long end);
    future<scattered_message_ptr> zrangebyscore(const redis_key& rk, double min, double max, bool reverse, bool with_score);
    future<scattered_message_ptr> zrank(const redis_key& rk, bytes& member, bool reverse);
    future<scattered_message_ptr> zscore(const redis_key& rk, bytes& member);
    future<scattered_message_ptr> zremrangebyscore(const redis_key& rk, double min, double max);
    future<scattered_message_ptr> zremrangebyrank(const redis_key& rk, size_t begin, size_t end);

    // [GEO]
    future<scattered_message_ptr> geodist(const redis_key& rk, bytes& lpos, bytes& rpos, int flag);
    future<scattered_message_ptr> geohash(const redis_key& rk, std::vector<bytes>& members);
    future<scattered_message_ptr> geopos(const redis_key& rk, std::vector<bytes>& members);
    using georadius_result_type = std::pair<std::vector<std::tuple<bytes, double, double, double, double>>, int>;
    future<foreign_ptr<lw_shared_ptr<georadius_result_type>>> georadius_coord_direct(const redis_key& rk, double longtitude, double latitude, double radius, size_t count, int flag);
    future<foreign_ptr<lw_shared_ptr<georadius_result_type>>> georadius_member_direct(const redis_key& rk, bytes& pos, double radius, size_t count, int flag);

    // [BITMAP]
    future<scattered_message_ptr> setbit(const redis_key& rk, size_t offset, bool value);
    future<scattered_message_ptr> getbit(const redis_key& rk, size_t offset);
    future<scattered_message_ptr> bitcount(const redis_key& rk, long start, long end);
    future<scattered_message_ptr> bitop(const redis_key& rk, int flags, std::vector<bytes>& keys);
    future<scattered_message_ptr> bitpos(const redis_key& rk, bool bit, long start, long end);

    // [HLL]
    future<scattered_message_ptr> pfadd(const redis_key& rk, std::vector<bytes>& keys);
    future<scattered_message_ptr> pfcount(const redis_key& rk);
    future<scattered_message_ptr> pfmerge(const redis_key& rk, uint8_t* merged_sources, size_t size);
    future<foreign_ptr<lw_shared_ptr<bytes>>> get_hll_direct(const redis_key& rk);

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
        ++_stat._read;
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
            if (!entries.empty()) ++_stat._hit;
            return reply_builder::build<Key, Value>(entries);
        });
    }
private:
    cache _cache;
    inline cache& current_store() { return _cache; }
    seastar::metrics::metric_groups _metrics;
    lw_shared_ptr<store::commit_log> _commit_log { nullptr };
    struct stats {
        uint64_t _read = 0;
        uint64_t _hit = 0;
        uint64_t _total_counter_entries = 0;
        uint64_t _total_string_entries = 0;
        uint64_t _total_dict_entries = 0;
        uint64_t _total_list_entries = 0;
        uint64_t _total_set_entries = 0;
        uint64_t _total_zset_entries = 0;
        uint64_t _total_bitmap_entries = 0;
        uint64_t _total_hll_entries = 0;

        uint64_t _echo = 0;
        uint64_t _set = 0;
        uint64_t _get = 0;
        uint64_t _del = 0;
        uint64_t _mset = 0;
        uint64_t _mget = 0;
        uint64_t _counter = 0;
        uint64_t _strlen = 0;
        uint64_t _exists = 0;
        uint64_t _append = 0;
        uint64_t _lpush = 0;
        uint64_t _lpushx = 0;
        uint64_t _rpush = 0;
        uint64_t _rpushx = 0;
        uint64_t _lpop = 0;
        uint64_t _rpop = 0;
        uint64_t _lindex = 0;
        uint64_t _llen = 0;
        uint64_t _linsert = 0;
        uint64_t _lrange = 0;
        uint64_t _lset = 0;
        uint64_t _ltrim = 0;
        uint64_t _lrem = 0;
        uint64_t _hdel = 0;
        uint64_t _hexists = 0;
        uint64_t _hset = 0;
        uint64_t _hget = 0;
        uint64_t _hmset = 0;
        uint64_t _hincrby = 0;
        uint64_t _hincrbyfloat = 0;
        uint64_t _hlen = 0;
        uint64_t _hstrlen = 0;
        uint64_t _hgetall = 0;
        uint64_t _hgetall_keys = 0;
        uint64_t _hgetall_values = 0;
        uint64_t _hmget = 0;
        uint64_t _smembers = 0;
        uint64_t _sadd = 0;
        uint64_t _scard = 0;
        uint64_t _sismember = 0;
        uint64_t _srem = 0;
        uint64_t _sdiff = 0;
        uint64_t _sdiff_store = 0;
        uint64_t _sinter = 0;
        uint64_t _sinter_store = 0;
        uint64_t _sunion = 0;
        uint64_t _sunion_store = 0;
        uint64_t _smove = 0;
        uint64_t _srandmember = 0;
        uint64_t _spop = 0;
        uint64_t _type = 0;
        uint64_t _expire = 0;
        uint64_t _pexpire = 0;
        uint64_t _pttl = 0;
        uint64_t _ttl = 0;
        uint64_t _persist = 0;
        uint64_t _zadd  = 0;
        uint64_t _zcard = 0;
        uint64_t _zrange = 0;
        uint64_t _zrangebyscore = 0;
        uint64_t _zcount = 0;
        uint64_t _zincrby = 0;
        uint64_t _zrank = 0;
        uint64_t _zrem = 0;
        uint64_t _zscore = 0;
        uint64_t _zunionstore = 0;
        uint64_t _zinterstore = 0;
        uint64_t _zdiffstore = 0;
        uint64_t _zremrangebyscore = 0;
        uint64_t _zremrangebyrank = 0;
        uint64_t _zdiff = 0;
        uint64_t _zunion = 0;
        uint64_t _zinter = 0;
        uint64_t _zrangebylex = 0;
        uint64_t _zlexcount = 0;
        uint64_t _select  = 0;
        uint64_t _geoadd = 0;
        uint64_t _geodist = 0;
        uint64_t _geohash = 0;
        uint64_t _geopos = 0;
        uint64_t _georadius = 0;
        uint64_t _setbit = 0;
        uint64_t _getbit = 0;
        uint64_t _bitcount = 0;
        uint64_t _bitop = 0;
        uint64_t _bitpos = 0;
        uint64_t _bitfield = 0;
        uint64_t _pfadd = 0;
        uint64_t _pfcount = 0;
        uint64_t _pfmerge = 0;
    };
    stats _stat;
    lw_shared_ptr<store::column_family> _sys_cf;
    lw_shared_ptr<store::column_family> _data_cf;
    using timeout_exception_factory = default_timeout_exception_factory;
    basic_semaphore<timeout_exception_factory> _flush_cache;
    bool _shutdown { false };
    // periodically flush dirty cache_entry to column_family
    using clock_type = lowres_clock;
    timer<clock_type> _flush_timer;
    void on_timer();
    void setup_metrics();
    size_t sum_expiring_entries();
};
}
