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
#include <functional>
#include "core/sharded.hh"
#include "core/sstring.hh"
#include <experimental/optional>
#include <iomanip>
#include <sstream>
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/metrics_registration.hh"
#include "core/bitops.hh"
#include "core/slab.hh"
#include "core/align.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include <unistd.h>
#include <cstdlib>
#include "common.hh"
#include "geo.hh"
namespace redis {

namespace stdx = std::experimental;

struct args_collection;
class database;
using message = scattered_message<char>;
class redis_service {
private:
    inline unsigned get_cpu(const sstring& key) {
        return std::hash<sstring>()(key) % smp::count;
    }
    inline unsigned get_cpu(const redis_key& key) {
        return key.hash() % smp::count;
    }
    distributed<database>& _db;
public:
    redis_service(distributed<database>& db) : _db(db)
    {
        setup_metrics();
    }

    // [TEST APIs]
    future<sstring> echo(args_collection& args);
    // [COUNTER APIs]
    future<> incr(args_collection& args, output_stream<char>& out);
    future<> decr(args_collection& args, output_stream<char>& out);
    future<> incrby(args_collection& args, output_stream<char>& out);
    future<> decrby(args_collection& args, output_stream<char>& out);

    // [STRING APIs]
    future<> mset(args_collection& args, output_stream<char>& out);
    future<> set(args_collection& args, output_stream<char>& out);
    future<> del(args_collection& args, output_stream<char>& out);
    future<> exists(args_collection& args, output_stream<char>& out);
    future<> append(args_collection& args, output_stream<char>& out);
    future<> strlen(args_collection& args, output_stream<char>& out);
    future<> get(args_collection& args, output_stream<char>& out);
    future<> mget(args_collection& args, output_stream<char>& out);

    // [LIST APIs]
    future<> lpush(args_collection& arg, output_stream<char>& out);
    future<> lpushx(args_collection& args, output_stream<char>& out);
    future<> rpush(args_collection& arg, output_stream<char>& out);
    future<> rpushx(args_collection& args, output_stream<char>& out);
    future<> lpop(args_collection& args, output_stream<char>& out);
    future<> rpop(args_collection& args, output_stream<char>& out);
    future<> llen(args_collection& args, output_stream<char>& out);
    future<> lindex(args_collection& args, output_stream<char>& out);
    future<> linsert(args_collection& args, output_stream<char>& out);
    future<> lset(args_collection& args, output_stream<char>& out);
    future<> lrange(args_collection& args, output_stream<char>& out);
    future<> ltrim(args_collection& args, output_stream<char>& out);
    future<> lrem(args_collection& args, output_stream<char>& out);

    // [HASH APIs]
    future<> hdel(args_collection& args, output_stream<char>& out);
    future<> hexists(args_collection& args, output_stream<char>& out);
    future<> hset(args_collection& args, output_stream<char>& out);
    future<> hmset(args_collection& args, output_stream<char>& out);
    future<> hincrby(args_collection& args, output_stream<char>& out);
    future<> hincrbyfloat(args_collection& args, output_stream<char>& out);
    future<> hlen(args_collection& args, output_stream<char>& out);
    future<> hstrlen(args_collection& args, output_stream<char>& out);
    future<> hget(args_collection& args, output_stream<char>& out);
    future<> hgetall(args_collection& args, output_stream<char>& out);
    future<> hgetall_keys(args_collection& args, output_stream<char>& out);
    future<> hgetall_values(args_collection& args, output_stream<char>& out);
    future<> hmget(args_collection& args, output_stream<char>& out);

    // [SET]
    future<> sadd(args_collection& args, output_stream<char>& out);
    future<> scard(args_collection& args, output_stream<char>& out);
    future<> srem(args_collection& args, output_stream<char>& out);
    future<> sismember(args_collection& args, output_stream<char>& out);
    future<> smembers(args_collection& args, output_stream<char>& out);
    future<> sdiff(args_collection& args, output_stream<char>& out);
    future<> sdiff_store(args_collection& args, output_stream<char>& out);
    future<> sinter(args_collection& args, output_stream<char>& out);
    future<> sinter_store(args_collection& args, output_stream<char>& out);
    future<> sunion(args_collection& args, output_stream<char>& out);
    future<> sunion_store(args_collection& args, output_stream<char>& out);
    future<> smove(args_collection& args, output_stream<char>& out);
    future<> srandmember(args_collection& args, output_stream<char>& out);
    future<> spop(args_collection& args, output_stream<char>& out);

    future<> type(args_collection& args, output_stream<char>& out);
    future<> expire(args_collection& args, output_stream<char>& out);
    future<> persist(args_collection& args, output_stream<char>& out);
    future<> pexpire(args_collection& args, output_stream<char>& out);
    future<> ttl(args_collection& args, output_stream<char>& out);
    future<> pttl(args_collection& args, output_stream<char>& out);

    // [ZSET]
    future<> zadd(args_collection& args, output_stream<char>& out);
    future<> zcard(args_collection& args, output_stream<char>& out);
    future<> zrange(args_collection&, bool, output_stream<char>& out);
    future<> zrangebyscore(args_collection&, bool, output_stream<char>& out);
    future<> zcount(args_collection& args, output_stream<char>& out);
    future<> zincrby(args_collection& args, output_stream<char>& out);
    future<> zrank(args_collection&, bool, output_stream<char>& out);
    future<> zrem(args_collection&, output_stream<char>& out);
    future<> zscore(args_collection&, output_stream<char>& out);
    future<> zunionstore(args_collection&, output_stream<char>& out);
    future<> zinterstore(args_collection&, output_stream<char>& out);
    future<> zdiffstore(args_collection&, output_stream<char>& out);
    future<> zunion(args_collection&, output_stream<char>& out);
    future<> zinter(args_collection&, output_stream<char>& out);
    future<> zdiff(args_collection&, output_stream<char>& out);
    future<> zrangebylex(args_collection&, output_stream<char>& out);
    future<> zlexcount(args_collection&, output_stream<char>& out);
    future<> zrevrangebylex(args_collection&, output_stream<char>& out);
    future<> zremrangebyscore(args_collection&, output_stream<char>& out);
    future<> zremrangebyrank(args_collection&, output_stream<char>& out);
    future<> select(args_collection&, output_stream<char>& out);

    // [GEO]
    future<> geoadd(args_collection&, output_stream<char>& out);
    future<> geopos(args_collection&, output_stream<char>& out);
    future<> geodist(args_collection&, output_stream<char>& out);
    future<> geohash(args_collection&, output_stream<char>& out);
    future<> georadius(args_collection&, bool, output_stream<char>& out);

    // [BITMAP]
    future<> setbit(args_collection&, output_stream<char>& out);
    future<> getbit(args_collection&, output_stream<char>& out);
    future<> bitcount(args_collection&, output_stream<char>& out);
    future<> bitop(args_collection&, output_stream<char>& out);
    future<> bitpos(args_collection&, output_stream<char>& out);
    future<> bitfield(args_collection&, output_stream<char>& out);

    // [HLL]
    future<> pfadd(args_collection&, output_stream<char>& out);
    future<> pfcount(args_collection&, output_stream<char>& out);
    future<> pfmerge(args_collection&, output_stream<char>& out);
private:
    future<std::pair<size_t, int>> zadds_impl(sstring& key, std::unordered_map<sstring, double>&& members, int flags);
    future<bool> exists_impl(sstring& key);
    future<> srem_impl(sstring& key, sstring& member, output_stream<char>& out);
    future<> sadd_impl(sstring& key, sstring& member, output_stream<char>& out);
    future<> sadds_impl(sstring& key, std::vector<sstring>& members, output_stream<char>& out);
    future<> sadds_impl_return_keys(sstring& key, std::vector<sstring>& members, output_stream<char>& out);
    future<> sdiff_impl(std::vector<sstring>& keys, sstring* dest, output_stream<char>& out);
    future<> sinter_impl(std::vector<sstring>& keys, sstring* dest, output_stream<char>& out);
    future<> sunion_impl(std::vector<sstring>& keys, sstring* dest, output_stream<char>& out);
    future<> smembers_impl(sstring& key, output_stream<char>& out);
    future<> pop_impl(args_collection& args, bool left, output_stream<char>& out);
    future<> push_impl(args_collection& arg, bool force, bool left, output_stream<char>& out);
    future<> push_impl(sstring& key, sstring& value, bool force, bool left, output_stream<char>& out);
    future<> push_impl(sstring& key, std::vector<sstring>& vals, bool force, bool left, output_stream<char>& out);
    future<bool> srem_direct(sstring& key, sstring& member);
    future<bool> sadd_direct(sstring& key, sstring& member);
    future<bool> set_impl(sstring& key, sstring& value, long expir, uint8_t flag);
    //future<item_ptr> get_impl(sstring& key);
    future<bool> remove_impl(sstring& key);
    future<int> hdel_impl(sstring& key, sstring& field);
    future<> counter_by(args_collection& args, bool incr, bool with_step, output_stream<char>& out);
    using georadius_result_type = std::pair<std::vector<std::tuple<sstring, double, double, double, double>>, int>;
    struct zset_args
    {
        sstring dest;
        size_t numkeys;
        std::vector<sstring> keys;
        std::vector<double> weights;
        int aggregate_flag;
    };
    bool parse_zset_args(args_collection& args, zset_args& uargs);
    static inline double score_aggregation(const double& old, const double& newscore, int flag)
    {
        if (flag == ZAGGREGATE_MIN) {
            return std::min(old, newscore);
        }
        else if (flag == ZAGGREGATE_SUM) {
            return old + newscore;
        }
        else {
            return std::max(old, newscore);
        }
    }
    struct stats {
        uint64_t _echo = 0;
        uint64_t _set = 0;
        uint64_t _get = 0;
        uint64_t _del = 0;
        uint64_t _mset = 0;
        uint64_t _mget = 0;
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
        uint64_t _incr = 0;
        uint64_t _incrby = 0;
        uint64_t _decr = 0;
        uint64_t _decrby = 0;
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
    seastar::metrics::metric_groups _metrics;
    void setup_metrics();
};

} /* namespace redis */
