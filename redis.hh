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
#include "core/bitops.hh"
#include "core/slab.hh"
#include "core/align.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include <unistd.h>
#include <cstdlib>
#include "structures/geo.hh"
#include "structures/sset_lsa.hh"
#include "keys.hh"
#include "request_wrapper.hh"
namespace redis {

namespace stdx = std::experimental;

struct request_wrapper;
class database;
using message = scattered_message<char>;
class redis_service {
private:
    inline unsigned get_cpu(const bytes& key) {
        return std::hash<bytes>()(key) % smp::count;
    }
    inline unsigned get_cpu(const redis_key& key) {
        return key.hash() % smp::count;
    }
    distributed<database>& _db;
public:
    redis_service(distributed<database>& db) : _db(db)
    {
    }

    // [TEST APIs]
    future<bytes> echo(request_wrapper& args);
    // [COUNTER APIs]
    future<> incr(request_wrapper& args, output_stream<char>& out);
    future<> decr(request_wrapper& args, output_stream<char>& out);
    future<> incrby(request_wrapper& args, output_stream<char>& out);
    future<> decrby(request_wrapper& args, output_stream<char>& out);

    // [STRING APIs]
    future<> mset(request_wrapper& args, output_stream<char>& out);
    future<> set(request_wrapper& args, output_stream<char>& out);
    future<> del(request_wrapper& args, output_stream<char>& out);
    future<> exists(request_wrapper& args, output_stream<char>& out);
    future<> append(request_wrapper& args, output_stream<char>& out);
    future<> strlen(request_wrapper& args, output_stream<char>& out);
    future<> get(request_wrapper& args, output_stream<char>& out);
    future<> mget(request_wrapper& args, output_stream<char>& out);

    // [LIST APIs]
    future<> lpush(request_wrapper& arg, output_stream<char>& out);
    future<> lpushx(request_wrapper& args, output_stream<char>& out);
    future<> rpush(request_wrapper& arg, output_stream<char>& out);
    future<> rpushx(request_wrapper& args, output_stream<char>& out);
    future<> lpop(request_wrapper& args, output_stream<char>& out);
    future<> rpop(request_wrapper& args, output_stream<char>& out);
    future<> llen(request_wrapper& args, output_stream<char>& out);
    future<> lindex(request_wrapper& args, output_stream<char>& out);
    future<> linsert(request_wrapper& args, output_stream<char>& out);
    future<> lset(request_wrapper& args, output_stream<char>& out);
    future<> lrange(request_wrapper& args, output_stream<char>& out);
    future<> ltrim(request_wrapper& args, output_stream<char>& out);
    future<> lrem(request_wrapper& args, output_stream<char>& out);

    // [HASH APIs]
    future<> hdel(request_wrapper& args, output_stream<char>& out);
    future<> hexists(request_wrapper& args, output_stream<char>& out);
    future<> hset(request_wrapper& args, output_stream<char>& out);
    future<> hmset(request_wrapper& args, output_stream<char>& out);
    future<> hincrby(request_wrapper& args, output_stream<char>& out);
    future<> hincrbyfloat(request_wrapper& args, output_stream<char>& out);
    future<> hlen(request_wrapper& args, output_stream<char>& out);
    future<> hstrlen(request_wrapper& args, output_stream<char>& out);
    future<> hget(request_wrapper& args, output_stream<char>& out);
    future<> hgetall(request_wrapper& args, output_stream<char>& out);
    future<> hgetall_keys(request_wrapper& args, output_stream<char>& out);
    future<> hgetall_values(request_wrapper& args, output_stream<char>& out);
    future<> hmget(request_wrapper& args, output_stream<char>& out);

    // [SET]
    future<> sadd(request_wrapper& args, output_stream<char>& out);
    future<> scard(request_wrapper& args, output_stream<char>& out);
    future<> srem(request_wrapper& args, output_stream<char>& out);
    future<> sismember(request_wrapper& args, output_stream<char>& out);
    future<> smembers(request_wrapper& args, output_stream<char>& out);
    future<> sdiff(request_wrapper& args, output_stream<char>& out);
    future<> sdiff_store(request_wrapper& args, output_stream<char>& out);
    future<> sinter(request_wrapper& args, output_stream<char>& out);
    future<> sinter_store(request_wrapper& args, output_stream<char>& out);
    future<> sunion(request_wrapper& args, output_stream<char>& out);
    future<> sunion_store(request_wrapper& args, output_stream<char>& out);
    future<> smove(request_wrapper& args, output_stream<char>& out);
    future<> srandmember(request_wrapper& args, output_stream<char>& out);
    future<> spop(request_wrapper& args, output_stream<char>& out);

    future<> type(request_wrapper& args, output_stream<char>& out);
    future<> expire(request_wrapper& args, output_stream<char>& out);
    future<> persist(request_wrapper& args, output_stream<char>& out);
    future<> pexpire(request_wrapper& args, output_stream<char>& out);
    future<> ttl(request_wrapper& args, output_stream<char>& out);
    future<> pttl(request_wrapper& args, output_stream<char>& out);

    // [ZSET]
    future<> zadd(request_wrapper& args, output_stream<char>& out);
    future<> zcard(request_wrapper& args, output_stream<char>& out);
    future<> zrange(request_wrapper&, bool, output_stream<char>& out);
    future<> zrangebyscore(request_wrapper&, bool, output_stream<char>& out);
    future<> zcount(request_wrapper& args, output_stream<char>& out);
    future<> zincrby(request_wrapper& args, output_stream<char>& out);
    future<> zrank(request_wrapper&, bool, output_stream<char>& out);
    future<> zrem(request_wrapper&, output_stream<char>& out);
    future<> zscore(request_wrapper&, output_stream<char>& out);
    future<> zunionstore(request_wrapper&, output_stream<char>& out);
    future<> zinterstore(request_wrapper&, output_stream<char>& out);
    future<> zdiffstore(request_wrapper&, output_stream<char>& out);
    future<> zunion(request_wrapper&, output_stream<char>& out);
    future<> zinter(request_wrapper&, output_stream<char>& out);
    future<> zdiff(request_wrapper&, output_stream<char>& out);
    future<> zrangebylex(request_wrapper&, output_stream<char>& out);
    future<> zlexcount(request_wrapper&, output_stream<char>& out);
    future<> zrevrangebylex(request_wrapper&, output_stream<char>& out);
    future<> zremrangebyscore(request_wrapper&, output_stream<char>& out);
    future<> zremrangebyrank(request_wrapper&, output_stream<char>& out);
    future<> select(request_wrapper&, output_stream<char>& out);

    // [GEO]
    future<> geoadd(request_wrapper&, output_stream<char>& out);
    future<> geopos(request_wrapper&, output_stream<char>& out);
    future<> geodist(request_wrapper&, output_stream<char>& out);
    future<> geohash(request_wrapper&, output_stream<char>& out);
    future<> georadius(request_wrapper&, bool, output_stream<char>& out);

    // [BITMAP]
    future<> setbit(request_wrapper&, output_stream<char>& out);
    future<> getbit(request_wrapper&, output_stream<char>& out);
    future<> bitcount(request_wrapper&, output_stream<char>& out);
    future<> bitop(request_wrapper&, output_stream<char>& out);
    future<> bitpos(request_wrapper&, output_stream<char>& out);
    future<> bitfield(request_wrapper&, output_stream<char>& out);

    // [HLL]
    future<> pfadd(request_wrapper&, output_stream<char>& out);
    future<> pfcount(request_wrapper&, output_stream<char>& out);
    future<> pfmerge(request_wrapper&, output_stream<char>& out);
private:
    future<std::pair<size_t, int>> zadds_impl(bytes& key, std::unordered_map<bytes, double>&& members, int flags);
    future<bool> exists_impl(bytes& key);
    future<> srem_impl(bytes& key, bytes& member, output_stream<char>& out);
    future<> sadd_impl(bytes& key, bytes& member, output_stream<char>& out);
    future<> sadds_impl(bytes& key, std::vector<bytes>& members, output_stream<char>& out);
    future<> sadds_impl_return_keys(bytes& key, std::vector<bytes>& members, output_stream<char>& out);
    future<> sdiff_impl(std::vector<bytes>& keys, bytes* dest, output_stream<char>& out);
    future<> sinter_impl(std::vector<bytes>& keys, bytes* dest, output_stream<char>& out);
    future<> sunion_impl(std::vector<bytes>& keys, bytes* dest, output_stream<char>& out);
    future<> smembers_impl(bytes& key, output_stream<char>& out);
    future<> pop_impl(request_wrapper& args, bool left, output_stream<char>& out);
    future<> push_impl(request_wrapper& arg, bool force, bool left, output_stream<char>& out);
    future<> push_impl(bytes& key, bytes& value, bool force, bool left, output_stream<char>& out);
    future<> push_impl(bytes& key, std::vector<bytes>& vals, bool force, bool left, output_stream<char>& out);
    future<bool> srem_direct(bytes& key, bytes& member);
    future<bool> sadd_direct(bytes& key, bytes& member);
    future<bool> set_impl(bytes& key, bytes& value, long expir, uint8_t flag);
    //future<item_ptr> get_impl(bytes& key);
    future<bool> remove_impl(bytes& key);
    future<int> hdel_impl(bytes& key, bytes& field);
    future<> counter_by(request_wrapper& args, bool incr, bool with_step, output_stream<char>& out);
    using georadius_result_type = std::pair<std::vector<std::tuple<bytes, double, double, double, double>>, int>;
    struct zset_args
    {
        bytes dest;
        size_t numkeys;
        std::vector<bytes> keys;
        std::vector<double> weights;
        int aggregate_flag;
    };
    bool parse_zset_args(request_wrapper& args, zset_args& uargs);
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
};

} /* namespace redis */
