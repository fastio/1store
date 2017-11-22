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
using scattered_message_ptr = foreign_ptr<lw_shared_ptr<scattered_message<char>>>;
class redis_service {
private:
    inline unsigned get_cpu(const bytes& key) {
        return std::hash<bytes>()(key) % smp::count;
    }
    inline unsigned get_cpu(const redis_key& key) {
        return key.hash() % smp::count;
    }
public:
    redis_service()
    {
    }

    // [TEST APIs]
    future<bytes> echo(request_wrapper& args);
    future<scattered_message_ptr> ping(request_wrapper& args);
    future<scattered_message_ptr> command(request_wrapper& args);
    // [COUNTER APIs]
    future<scattered_message_ptr> incr(request_wrapper& args);
    future<scattered_message_ptr> decr(request_wrapper& args);
    future<scattered_message_ptr> incrby(request_wrapper& args);
    future<scattered_message_ptr> decrby(request_wrapper& args);

    // [STRING APIs]
    future<scattered_message_ptr> mset(request_wrapper& args);
    future<scattered_message_ptr> set(request_wrapper& args);
    future<scattered_message_ptr> del(request_wrapper& args);
    future<scattered_message_ptr> exists(request_wrapper& args);
    future<scattered_message_ptr> append(request_wrapper& args);
    future<scattered_message_ptr> strlen(request_wrapper& args);
    future<scattered_message_ptr> get(request_wrapper& args);
    future<scattered_message_ptr> mget(request_wrapper& args);

    // [LIST APIs]
    future<scattered_message_ptr> lpush(request_wrapper& arg);
    future<scattered_message_ptr> lpushx(request_wrapper& args);
    future<scattered_message_ptr> rpush(request_wrapper& arg);
    future<scattered_message_ptr> rpushx(request_wrapper& args);
    future<scattered_message_ptr> lpop(request_wrapper& args);
    future<scattered_message_ptr> rpop(request_wrapper& args);
    future<scattered_message_ptr> llen(request_wrapper& args);
    future<scattered_message_ptr> lindex(request_wrapper& args);
    future<scattered_message_ptr> linsert(request_wrapper& args);
    future<scattered_message_ptr> lset(request_wrapper& args);
    future<scattered_message_ptr> lrange(request_wrapper& args);
    future<scattered_message_ptr> ltrim(request_wrapper& args);
    future<scattered_message_ptr> lrem(request_wrapper& args);

    // [HASH APIs]
    future<scattered_message_ptr> hdel(request_wrapper& args);
    future<scattered_message_ptr> hexists(request_wrapper& args);
    future<scattered_message_ptr> hset(request_wrapper& args);
    future<scattered_message_ptr> hmset(request_wrapper& args);
    future<scattered_message_ptr> hincrby(request_wrapper& args);
    future<scattered_message_ptr> hincrbyfloat(request_wrapper& args);
    future<scattered_message_ptr> hlen(request_wrapper& args);
    future<scattered_message_ptr> hstrlen(request_wrapper& args);
    future<scattered_message_ptr> hget(request_wrapper& args);
    future<scattered_message_ptr> hgetall(request_wrapper& args);
    future<scattered_message_ptr> hgetall_keys(request_wrapper& args);
    future<scattered_message_ptr> hgetall_values(request_wrapper& args);
    future<scattered_message_ptr> hmget(request_wrapper& args);

    // [SET]
    future<scattered_message_ptr> sadd(request_wrapper& args);
    future<scattered_message_ptr> scard(request_wrapper& args);
    future<scattered_message_ptr> srem(request_wrapper& args);
    future<scattered_message_ptr> sismember(request_wrapper& args);
    future<scattered_message_ptr> smembers(request_wrapper& args);
    future<scattered_message_ptr> sdiff(request_wrapper& args);
    future<scattered_message_ptr> sdiff_store(request_wrapper& args);
    future<scattered_message_ptr> sinter(request_wrapper& args);
    future<scattered_message_ptr> sinter_store(request_wrapper& args);
    future<scattered_message_ptr> sunion(request_wrapper& args);
    future<scattered_message_ptr> sunion_store(request_wrapper& args);
    future<scattered_message_ptr> smove(request_wrapper& args);
    future<scattered_message_ptr> srandmember(request_wrapper& args);
    future<scattered_message_ptr> spop(request_wrapper& args);

    future<scattered_message_ptr> type(request_wrapper& args);
    future<scattered_message_ptr> expire(request_wrapper& args);
    future<scattered_message_ptr> persist(request_wrapper& args);
    future<scattered_message_ptr> pexpire(request_wrapper& args);
    future<scattered_message_ptr> ttl(request_wrapper& args);
    future<scattered_message_ptr> pttl(request_wrapper& args);

    // [ZSET]
    future<scattered_message_ptr> zadd(request_wrapper& args);
    future<scattered_message_ptr> zcard(request_wrapper& args);
    future<scattered_message_ptr> zrange(request_wrapper&, bool);
    future<scattered_message_ptr> zrangebyscore(request_wrapper&, bool);
    future<scattered_message_ptr> zcount(request_wrapper& args);
    future<scattered_message_ptr> zincrby(request_wrapper& args);
    future<scattered_message_ptr> zrank(request_wrapper&, bool);
    future<scattered_message_ptr> zrem(request_wrapper&);
    future<scattered_message_ptr> zscore(request_wrapper&);
    future<scattered_message_ptr> zunionstore(request_wrapper&);
    future<scattered_message_ptr> zinterstore(request_wrapper&);
    future<scattered_message_ptr> zdiffstore(request_wrapper&);
    future<scattered_message_ptr> zunion(request_wrapper&);
    future<scattered_message_ptr> zinter(request_wrapper&);
    future<scattered_message_ptr> zdiff(request_wrapper&);
    future<scattered_message_ptr> zrangebylex(request_wrapper&);
    future<scattered_message_ptr> zlexcount(request_wrapper&);
    future<scattered_message_ptr> zrevrangebylex(request_wrapper&);
    future<scattered_message_ptr> zremrangebyscore(request_wrapper&);
    future<scattered_message_ptr> zremrangebyrank(request_wrapper&);
    future<scattered_message_ptr> select(request_wrapper&);

    // [GEO]
    future<scattered_message_ptr> geoadd(request_wrapper&);
    future<scattered_message_ptr> geopos(request_wrapper&);
    future<scattered_message_ptr> geodist(request_wrapper&);
    future<scattered_message_ptr> geohash(request_wrapper&);
    future<scattered_message_ptr> georadius(request_wrapper&, bool);

    // [BITMAP]
    future<scattered_message_ptr> setbit(request_wrapper&);
    future<scattered_message_ptr> getbit(request_wrapper&);
    future<scattered_message_ptr> bitcount(request_wrapper&);
    future<scattered_message_ptr> bitop(request_wrapper&);
    future<scattered_message_ptr> bitpos(request_wrapper&);
    future<scattered_message_ptr> bitfield(request_wrapper&);

    // [HLL]
    future<scattered_message_ptr> pfadd(request_wrapper&);
    future<scattered_message_ptr> pfcount(request_wrapper&);
    future<scattered_message_ptr> pfmerge(request_wrapper&);
private:
    future<std::pair<size_t, int>> zadds_impl(bytes& key, std::unordered_map<bytes, double>&& members, int flags);
    future<bool> exists_impl(bytes& key);
    future<scattered_message_ptr> srem_impl(bytes& key, bytes& member);
    future<scattered_message_ptr> sadd_impl(bytes& key, bytes& member);
    future<scattered_message_ptr> sadds_impl(bytes& key, std::vector<bytes>& members);
    future<scattered_message_ptr> sadds_impl_return_keys(bytes& key, std::vector<bytes>& members);
    future<scattered_message_ptr> sdiff_impl(std::vector<bytes>& keys, bytes* dest);
    future<scattered_message_ptr> sinter_impl(std::vector<bytes>& keys, bytes* dest);
    future<scattered_message_ptr> sunion_impl(std::vector<bytes>& keys, bytes* dest);
    future<scattered_message_ptr> smembers_impl(bytes& key);
    future<scattered_message_ptr> pop_impl(request_wrapper& args, bool left);
    future<scattered_message_ptr> push_impl(request_wrapper& arg, bool force, bool left);
    future<scattered_message_ptr> push_impl(bytes& key, bytes& value, bool force, bool left);
    future<scattered_message_ptr> push_impl(bytes& key, std::vector<bytes>& vals, bool force, bool left);
    future<bool> srem_direct(bytes& key, bytes& member);
    future<bool> sadd_direct(bytes& key, bytes& member);
    future<bool> set_impl(bytes& key, bytes& value, long expir, uint8_t flag);
    //future<item_ptr> get_impl(bytes& key);
    future<bool> remove_impl(bytes& key);
    future<int> hdel_impl(bytes& key, bytes& field);
    future<scattered_message_ptr> counter_by(request_wrapper& args, bool incr, bool with_step);
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
