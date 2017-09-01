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
#include <tuple>
#include "cache.hh"
#include "reply_builder.hh"
#include  <experimental/vector>
#include "bytes.hh"
namespace redis {

static constexpr uint32_t FLAG_SET_NO = 1 << 0;
static constexpr uint32_t FLAG_SET_EX = 1 << 1;
static constexpr uint32_t FLAG_SET_PX = 1 << 2;
static constexpr uint32_t FLAG_SET_NX = 1 << 3;
static constexpr uint32_t FLAG_SET_XX = 1 << 4;

using namespace seastar;
using scattered_message_ptr = foreign_ptr<lw_shared_ptr<scattered_message<char>>>;
class sset_lsa;
class database final : private logalloc::region {
public:
    database();
    ~database();

    future<scattered_message_ptr> set(const decorated_key& dk, bytes& val, long expired, uint32_t flag);
    future<scattered_message_ptr> del(const decorated_key& key);
    future<scattered_message_ptr> get(const decorated_key& key);
private:
    static const int DEFAULT_DB_COUNT = 1;
    cache _cache_stores[DEFAULT_DB_COUNT];
    size_t current_store_index = 0;
    inline cache& current_store() { return _cache_stores[current_store_index]; }
    seastar::metrics::metric_groups _metrics;
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
    void setup_metrics();
    size_t sum_expiring_entries();
};
}
