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
#include "db.hh"
#include <random>
#include <chrono>
#include <algorithm>
#include "util/log.hh"
#include "structures/bits_operation.hh"
#include "core/metrics.hh"
#include "structures/hll.hh"

using logger =  seastar::logger;
static logger db_log ("db");

namespace redis {


class rand_generater final
{
    friend class database;
public:
    inline static size_t rand_less_than(const size_t size) {
        static thread_local rand_generater _rand;
        return _rand.rand() % size;
    }
private:
    rand_generater()
        : _re(std::chrono::system_clock::now().time_since_epoch().count())
        , _dist(0, std::numeric_limits<size_t>::max())
    {
    }

    ~rand_generater()
    {
    }

    inline size_t rand()
    {
        return _dist(_re);
    }

    std::default_random_engine _re;
    std::uniform_int_distribution<size_t> _dist;
};

distributed<database> _the_database;

database::database()
{
    using namespace std::chrono;

    for (size_t i = 0; i < DEFAULT_DB_COUNT; ++i) {
        auto& store = _cache_stores[i];
        _cache_stores[i].set_expired_entry_releaser([this, &store] (cache_entry& e) {
             with_allocator(allocator(), [this, &store, &e] {
                 auto type = e.type();
                 if (store.erase(e)) {
                     switch (type) {
                         case entry_type::ENTRY_FLOAT:
                         case entry_type::ENTRY_INT64:
                             --_stat._total_counter_entries;
                             break;
                         case entry_type::ENTRY_BYTES:
                             --_stat._total_string_entries;
                             break;
                         case entry_type::ENTRY_LIST:
                             --_stat._total_list_entries;
                             break;
                         case entry_type::ENTRY_MAP:
                             --_stat._total_bitmap_entries;
                             break;
                         case entry_type::ENTRY_SET:
                             --_stat._total_set_entries;
                             break;
                         case entry_type::ENTRY_SSET:
                             --_stat._total_zset_entries;
                             break;
                         case entry_type::ENTRY_HLL:
                             --_stat._total_hll_entries;
                             break;
                     }
                 }
             });
        });
    }
    setup_metrics();
}

database::~database()
{
    with_allocator(allocator(), [this] {
        for (size_t i = 0; i < DEFAULT_DB_COUNT; ++i) {
            db_log.info("total {} entries were released in cache [{}]", _cache_stores[i].size(), i);
            _cache_stores[i].flush_all();
        }
    });
}

size_t database::sum_expiring_entries()
{
    size_t sum = 0;
    for (size_t i = 0; i < DEFAULT_DB_COUNT; ++i) {
        sum += _cache_stores[i].expiring_size();
    }
    return sum;
}

void database::setup_metrics()
{
    namespace sm = seastar::metrics;
    _metrics.add_group("db", {
        sm::make_counter("read", [this] { return _stat._read; }, sm::description("Total number of read operations.")),
        sm::make_counter("hit", [this] { return _stat._hit; }, sm::description("Total number of the read hit.")),
        sm::make_counter("total_counter_entries", [this] { return _stat._total_counter_entries; }, sm::description("Total of counter entries.")),
        sm::make_counter("total_string_entries", [this] { return _stat._total_string_entries; }, sm::description("Total of string entries.")),
        sm::make_counter("total_dict_entries", [this] { return _stat._total_dict_entries; }, sm::description("Total of dict entries.")),
        sm::make_counter("total_list_entries", [this] { return _stat._total_list_entries; }, sm::description("Total of list entries.")),
        sm::make_counter("total_set_entries", [this] { return _stat._total_set_entries; }, sm::description("Total of set entries.")),
        sm::make_counter("total_sorted_set_entries", [this] { return _stat._total_zset_entries; }, sm::description("Total of sorted set entries.")),
        sm::make_counter("total_hll_entries", [this] { return _stat._total_hll_entries; }, sm::description("Total of hyperloglog entries.")),
        sm::make_counter("total_expiring_entries", [this] { return sum_expiring_entries(); }, sm::description("Total of expiring entries.")),
    });

    _metrics.add_group("op", {
        sm::make_counter("echo", [this] { return _stat._echo; }, sm::description("ECHO")),
        sm::make_counter("set", [this] { return _stat._set; }, sm::description("SET")),
        sm::make_counter("get", [this] { return _stat._get; }, sm::description("GET")),
        sm::make_counter("del", [this] { return _stat._del; }, sm::description("DEL")),
        sm::make_counter("mset", [this] { return _stat._mset; }, sm::description("MSET")),
        sm::make_counter("mget", [this] { return _stat._mget; }, sm::description("MGET")),
        sm::make_counter("strlen", [this] { return _stat._strlen; }, sm::description("STRLEN")),
        sm::make_counter("exists", [this] { return _stat._exists; }, sm::description("EXISTS")),
        sm::make_counter("append", [this] { return _stat._append; }, sm::description("APPEND")),
        sm::make_counter("lpush", [this] { return _stat._lpush; }, sm::description("LPUSH")),
        sm::make_counter("lpushx", [this] { return _stat._lpushx; }, sm::description("LPUSHX")),
        sm::make_counter("lpop", [this] { return _stat._lpop; }, sm::description("LPOP")),
        sm::make_counter("rpop", [this] { return _stat._rpop; }, sm::description("RPOP")),
        sm::make_counter("lindex", [this] { return _stat._lindex; }, sm::description("LINDEX")),
        sm::make_counter("llen", [this] { return _stat._llen; }, sm::description("LLEN")),
        sm::make_counter("linsert", [this] { return _stat._linsert; }, sm::description("LINSERT")),
        sm::make_counter("lrange", [this] { return _stat._lrange; }, sm::description("LRANGE")),
        sm::make_counter("lset", [this] { return _stat._lset; }, sm::description("LSET")),
        sm::make_counter("ltrim", [this] { return _stat._ltrim; }, sm::description("LTRIM")),
        sm::make_counter("lrem", [this] { return _stat._lrem; }, sm::description("LREM")),
        sm::make_counter("counter", [this] { return _stat._counter; }, sm::description("DECR")),
        sm::make_counter("hdel", [this] { return _stat._hdel; }, sm::description("HDEL")),
        sm::make_counter("hexists", [this] { return _stat._hexists; }, sm::description("HEXISTS")),
        sm::make_counter("hset", [this] { return _stat._hset; }, sm::description("HSET")),
        sm::make_counter("hmset", [this] { return _stat._hmset; }, sm::description("HMSET")),
        sm::make_counter("hincrby", [this] { return _stat._hincrby; }, sm::description("HINCR")),
        sm::make_counter("hincrbyfloat", [this] { return _stat._hincrbyfloat; }, sm::description("HINCRBYFLOAT")),
        sm::make_counter("hlen", [this] { return _stat._hlen; }, sm::description("HLEN")),
        sm::make_counter("hstrlen", [this] { return _stat._hstrlen; }, sm::description("HSTRLEN")),
        sm::make_counter("hgetall", [this] { return _stat._hgetall; }, sm::description("HGETALL")),
        sm::make_counter("hgetallkeys", [this] { return _stat._hgetall_keys; }, sm::description("HGETALLKEYS")),
        sm::make_counter("hgetallvalues", [this] { return _stat._hgetall_values; }, sm::description("HGETALLVALUES")),
        sm::make_counter("hmget", [this] { return _stat._hmget; }, sm::description("HMGET")),
        sm::make_counter("smembers", [this] { return _stat._smembers; }, sm::description("SMEMBERS")),
        sm::make_counter("sadd", [this] { return _stat._sadd; }, sm::description("SADD")),
        sm::make_counter("scard", [this] { return _stat._scard; }, sm::description("SCARD")),
        sm::make_counter("sismember", [this] { return _stat._sismember; }, sm::description("SISMEMBER")),
        sm::make_counter("srem", [this] { return _stat._srem; }, sm::description("SREM")),
        sm::make_counter("sdiff", [this] { return _stat._sdiff; }, sm::description("SDIFF")),
        sm::make_counter("sdiffstore", [this] { return _stat._sdiff_store; }, sm::description("SDIFFSTORE")),
        sm::make_counter("sinter", [this] { return _stat._sinter; }, sm::description("SINTER")),
        sm::make_counter("sinterstore", [this] { return _stat._sinter_store; }, sm::description("SINTERSTORE")),
        sm::make_counter("sunion", [this] { return _stat._sunion; }, sm::description("SUNION")),
        sm::make_counter("sunionstore", [this] { return _stat._sunion_store; }, sm::description("SUNIONSTORE")),
        sm::make_counter("smove", [this] { return _stat._smove; }, sm::description("SMOVE")),
        sm::make_counter("srandmember", [this] { return _stat._srandmember; }, sm::description("SRANDMEMBER")),
        sm::make_counter("spop", [this] { return _stat._spop; }, sm::description("SPOP")),
        sm::make_counter("type", [this] { return _stat._type; }, sm::description("TYPE")),
        sm::make_counter("expire", [this] { return _stat._expire; }, sm::description("EXPIRE")),
        sm::make_counter("pexpire", [this] { return _stat._pexpire; }, sm::description("PEXPIRE")),
        sm::make_counter("ttl", [this] { return _stat._ttl; }, sm::description("TTL")),
        sm::make_counter("pttl", [this] { return _stat._pttl; }, sm::description("PTTL")),
        sm::make_counter("persist", [this] { return _stat._persist; }, sm::description("PERSIST")),
        sm::make_counter("zadd", [this] { return _stat._zadd; }, sm::description("ZADD")),
        sm::make_counter("zcard", [this] { return _stat._zcard; }, sm::description("ZCARD")),
        sm::make_counter("zrange", [this] { return _stat._zrange; }, sm::description("ZRANGE")),
        sm::make_counter("zrangebyscore", [this] { return _stat._zrangebyscore; }, sm::description("ZRANGEBYSCORE")),
        sm::make_counter("zcount", [this] { return _stat._zcount; }, sm::description("ZCOUNT")),
        sm::make_counter("zincrby", [this] { return _stat._zincrby; }, sm::description("ZINCRBY")),
        sm::make_counter("zrank", [this] { return _stat._zrank; }, sm::description("ZRANK")),
        sm::make_counter("zrem", [this] { return _stat._zrem; }, sm::description("ZREM")),
        sm::make_counter("zscore", [this] { return _stat._zscore; }, sm::description("ZSCORE")),
        sm::make_counter("zremrangebyscore", [this] { return _stat._zremrangebyscore; }, sm::description("ZREMRANGEBYSCORE")),
        sm::make_counter("zremrangebyrank", [this] { return _stat._zremrangebyrank; }, sm::description("ZREMRANGEBYRANK")),
        sm::make_counter("zdiffstore", [this] { return _stat._zdiffstore; }, sm::description("ZDIFFSTORE")),
        sm::make_counter("zunionstore", [this] { return _stat._zunionstore; }, sm::description("ZUNIONSTORE")),
        sm::make_counter("zinterstore", [this] { return _stat._zinterstore; }, sm::description("ZINTERSTORE")),
        sm::make_counter("zdiff", [this] { return _stat._zdiff; }, sm::description("ZDIFF")),
        sm::make_counter("zunion", [this] { return _stat._zunion; }, sm::description("ZUNION")),
        sm::make_counter("zinter", [this] { return _stat._zinter; }, sm::description("ZINTER")),
        sm::make_counter("zrangebylex", [this] { return _stat._zrangebylex; }, sm::description("ZRANGEBYLEX")),
        sm::make_counter("zlexcount", [this] { return _stat._zlexcount; }, sm::description("ZLEXCOUNT")),
        sm::make_counter("select", [this] { return _stat._select; }, sm::description("SELECT")),
        sm::make_counter("geoadd", [this] { return _stat._geoadd; }, sm::description("GEOADD")),
        sm::make_counter("geodist", [this] { return _stat._geodist; }, sm::description("GEODIST")),
        sm::make_counter("geohash", [this] { return _stat._geohash; }, sm::description("GEOHASH")),
        sm::make_counter("geopos", [this] { return _stat._geopos; }, sm::description("GEOPOS")),
        sm::make_counter("georadius", [this] { return _stat._georadius; }, sm::description("GEORADIUS")),
        sm::make_counter("setbit", [this] { return _stat._setbit; }, sm::description("SETBIT")),
        sm::make_counter("getbit", [this] { return _stat._getbit; }, sm::description("GETBIT")),
        sm::make_counter("bitcount", [this] { return _stat._bitcount; }, sm::description("BITCOUNT")),
        sm::make_counter("bitop", [this] { return _stat._bitop; }, sm::description("BITOP")),
        sm::make_counter("bitpos", [this] { return _stat._bitpos; }, sm::description("BITPOS")),
        sm::make_counter("bitfield", [this] { return _stat._bitfield; }, sm::description("BITFIELD")),
        sm::make_counter("pfadd", [this] { return _stat._pfadd; }, sm::description("PFADD")),
        sm::make_counter("pfcount", [this] { return _stat._pfcount; }, sm::description("PFCOUNT")),
        sm::make_counter("pfmerge", [this] { return _stat._pfmerge; }, sm::description("PFMERGE")),
     });
}

future<scattered_message_ptr> database::set(const redis_key& rk, bytes& val, long expired, uint32_t flag)
{
    ++_stat._set;
    return with_allocator(allocator(), [this, &rk, &val, expired, flag] {
        auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), val);
        bool result = true;
        if (current_store().insert_if(entry, expired, flag & FLAG_SET_NX, flag & FLAG_SET_XX)) {
            ++_stat._total_string_entries;
        }
        else {
            result = false;
            current_allocator().destroy<cache_entry>(entry);
        }
        return reply_builder::build(result ? msg_ok : msg_nil);
    });
}

future<scattered_message_ptr> database::del(const redis_key& rk)
{
    ++_stat._del;
    return current_store().with_entry_run(rk, [this, &rk] (cache_entry* e) {
        if (!e) return reply_builder::build(msg_zero);
        if (e->type_of_bytes()) {
            --_stat._total_string_entries;
        }
        else if (e->type_of_set()) {
            --_stat._total_set_entries;
        }
        else if (e->type_of_list()) {
            --_stat._total_list_entries;
        }
        else if (e->type_of_map()) {
            --_stat._total_dict_entries;
        }
        else if (e->type_of_sset()) {
            --_stat._total_zset_entries;
        }
        else if (e->type_of_hll()) {
            --_stat._total_hll_entries;
        }
        else {
            --_stat._total_counter_entries;
        }
        auto result =  current_store().erase(*e);
        return reply_builder::build(result ? msg_one : msg_zero);
    });
}

future<scattered_message_ptr> database::get(const redis_key& rk)
{
    ++_stat._read;
    ++_stat._get;
    return current_store().with_entry_run(rk, [this] (const cache_entry* e) {
       if (e && e->type_of_bytes() == false) {
           return reply_builder::build(msg_type_err);
       }
       else {
           if (e != nullptr) ++_stat._hit;
           return reply_builder::build<false, true>(e);
       }
    });
}
future<> database::start()
{
    return make_ready_future<>();
}

future<> database::stop()
{
    return make_ready_future<>();
}
}
