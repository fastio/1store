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
#include "core/metrics.hh"
#include "core/sleep.hh"
#include "structures/hll.hh"
#include "types.hh"
//using logger =  seastar::logger;
//static logger db_log ("db");

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

distributed<database> _databases;

database::database()
    : _stat()
    , _sys_cf(make_lw_shared<store::column_family>("SYSTEM", true))
    , _data_cf(make_lw_shared<store::column_family>("DATA", false))
    , _flush_cache(0)
    , _shutdown(false)
{
    using namespace std::chrono;

    _cache.set_expired_entry_releaser([this] (cache_entry& e) {
         with_allocator(allocator(), [this, &e] {
             auto type = e.type();
             if (_cache.erase(e)) {
                 switch (type) {
                     case data_type::numeric:
                     case data_type::int64:
                         --_stat._total_counter_entries;
                         break;
                     case data_type::bytes:
                         --_stat._total_string_entries;
                         break;
                     case data_type::list:
                         --_stat._total_list_entries;
                         break;
                     case data_type::bitmap:
                         --_stat._total_bitmap_entries;
                         break;
                     case data_type::set:
                         --_stat._total_set_entries;
                         break;
                     case data_type::sset:
                         --_stat._total_zset_entries;
                         break;
                     case data_type::hll:
                         --_stat._total_hll_entries;
                         break;
                     default:
                         break;
                 }
             }
         });
    });
    _commit_log = store::make_commit_log();
    setup_metrics();
}

future<> database::initialize()
{
    assert(_sys_cf);
    assert(_data_cf);
    repeat([this] {
        return _flush_cache.wait().then([this] {
            return _cache.flush_dirty_entry(*_data_cf).then([this] {
                return _shutdown ? stop_iteration::yes : stop_iteration::no;
            });
        });
    });

     _flush_timer.set_callback(std::bind(&database::on_timer, this));
    if (!_shutdown) {
        _flush_timer.arm(std::chrono::milliseconds(8000));
    }
    return make_ready_future<>();
}

void database::on_timer()
{
    if (_cache.should_flush_dirty_entry()) {
        _flush_cache.signal();
    }
    if (!_shutdown) {
        _flush_timer.arm(std::chrono::milliseconds(8000));
    }
}

database::~database()
{
    with_allocator(allocator(), [this] {
        _cache.flush_all();
    });
}

size_t database::sum_expiring_entries()
{
    return _cache.expiring_size();
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

bool database::set_direct(redis_key rk, bytes val, long expired, uint32_t flag)
{
    ++_stat._set;
    return with_allocator(allocator(), [this, rk = std::move(rk), val = std::move(val), expired, flag] {
        auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), val);
        bool result = true;
        if (_cache.insert_if(entry, expired, flag & FLAG_SET_NX, flag & FLAG_SET_XX)) {
            ++_stat._total_string_entries;
        }
        else {
            result = false;
            current_allocator().destroy<cache_entry>(entry);
        }
        return result;
    });
}

future<scattered_message_ptr> database::set(redis_key rk, bytes val, long expired, uint32_t flag)
{
    // First, append the operation commitlog.
    // Second, update the data-structures in the cache, reply message to client.
    // Third, flush dirty data in the cache to memtable periodically.
    // Then, flush memtable to disk when some conditions are statisfied.
    //
    ++_stat._set;
    auto m = make_bytes_mutation(rk.key(), val, expired, flag);
    return _commit_log->append(m).then([this, rk = std::move(rk), val = std::move(val), expired, flag] {
        return with_allocator(allocator(), [this, rk = std::move(rk), val = std::move(val), expired, flag] {
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), val);
            bool result = true;
            if (_cache.insert_if(entry, expired, flag & FLAG_SET_NX, flag & FLAG_SET_XX)) {
                ++_stat._total_string_entries;
            }
            else {
                result = false;
                current_allocator().destroy<cache_entry>(entry);
            }
            return reply_builder::build(result ? msg_ok : msg_nil);
        });
    });
}

future<bool> database::del_direct(redis_key rk)
{
    ++_stat._del;
    auto m = make_deleted_mutation(rk.key());
    return _commit_log->append(m).then([this, rk = std::move(rk)] {
        return _cache.run_with_entry(rk, [this] (cache_entry* e) {
            if (!e) return false;
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
            return with_allocator(allocator(), [this, e] {
                auto result =  _cache.erase(*e);
                return result;
            });
        });
    });
}

future<scattered_message_ptr> database::del(redis_key rk)
{
    ++_stat._del;
    auto m = make_deleted_mutation(rk.key());
    return _commit_log->append(m).then([this, rk = std::move(rk)] {
        return _cache.run_with_entry(rk, [this] (cache_entry* e) {
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
            return with_allocator(allocator(), [this, e] {
                auto result =  _cache.erase(*e);
                return reply_builder::build(result ? msg_one : msg_zero);
            });
        });
    });
}

bool database::exists_direct(redis_key rk)
{
    ++_stat._exists;
    return _cache.exists(rk);
}

future<scattered_message_ptr> database::exists(redis_key rk)
{
    ++_stat._exists;
    auto result = _cache.exists(rk);
    return reply_builder::build(result ? msg_one : msg_zero);
}

future<scattered_message_ptr> database::counter_by(redis_key rk, int64_t step, bool incr)
{
    ++_stat._counter;
    return with_allocator(allocator(), [this, rk = std::move(rk), step, incr] {
        auto e = _cache.find(rk);
        if (!e) {
            // not exists
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), int64_t{step});
            _cache.replace(entry);
            ++_stat._total_counter_entries;
            return reply_builder::build<false, true>(entry);
        }
        if (!e->type_of_integer()) {
            return reply_builder::build(msg_type_err);
        }
        if (incr) {
            e->value_integer_incr(step);
        }
        else {
            e->value_integer_incr(-step);
        }
        return reply_builder::build<false, true>(e);
    });
}

future<scattered_message_ptr> database::append(redis_key rk, bytes val)
{
    ++_stat._append;
    return with_allocator(allocator(), [this, rk = std::move(rk), val = std::move(val)] {
        auto e = _cache.find(rk);
        if (!e) {
            // not exists
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), val);
            _cache.replace(entry);
            ++_stat._total_string_entries;
            return reply_builder::build(val.size());
        }
        if (!e->type_of_bytes()) {
            return reply_builder::build(msg_type_err);
        }
        size_t new_size = e->value_bytes_size() + val.size();
        auto data = std::unique_ptr<bytes_view::value_type[]>(new bytes_view::value_type[new_size]);
        std::copy_n(e->value_bytes_data(), e->value_bytes_size(), data.get());
        std::copy_n(val.data(), val.size(), data.get() + e->value_bytes_size());
        auto new_value = current_allocator().construct<managed_bytes>(data.get(), new_size);
        auto& old_value = e->value_bytes();
        current_allocator().destroy<managed_bytes>(&old_value);
        e->value_bytes() = std::move(*new_value);
        return reply_builder::build(new_size);
    });
}

future<scattered_message_ptr> database::get(redis_key rk)
{
    ++_stat._read;
    ++_stat._get;
    return _cache.run_with_entry(rk, [this] (const cache_entry* e) {
       if (e && e->type_of_bytes() == false) {
           return reply_builder::build(msg_type_err);
       }
       else {
           if (e != nullptr) ++_stat._hit;
           return reply_builder::build<false, true>(e);
       }
    });
}

future<scattered_message_ptr> database::strlen(redis_key rk)
{
    ++_stat._strlen;
    return _cache.run_with_entry(rk, [] (const cache_entry* e) {
        if (e) {
            if (e->type_of_bytes()) {
                return reply_builder::build(e->value_bytes_size());
            }
            return reply_builder::build(msg_type_err);
        }
        return reply_builder::build(msg_zero);
    });
}

future<scattered_message_ptr> database::type(redis_key rk)
{
    ++_stat._type;
    auto e = _cache.find(rk);
    if (!e) {

        return reply_builder::build(msg_type_none);
    }
    auto type = e->type_name();
    return reply_builder::build(type);
}

future<scattered_message_ptr> database::expire(redis_key rk, long expired)
{
    ++_stat._expire;
    auto result = _cache.expire(rk, expired);
    return reply_builder::build(result ? msg_one : msg_zero);
}

future<scattered_message_ptr> database::persist(redis_key rk)
{
    ++_stat._persist;
    auto result = _cache.never_expired(rk);
    return reply_builder::build(result ? msg_one : msg_zero);
}

future<scattered_message_ptr> database::push(redis_key rk, bytes val, bool force, bool left)
{
    left ? ++_stat._lpush : ++_stat._rpush;
    return with_allocator(allocator(), [this, rk = std::move(rk), val = std::move(val), force, left] () {
        auto e = _cache.find(rk);
        if (!e) {
             if (!force) {
                 return reply_builder::build(msg_err);
             }
            // create new list object
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::list_initializer());
            _cache.insert(entry);
            ++_stat._total_list_entries;
            e = entry;
        }
        if (e->type_of_list() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& list = e->value_list();
        left ? list.insert_head(val) : list.insert_tail(val);
        return reply_builder::build(list.size());
    });
}

future<scattered_message_ptr> database::push_multi(redis_key rk, std::vector<bytes> values, bool force, bool left)
{
    left ? ++_stat._lpush : ++_stat._rpush;
    return with_allocator(allocator(), [this, rk = std::move(rk), values = std::move(values), force, left] () {
        auto e = _cache.find(rk);
        if (!e) {
             if (!force) {
                 return reply_builder::build(msg_err);
             }
            // create new list object
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::list_initializer());
            _cache.insert(entry);
            ++_stat._total_list_entries;
            e = entry;
        }
        if (e->type_of_list() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& list = e->value_list();
        for (auto& val : values) {
            left ? list.insert_head(val) : list.insert_tail(val);
        }
        return reply_builder::build(list.size());
    });
}

future<scattered_message_ptr> database::pop(redis_key rk, bool left)
{
    ++_stat._read;
    left ? ++_stat._lpop : ++_stat._rpop;
    return with_allocator(allocator(), [this, rk = std::move(rk), left] () {
        auto e = _cache.find(rk);
        if (!e) {
            return reply_builder::build(msg_nil);
        }
        if (e->type_of_list() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& list = e->value_list();
        assert(!list.empty());
        auto reply = reply_builder::build(left ? list.front() : list.back());
        left ? list.pop_front() : list.pop_back();
        if (list.empty()) {
           --_stat._total_list_entries;
           _cache.erase(rk);
        }
        ++_stat._hit;
        return reply;
    });
}

future<scattered_message_ptr> database::llen(redis_key rk)
{
    ++_stat._llen;
    auto e = _cache.find(rk);
    if (!e) {
        return reply_builder::build(msg_zero);
    }
    if (e->type_of_list() == false) {
        return reply_builder::build(msg_type_err);
    }
    auto& list = e->value_list();
    return reply_builder::build(list.size());
}

future<scattered_message_ptr> database::lindex(redis_key rk, long idx)
{
    ++_stat._read;
    ++_stat._lindex;
    return _cache.run_with_entry(rk, [this, rk = std::move(rk), idx] (const cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_nil);
        }
        if (e->type_of_list() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& list = e->value_list();
        auto index = alignment_index_base_on(list.size(), idx);
        if (list.index_out_of_range(index)) {
            return reply_builder::build(msg_nil);
        }
        auto& result = list.at(static_cast<size_t>(index));
        ++_stat._hit;
        return reply_builder::build(result);
    });
}

future<scattered_message_ptr> database::lrange(redis_key rk, long start, long end)
{
    ++_stat._read;
    ++_stat._lrange;
    auto e = _cache.find(rk);
    if (!e) {
        return reply_builder::build(msg_err);
    }
    if (e->type_of_list() == false) {
        return reply_builder::build(msg_type_err);
    }
    auto& list = e->value_list();
    start = database::alignment_index_base_on(list.size(), start);
    end = database::alignment_index_base_on(list.size(), end);
    if (start < 0) start = 0;
    if (end >= static_cast<long>(list.size())) end = static_cast<size_t>(list.size()) - 1;
    std::vector<const managed_bytes*> data;
    if (start < end) {
       for (auto i = start; i < end; ++i) {
          const auto& b = list.at(static_cast<size_t>(i));
          data.push_back(&b);
       }
    }
    if (!data.empty()) ++_stat._hit;
    return reply_builder::build(data);
}

future<scattered_message_ptr> database::lrem(redis_key rk, long count, bytes val)
{
    ++_stat._lrem;
    return with_allocator(allocator(), [this, rk = std::move(rk), count, val = std::move(val)] {
        auto e = _cache.find(rk);
        if (!e) {
            return reply_builder::build(msg_err);
        }
        if (e->type_of_list() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& list = e->value_list();
        size_t removed = 0;
        if (count == 0) removed = list.trem<true, true>(val, count);
        else if (count > 0) removed = list.trem<false, true>(val, count);
        else removed = list.trem<false, false>(val, count);
        if (list.empty()) {
            --_stat._total_list_entries;
            _cache.erase(rk);
        }
        return reply_builder::build(removed);
    });
}

future<scattered_message_ptr> database::linsert(redis_key rk, bytes pivot, bytes val, bool after)
{
    ++_stat._linsert;
    return with_allocator(allocator(), [this, rk = std::move(rk), pivot = std::move(pivot), val = std::move(val), after] {
        auto e = _cache.find(rk);
        if (!e) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_list() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& list = e->value_list();
        auto index = list.index_of(pivot);
        if (list.index_out_of_range(index)) {
            return reply_builder::build(msg_zero);
        }
        if (after) {
            if (index == list.size()) list.insert_tail(val);
            else  list.insert_at(++index, val);
        }
        else {
           if (index == 0) list.insert_head(val);
           else list.insert_at(--index, val);
        }
        return reply_builder::build(msg_one);
    });
}

future<scattered_message_ptr> database::lset(redis_key rk, long idx, bytes val)
{
    ++_stat._lset;
    return with_allocator(allocator(), [this, rk = std::move(rk), idx, val = std::move(val)] {
        auto e = _cache.find(rk);
        if (!e) {
            return reply_builder::build(msg_nokey_err);
        }
        if (e->type_of_list() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& list = e->value_list();
        auto nidx = database::alignment_index_base_on(list.size(), idx);
        if (list.index_out_of_range(nidx)) {
            return reply_builder::build(msg_out_of_range_err);
        }
        auto& old = list.at(static_cast<size_t>(nidx));
        current_allocator().destroy<managed_bytes>(&old);
        auto entry = current_allocator().construct<managed_bytes>(bytes_view{val});
        list.at(idx) = std::move(*entry);
        return reply_builder::build(msg_ok);
        
    });
}

future<scattered_message_ptr> database::ltrim(redis_key rk, long start, long end)
{
    ++_stat._ltrim;
    return with_allocator(allocator(), [this, rk = std::move(rk), start, end] {
        auto e = _cache.find(rk);
        if (!e) {
            return reply_builder::build(msg_ok);
        }
        if (e->type_of_list() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& list = e->value_list();
        auto nstart = database::alignment_index_base_on(list.size(), start);
        if (nstart < 0) nstart = 0;
        auto nend = database::alignment_index_base_on(list.size(), end);
        if (nstart > nend || nstart > static_cast<long>(list.size()) || nend < 0) {
            list.clear();
        }
        list.trim(static_cast<size_t>(nstart), static_cast<size_t>(nend));
        if (list.empty()) {
            --_stat._total_list_entries;
            _cache.erase(rk);
        }
        return reply_builder::build(msg_ok);
        
    });
}

future<scattered_message_ptr> database::hset(redis_key rk, bytes key, bytes val)
{
    ++_stat._hset;
    return with_allocator(allocator(), [this, rk = std::move(rk), key = std::move(key), val = std::move(val)] {
        auto e = _cache.find(rk);
        if (!e) {
            // the rk was not exists, then create it.
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::dict_initializer());
            _cache.insert(entry);
            ++_stat._total_dict_entries;
            e = entry;
        }
        if (e->type_of_map() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& map = e->value_map();
        bool exists = map.exists(key);
        auto entry = current_allocator().construct<dict_entry>(key, val);
        map.insert(entry);
        return reply_builder::build(exists ? msg_zero : msg_one);
        
    });
}

future<scattered_message_ptr> database::hincrby(redis_key rk, bytes key, int64_t delta)
{
    ++_stat._hincrby;
    return with_allocator(allocator(), [this, rk = std::move(rk), key = std::move(key), delta] {
        auto e = _cache.find(rk);
        if (!e) {
            // the rk was not exists, then create it.
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::dict_initializer());
            _cache.insert(entry);
            ++_stat._total_dict_entries;
            e = entry;
        }
        if (e->type_of_map() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& map = e->value_map();
        return map.run_with_entry(key, [&map, &key, delta] (dict_entry* d) {
            if (!d) {
                auto entry = current_allocator().construct<dict_entry>(key, delta);
                map.insert(entry);
                return reply_builder::build<false, true>(entry);
            }
            if (!d->type_of_integer()) {
                return reply_builder::build(msg_not_integer_err);
            }
            d->value_integer_incr(delta);
            return reply_builder::build<false, true>(d);
        });
        
    });
}

future<scattered_message_ptr> database::hincrbyfloat(redis_key rk, bytes key, double delta)
{
    ++_stat._hincrbyfloat;
    return with_allocator(allocator(), [this, rk = std::move(rk), key = std::move(key), delta] {
        auto e = _cache.find(rk);
        if (!e) {
            // the rk was not exists, then create it.
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::dict_initializer());
            _cache.insert(entry);
            ++_stat._total_dict_entries;
            e = entry;
        }
        if (e->type_of_map() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& map = e->value_map();
        return map.run_with_entry(key, [&map, &key, delta] (dict_entry* d) {
            if (!d) {
                auto entry = current_allocator().construct<dict_entry>(key, delta);
                map.insert(entry);
                return reply_builder::build<false, true>(entry);
            }
            if (!d->type_of_float()) {
                return reply_builder::build(msg_not_float_err);
            }
            d->value_float_incr(delta);
            return reply_builder::build<false, true>(d);
        });
    });
}

future<scattered_message_ptr> database::hmset(redis_key rk, std::unordered_map<bytes, bytes> kvs)
{
    ++_stat._hmset;
    return with_allocator(allocator(), [this, rk = std::move(rk), kvs = std::move(kvs)] {
        auto e = _cache.find(rk);
        if (!e) {
            // the rk was not exists, then create it.
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::dict_initializer());
            _cache.insert(entry);
            ++_stat._total_dict_entries;
            e = entry;
        }
        if (e->type_of_map() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& map = e->value_map();
        bool result = false;
        for (auto& kv : kvs) {
           auto entry = current_allocator().construct<dict_entry>(kv.first, kv.second);
           result = map.insert(entry);
           if (!result) break;
        }
        return reply_builder::build(result ? msg_ok : msg_err);
    });
}

future<scattered_message_ptr> database::hget(redis_key rk, bytes key)
{
    ++_stat._read;
    ++_stat._hget;
    auto e = _cache.find(rk);
    if (!e) {
        return reply_builder::build(msg_err);
    }
    if (e->type_of_map() == false) {
        return reply_builder::build(msg_type_err);
    }
    auto& map = e->value_map();
    return map.run_with_entry(key, [this] (const dict_entry* d) {
        if (d) ++_stat._hit;
        return reply_builder::build<false, true>(d);
    });
}

future<scattered_message_ptr> database::hdel_multi(redis_key rk, std::vector<bytes> keys)
{
    ++_stat._hdel;
    return with_allocator(allocator(), [this, rk = std::move(rk), keys = std::move(keys)] {
        auto e = _cache.find(rk);
        if (!e) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_map() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& map = e->value_map();
        size_t removed = 0;
        for (auto& key : keys) {
            if (map.erase(key)) {
                ++ removed;
            }
        }
        if (map.empty()) {
            --_stat._total_dict_entries;
            _cache.erase(rk);
        }
        return reply_builder::build(removed);
    });
}


future<scattered_message_ptr> database::hdel(redis_key rk, bytes key)
{
    ++_stat._hdel;
    return with_allocator(allocator(), [this, rk = std::move(rk), key = std::move(key)] {
        auto e = _cache.find(rk);
        if (!e) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_map() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& map = e->value_map();
        bool exists = map.erase(key);
        if (map.empty()) {
            --_stat._total_dict_entries;
            _cache.erase(rk);
        }
        return reply_builder::build(exists ? msg_ok : msg_err);
    });
}

future<scattered_message_ptr> database::hexists(redis_key rk, bytes key)
{
    ++_stat._hexists;
    return with_allocator(allocator(), [this, rk = std::move(rk), key = std::move(key)] {
        auto e = _cache.find(rk);
        if (!e) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_map() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& map = e->value_map();
        auto result = map.exists(key);
        return reply_builder::build(result ? msg_one : msg_zero);
    });
}

future<scattered_message_ptr> database::hstrlen(redis_key rk, bytes key)
{
    ++_stat._hstrlen;
    auto e = _cache.find(rk);
    if (!e) {
        return reply_builder::build(msg_zero);
    }
    if (e->type_of_map() == false) {
        return reply_builder::build(msg_type_err);
    }
    auto& map = e->value_map();
    return map.run_with_entry(key, [] (const dict_entry* d) {
        if (!d) {
            return reply_builder::build(msg_zero);
        }
        return reply_builder::build(d->value_bytes_size());
    });
}

future<scattered_message_ptr> database::hlen(redis_key rk)
{
    ++_stat._hlen;
    auto e = _cache.find(rk);
    if (!e) {
        return reply_builder::build(msg_zero);
    }
    if (e->type_of_map() == false) {
        return reply_builder::build(msg_type_err);
    }
    auto& map = e->value_map();
    return reply_builder::build(map.size());
}

future<scattered_message_ptr> database::hgetall(redis_key rk)
{
    return hgetall_impl<true, true>(rk);
}

future<scattered_message_ptr> database::hgetall_values(redis_key rk)
{
    return hgetall_impl<false, true>(rk);
}

future<scattered_message_ptr> database::hgetall_keys(redis_key rk)
{
    return hgetall_impl<true, false>(rk);
}


future<scattered_message_ptr> database::hmget(redis_key rk, std::vector<bytes> keys)
{
    ++_stat._read;
    ++_stat._hmget;
    auto e = _cache.find(rk);
    if (!e) {
        return reply_builder::build(msg_err);
    }
    if (e->type_of_map() == false) {
        return reply_builder::build(msg_type_err);
    }
    auto& map = e->value_map();
    std::vector<const dict_entry*> entries;
    map.fetch(keys, entries);
    if (!entries.empty()) ++_stat._hit;
    return reply_builder::build<false, true>(entries);
}

future<scattered_message_ptr> database::srandmember(redis_key rk, size_t count)
{
    ++_stat._read;
    ++_stat._srandmember;
    auto e = _cache.find(rk);
    std::vector<const dict_entry*> result;
    if (!e) {
        return reply_builder::build<true, false>(result);
    }
    if (e->type_of_set() == false) {
        return reply_builder::build(msg_type_err);
    }
    auto& set = e->value_set();
    count = std::min(count, set.size());
    for (size_t i = 0; i < count; ++i) {
        auto index = rand_generater::rand_less_than(set.size());
        auto entry = set.at(index);
        result.push_back(&(*entry));
    }
    if (!result.empty()) ++_stat._hit;
    return reply_builder::build<true, false>(result);
}

future<scattered_message_ptr> database::sadds(redis_key rk, std::vector<bytes> members)
{
    ++_stat._sadd;
    return with_allocator(allocator(), [this, rk = std::move(rk), members = std::move(members)] {
        auto o = _cache.find(rk);
        if (!o) {
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::set_initializer());
            _cache.insert(entry);
            ++_stat._total_set_entries;
            o = entry;
        }
        if (o->type_of_set() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& set = o->value_set();
        size_t inserted = 0;
        for (auto& member : members) {
            if (set.insert(current_allocator().construct<dict_entry>(member))) {
                inserted++;
            }
        }
        return reply_builder::build(inserted);
    });
}

bool database::sadd_direct(redis_key rk, bytes member)
{
    ++_stat._sadd;
    return with_allocator(allocator(), [this, rk = std::move(rk), member = std::move(member)] {
        auto o = _cache.find(rk);
        if (!o) {
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::set_initializer());
            _cache.insert(entry);
            ++_stat._total_set_entries;
            o = entry;
        }
        if (o->type_of_set() == false) {
            return false;
        }
        auto& set = o->value_set();
        set.insert(current_allocator().construct<dict_entry>(member));
        return true;
    });
}

bool database::sadds_direct(redis_key rk, std::vector<bytes> members)
{
    ++_stat._sadd;
    return with_allocator(allocator(), [this, rk = std::move(rk), members = std::move(members)] {
        auto o = _cache.find(rk);
        if (!o) {
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::set_initializer());
            _cache.insert(entry);
            ++_stat._total_set_entries;
            o = entry;
        }
        if (o->type_of_set() == false) {
            return false;
        }
        auto& set = o->value_set();
        size_t inserted = 0;
        for (auto& member : members) {
            if (set.insert(current_allocator().construct<dict_entry>(member))) {
                inserted++;
            }
        }
        return true;
    });
}

future<scattered_message_ptr> database::scard(redis_key rk)
{
    ++_stat._scard;
    auto e = _cache.find(rk);
    if (!e) {
        return reply_builder::build(msg_zero);
    }
    if (e->type_of_set() == false) {
        return reply_builder::build(msg_type_err);
    }
    auto& set = e->value_set();
    return reply_builder::build(set.size());
}

future<scattered_message_ptr> database::sismember(redis_key rk, bytes member)
{
    ++_stat._sismember;
    auto e = _cache.find(rk);
    if (!e) {
        return reply_builder::build(msg_zero);
    }
    if (e->type_of_set() == false) {
        return reply_builder::build(msg_type_err);
    }
    auto& set = e->value_set();
    auto result = set.exists(member);
    return reply_builder::build(result ? msg_one : msg_zero);
}

future<scattered_message_ptr> database::smembers(redis_key rk)
{
    ++_stat._read;
    ++_stat._smembers;
    return _cache.run_with_entry(rk, [this] (const cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_nil);
        }
        if (e->type_of_set() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& set = e->value_set();
        std::vector<const dict_entry*> entries;
        set.fetch(entries);
        if (!entries.empty()) ++_stat._hit;
        return reply_builder::build<true, false>(entries);
    });
}

future<foreign_ptr<lw_shared_ptr<bytes>>> database::get_hll_direct(redis_key rk)
{
    using return_type = foreign_ptr<lw_shared_ptr<bytes>>;
    auto e = _cache.find(rk);
    if (!e || e->type_of_hll() == false) {
        return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<bytes>>(nullptr));
    }
    auto data = e->value_bytes_data();
    auto size = e->value_bytes_size();
    ++_stat._hit;
    return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<bytes>>(make_lw_shared<bytes>(bytes {data, size})));
}

future<foreign_ptr<lw_shared_ptr<bytes>>> database::get_direct(redis_key rk)
{
    ++_stat._read;
    ++_stat._get;
    using return_type = foreign_ptr<lw_shared_ptr<bytes>>;
    auto e = _cache.find(rk);
    if (!e || e->type_of_bytes() == false) {
        return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<bytes>>(nullptr));
    }
    auto data = e->value_bytes_data();
    auto size = e->value_bytes_size();
    ++_stat._hit;
    return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<bytes>>(make_lw_shared<bytes>(bytes {data, size})));
}

future<foreign_ptr<lw_shared_ptr<std::vector<bytes>>>> database::smembers_direct(redis_key rk)
{
    ++_stat._read;
    ++_stat._smembers;
    using result_type = std::vector<bytes>;
    using return_type = foreign_ptr<lw_shared_ptr<result_type>>;
    return _cache.run_with_entry(rk, [this] (const cache_entry* e) {
        if (!e || e->type_of_set() == false) {
            return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<result_type>>(make_lw_shared<result_type>(result_type {})));
        }
        auto& set = e->value_set();
        result_type keys;
        set.fetch_keys(keys);
        if (!keys.empty()) ++_stat._hit;
        return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<result_type>>(make_lw_shared<result_type>(std::move(keys))));
    });
}

future<scattered_message_ptr> database::spop(redis_key rk, size_t count)
{
    ++_stat._read;
    ++_stat._spop;
    return with_allocator(allocator(), [this, rk = std::move(rk), &count] {
        auto e = _cache.find(rk);
        if (!e) {
            return reply_builder::build(msg_nil);
        }
        if (e->type_of_set() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& set = e->value_set();
        std::vector<dict_lsa::const_iterator> removed;
        std::vector<const dict_entry*> entries;
        count = std::min(count, set.size());
        for (size_t i = 0; i < count; ++i) {
            auto index = rand_generater::rand_less_than(set.size());
            auto entry = set.at(index);
            entries.push_back(&(*entry));
            removed.push_back(entry);
        }
        auto reply = reply_builder::build<true, false>(entries);
        if (!removed.empty()) {
            for (auto it : removed) {
                set.erase(it);
            }
            if (set.empty()) {
                --_stat._total_set_entries;
                _cache.erase(rk);
            }
        }
        return reply;
    });
}

future<scattered_message_ptr> database::srem(redis_key rk, bytes member)
{
    ++_stat._srem;
    return with_allocator(allocator(), [this, rk = std::move(rk), member = std::move(member)] {
        auto e = _cache.find(rk);
        if (!e) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_set() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& set = e->value_set();
        auto result = set.erase(member);
        if (set.empty()) {
            --_stat._total_set_entries;
            _cache.erase(rk);
        }
        return reply_builder::build(result ? msg_one : msg_zero);
    });
}
bool database::srem_direct(redis_key rk, bytes member)
{
    ++_stat._srem;
    return with_allocator(allocator(), [this, rk = std::move(rk), member = std::move(member)] {
        auto e = _cache.find(rk);
        if (!e) {
            return true;
        }
        if (e->type_of_set() == false) {
            return false;
        }
        auto& set = e->value_set();
        bool result = set.erase(member);
        if (set.empty()) {
            --_stat._total_set_entries;
            _cache.erase(rk);
        }
        return result;
    });
}

future<scattered_message_ptr> database::srems(redis_key rk, std::vector<bytes> members)
{
    ++_stat._srem;
    return with_allocator(allocator(), [this, rk = std::move(rk), members = std::move(members)] {
        auto e = _cache.find(rk);
        if (!e) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_set() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& set = e->value_set();
        size_t removed = 0;
        for (auto& member : members) {
            if (set.erase(member)) {
                removed++;
            }
        }
        if (set.empty()) {
            --_stat._total_set_entries;
            _cache.erase(rk);
        }
        return reply_builder::build(removed);
    });
}

future<scattered_message_ptr> database::pttl(redis_key rk)
{
    ++_stat._pttl;
    return _cache.run_with_entry(rk, [this] (const cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_neg_two);
        }
        if (!e->ever_expires()) {
            return reply_builder::build(msg_neg_one);
        }
        return reply_builder::build(e->time_of_live());
    });
}

future<scattered_message_ptr> database::ttl(redis_key rk)
{
    ++_stat._ttl;
    return _cache.run_with_entry(rk, [this] (const cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_neg_two);
        }
        if (!e->ever_expires()) {
            return reply_builder::build(msg_neg_one);
        }
        return reply_builder::build(e->time_of_live() / 1000);
    });
}

future<scattered_message_ptr> database::zadds(redis_key rk, std::unordered_map<bytes, double> members, int flags)
{
    ++_stat._zadd;
    return with_allocator(allocator(), [this, rk = std::move(rk), members = std::move(members), flags] {
        auto o = _cache.find(rk);
        if (o == nullptr) {
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::sset_initializer());
            _cache.insert(entry);
            ++_stat._total_zset_entries;
            o = entry;
        }
        if (o->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& sset = o->value_sset();
        size_t inserted = 0;
        if (flags & ZADD_NX) {
            inserted = sset.insert_if_not_exists(members);
        }
        else if (flags & ZADD_XX) {
            inserted = sset.update_if_only_exists(members);
        }
        else if (flags & ZADD_CH) {
            inserted = sset.insert_or_update(members);
        }
        else {
            // FIXME: RETURN ERROR MESSAGE
            assert(false);
        }
        return reply_builder::build(inserted);
    });
}

bool database::zadds_direct(redis_key rk, std::unordered_map<bytes, double> members, int flags)
{
    ++_stat._zadd;
    return with_allocator(allocator(), [this, rk = std::move(rk), members = std::move(members), flags] {
        auto o = _cache.find(rk);
        if (o == nullptr) {
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::sset_initializer());
            _cache.insert(entry);
            ++_stat._total_zset_entries;
            o = entry;
        }
        if (o->type_of_sset() == false) {
            return false;
        }
        auto& sset = o->value_sset();
        size_t inserted = 0;
        if (flags & ZADD_NX) {
            inserted = sset.insert_if_not_exists(members);
        }
        else if (flags & ZADD_XX) {
            inserted = sset.update_if_only_exists(members);
        }
        else if (flags & ZADD_CH) {
            inserted = sset.insert_or_update(members);
        }
        else {
            assert(false);
        }
        return inserted > 0;
    });
}


future<scattered_message_ptr> database::zcard(redis_key rk)
{
    ++_stat._zcard;
    return _cache.run_with_entry(rk, [] (const cache_entry* e) {
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        return reply_builder::build(sset.size());
    });
}

future<scattered_message_ptr> database::zrem(redis_key rk, std::vector<bytes> members)
{
    ++_stat._zrem;
    return with_allocator(allocator(), [this, rk = std::move(rk), members = std::move(members)] {
        auto e = _cache.find(rk);
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        auto removed = sset.erase(members);
        if (sset.empty()) {
           --_stat._total_zset_entries;
           _cache.erase(rk);
        }
        return reply_builder::build(removed);
    });
}

future<scattered_message_ptr> database::zcount(redis_key rk, double min, double max)
{
    ++_stat._zcount;
    return _cache.run_with_entry(rk, [min, max] (const cache_entry* e) {
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        auto count = sset.count_by_score(min, max);
        return reply_builder::build(count);
    });
}

future<scattered_message_ptr> database::zincrby(redis_key rk, bytes member, double delta)
{
    ++_stat._zincrby;
    return with_allocator(allocator(), [this, rk = std::move(rk), member = std::move(member), delta] {
        auto o = _cache.find(rk);
        if (o == nullptr) {
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::sset_initializer());
            _cache.insert(entry);
            ++_stat._total_zset_entries;
            o = entry;
        }
        if (o->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& sset = o->value_sset();
        auto result = sset.insert_or_update(member, delta);
        return reply_builder::build(result);
    });
}

future<foreign_ptr<lw_shared_ptr<std::vector<std::pair<bytes, double>>>>> database::zrange_direct(redis_key rk, long begin, long end)
{
    ++_stat._read;
    ++_stat._zrange;
    using return_type = foreign_ptr<lw_shared_ptr<std::vector<std::pair<bytes, double>>>>;
    using result_type = std::vector<std::pair<bytes, double>>;
    return _cache.run_with_entry(rk, [this, begin, end] (const cache_entry* e) {
        if (e == nullptr || e->type_of_sset() == false) {
            return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<result_type>>(make_lw_shared<result_type>(result_type {})));
        }
        auto& sset = e->value_sset();
        result_type entries {};
        sset.fetch_by_rank(begin, end, entries);
        if (!entries.empty()) ++_stat._hit;
        return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<result_type>>(make_lw_shared<result_type>(std::move(entries))));
    });
}

future<scattered_message_ptr> database::zrange(redis_key rk, long begin, long end, bool reverse, bool with_score)
{
    ++_stat._read;
    ++_stat._zrange;
    return _cache.run_with_entry(rk, [this, begin, end, reverse, with_score] (const cache_entry* e) {
        std::vector<const sset_entry*> entries;
        if (e == nullptr) {
            return reply_builder::build(entries, with_score);
        }
        if (e->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        sset.fetch_by_rank(begin, end, entries);
        if (reverse) {
           std::reverse(std::begin(entries), std::end(entries));
        }
        if (!entries.empty()) ++_stat._hit;
        return reply_builder::build(entries, with_score);
    });
}

future<scattered_message_ptr> database::zrangebyscore(redis_key rk, double min, double max, bool reverse, bool with_score)
{
    ++_stat._read;
    ++_stat._zrangebyscore;
    return _cache.run_with_entry(rk, [this, min, max, reverse, with_score] (const cache_entry* e) {
        std::vector<const sset_entry*> entries;
        if (e == nullptr) {
            return reply_builder::build(entries, with_score);
        }
        if (e->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        sset.fetch_by_score(min, max, entries);
        if (reverse) {
           std::reverse(std::begin(entries), std::end(entries));
        }
        if (!entries.empty()) ++_stat._hit;
        return reply_builder::build(entries, with_score);
    });
}

future<scattered_message_ptr> database::zrank(redis_key rk, bytes member, bool reverse)
{
    ++_stat._read;
    ++_stat._zrank;
    auto e = _cache.find(rk);
    if (e == nullptr) {
        return reply_builder::build(msg_nil);
    }
    if (e->type_of_sset() == false) {
        return reply_builder::build(msg_type_err);
    }
    auto& sset = e->value_sset();
    auto rank_opt = sset.rank(member);
    if (rank_opt) {
       auto rank = *rank_opt;
       if (reverse) {
           rank = sset.size() - rank;
       }
       ++_stat._hit;
       return reply_builder::build(rank);
    }
    return reply_builder::build(msg_nil);
}

future<scattered_message_ptr> database::zscore(redis_key rk, bytes member)
{
    ++_stat._read;
    ++_stat._zscore;
    auto e = _cache.find(rk);
    if (e == nullptr) {
        return reply_builder::build(msg_zero);
    }
    if (e->type_of_sset() == false) {
        return reply_builder::build(msg_type_err);
    }
    auto& sset = e->value_sset();
    auto score_opt = sset.score(member);
    if (score_opt) {
       return reply_builder::build(*score_opt);
       ++_stat._hit;
    }
    return reply_builder::build(msg_err);
}

future<scattered_message_ptr> database::zremrangebyscore(redis_key rk, double min, double max)
{
    ++_stat._zremrangebyscore;
    return with_allocator(allocator(), [this, rk = std::move(rk), min, max] {
        auto e = _cache.find(rk);
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        std::vector<const sset_entry*> entries;
        auto& sset = e->value_sset();
        sset.fetch_by_score(min, max, entries);
        auto removed = sset.erase(entries);
        if (sset.empty()) {
            --_stat._total_zset_entries;
            _cache.erase(rk);
        }
        return reply_builder::build(removed);
    });
}

future<scattered_message_ptr> database::zremrangebyrank(redis_key rk, size_t begin, size_t end)
{
    ++_stat._zremrangebyrank;
    return with_allocator(allocator(), [this, rk = std::move(rk), begin, end] {
        auto e = _cache.find(rk);
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        std::vector<const sset_entry*> entries;
        auto& sset = e->value_sset();
        sset.fetch_by_rank(begin, end, entries);
        auto removed = sset.erase(entries);
        if (sset.empty()) {
            --_stat._total_zset_entries;
            _cache.erase(rk);
        }
        return reply_builder::build(removed);
    });
}

bool database::select(size_t index)
{
    (void) index;
    return true;
}


future<scattered_message_ptr> database::geodist(redis_key rk, bytes lpos, bytes rpos, int flag)
{
    ++_stat._read;
    ++_stat._geodist;
    double factor = 1;
    if (flag & GEODIST_UNIT_M) {
        factor = 1;
    }
    else if (flag & GEODIST_UNIT_KM) {
        factor = 1000;
    }
    else if (flag & GEODIST_UNIT_MI) {
        factor = 0.3048;
    }
    else if (flag & GEODIST_UNIT_FT) {
        factor = 1609.34;
    }
    auto e = _cache.find(rk);
    if (e == nullptr) {
       return reply_builder::build(msg_err);
    }
    if (e->type_of_sset() == false) {
       return reply_builder::build(msg_type_err);
    }
    auto& sset = e->value_sset();
    auto l_score_opt = sset.score(lpos);
    auto r_score_opt = sset.score(rpos);
    if (!l_score_opt || !r_score_opt) {
       return reply_builder::build(msg_err);
    }
    double dist = 0;
    if (geo::dist(*l_score_opt, *r_score_opt, dist)) {
       ++_stat._hit;
       return reply_builder::build(dist / factor);
    }
    else {
       return reply_builder::build(msg_err);
    }
}

future<scattered_message_ptr> database::geohash(redis_key rk, std::vector<bytes> members)
{
    ++_stat._read;
    ++_stat._geohash;
    auto e = _cache.find(rk);
    if (e == nullptr) {
       return reply_builder::build(msg_err);
    }
    if (e->type_of_sset() == false) {
       return reply_builder::build(msg_type_err);
    }
    auto& sset = e->value_sset();
    std::vector<const sset_entry*> entries;
    sset.fetch_by_key(members, entries);
    std::vector<bytes> geohash_set;
    for (size_t i = 0; i < entries.size(); ++i) {
        auto entry = entries[i];
        bytes hashstr;
        if (geo::encode_to_geohash_string(entry->score(), hashstr) == false) {
            return reply_builder::build(msg_err);
        }
        geohash_set.emplace_back(std::move(hashstr));;
    }
    if (!geohash_set.empty()) ++_stat._hit;
    return reply_builder::build(geohash_set);
}

future<scattered_message_ptr> database::geopos(redis_key rk, std::vector<bytes> members)
{
    ++_stat._read;
    ++_stat._geopos;
    auto e = _cache.find(rk);
    if (e == nullptr) {
       return reply_builder::build(msg_err);
    }
    if (e->type_of_sset() == false) {
       return reply_builder::build(msg_type_err);
    }
    auto& sset = e->value_sset();
    std::vector<const sset_entry*> entries;
    sset.fetch_by_key(members, entries);
    std::vector<bytes> geohash_set;
    for (size_t i = 0; i < entries.size(); ++i) {
        auto entry = entries[i];
        bytes hashstr;
        if (geo::encode_to_geohash_string(entry->score(), hashstr) == false) {
            return reply_builder::build(msg_err);
        }
        geohash_set.emplace_back(std::move(hashstr));;
    }
    if (!geohash_set.empty()) ++_stat._hit;
    return reply_builder::build(geohash_set);
}

using georadius_result_type = std::pair<std::vector<std::tuple<bytes, double, double, double, double>>, int>;
future<foreign_ptr<lw_shared_ptr<georadius_result_type>>> database::georadius_coord_direct(redis_key rk, double longitude, double latitude, double radius, size_t count, int flag)
{
    using return_type = foreign_ptr<lw_shared_ptr<georadius_result_type>>;
    return _cache.run_with_entry(rk, [this, longitude, latitude, radius, count, flag] (const cache_entry* e) {
        if (e == nullptr) {
            return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_ERR})));
        }
        if (e->type_of_sset() == false) {
            return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_WRONG_TYPE})));
        }
        auto& sset = e->value_sset();
        return georadius(sset, longitude, latitude, radius, count, flag);
    });
}

future<foreign_ptr<lw_shared_ptr<georadius_result_type>>> database::georadius_member_direct(redis_key rk, bytes pos, double radius, size_t count, int flag)
{

    using return_type = foreign_ptr<lw_shared_ptr<georadius_result_type>>;
    auto e = _cache.find(rk);
    if (e == nullptr) {
        return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_ERR})));
    }
    if (e->type_of_sset() == false) {
        return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_WRONG_TYPE})));
    }
    auto& sset = e->value_sset();
    auto score_opt = sset.score(pos);
    if (!score_opt) {
        return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_ERR})));
    }
    double longitude = 0, latitude = 0;
    if (geo::decode_from_geohash(*score_opt, longitude, latitude) == false) {
        return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_ERR})));
    }
    return georadius(sset, longitude, latitude, radius, count, flag);
}

future<foreign_ptr<lw_shared_ptr<georadius_result_type>>> database::georadius(const sset_lsa& sset, double longitude, double latitude, double radius, size_t count, int flag)
{
    ++_stat._read;
    ++_stat._georadius;
    using return_type = foreign_ptr<lw_shared_ptr<georadius_result_type>>;
    using data_type = std::vector<std::tuple<bytes, double, double, double, double>>;
    data_type points;
    auto fetch_point = [&sset, count] (double min, double max, double log, double lat, double r, data_type& points) -> size_t {
        std::vector<const sset_entry*> entries;
        sset.fetch_by_score(min, max, entries, count);
        size_t _count = 0;
        for (size_t i = 0; i < entries.size(); ++i) {
            auto e = entries[i];
            double score = e->score(), longitude = 0, latitude = 0, dist = 0;
            if (geo::decode_from_geohash(score, longitude, latitude) == false) {
                continue;
            }
            if (geo::dist(log, lat, longitude, latitude, dist) == false) {
                continue;
            }
            if (dist < r) {
                _count++;
                bytes n(e->key_data(), e->key_size());
                points.emplace_back(std::move(std::tuple<bytes, double, double, double, double>{std::move(n), score, dist, longitude, latitude}));
            }
        }
        return _count;
    };
    if (geo::fetch_points_from_location(longitude, latitude, radius, std::move(fetch_point), points) == false) {
        return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_ERR})));
    }
    if (flag & GEORADIUS_ASC) {
        std::sort(points.begin(), points.end(), [] (const auto& l, const auto& r) { return std::get<2>(l) > std::get<2>(r); });
    }
    else if (flag & GEORADIUS_DESC) {
        std::sort(points.begin(), points.end(), [] (const auto& l, const auto& r) { return std::get<2>(l) < std::get<2>(r); });
    }
    if (!points.empty()) ++_stat._hit;
    return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {std::move(points), REDIS_OK})));
}

future<scattered_message_ptr> database::setbit(redis_key rk, size_t offset, bool value)
{
    ++_stat._setbit;
    return with_allocator(allocator(), [this, rk = std::move(rk), offset, value] {
        auto o = _cache.find(rk);
        size_t offset_in_bytes = offset >> 3;
        if (o == nullptr) {
           size_t offset_in_bytes = offset >> 3;
           size_t origin_size = offset_in_bytes + offset_in_bytes / 4;
           if (origin_size < 15) {
               origin_size = 15;
           }
           auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), origin_size);
           _cache.insert(entry);
            --_stat._total_bitmap_entries;
           o = entry;
        }
        if (o->type_of_bytes() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& mbytes = o->value_bytes();
        if (mbytes.size() < offset_in_bytes) {
            //auto extend_size = offset_in_bytes + offset_in_bytes / 4;
            //mbytes.extend(extend_size, 0);
        }
        auto result = bits_operation::set(mbytes, offset, value);
        return reply_builder::build(result ? msg_one : msg_zero);
    });
}

future<scattered_message_ptr> database::getbit(redis_key rk, size_t offset)
{
    ++_stat._read;
    ++_stat._getbit;
    return _cache.run_with_entry(rk, [this, offset] (const cache_entry* e) {
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_bytes() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& mbytes = e->value_bytes();
        auto result = bits_operation::get(mbytes, offset);
        ++_stat._hit;
        return reply_builder::build(result ? msg_one : msg_zero);
    });
}

future<scattered_message_ptr> database::bitcount(redis_key rk, long start, long end)
{
    ++_stat._read;
    ++_stat._bitcount;
    return _cache.run_with_entry(rk, [this, start, end] (const cache_entry* e) {
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_bytes() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& mbytes = e->value_bytes();
        auto result = bits_operation::count(mbytes, start, end);
        ++_stat._hit;
        return reply_builder::build(result);
    });
}

future<scattered_message_ptr> database::bitpos(redis_key rk, bool bit, long start, long end)
{
    return reply_builder::build(msg_nil);
}

future<scattered_message_ptr> database::pfadd(redis_key rk, std::vector<bytes> elements)
{
    ++_stat._pfadd;
    return with_allocator(allocator(), [this, rk = std::move(rk), elements = std::move(elements)] {
        auto e = _cache.find(rk);
        if (e == nullptr) {
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::hll_initializer());
            _cache.insert(entry);
            --_stat._total_hll_entries;
            e = entry;
        }
        if (e->type_of_hll() == false) {
           return reply_builder::build(msg_type_err);
        }
        managed_bytes& mbytes = e->value_bytes();
        auto result = hll::append(mbytes, elements);
        return reply_builder::build(result);
    });
}

future<scattered_message_ptr> database::pfcount(redis_key rk)
{
    ++_stat._read;
    ++_stat._pfcount;
    return _cache.run_with_entry(rk, [this] (cache_entry* e) {
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_hll() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& mbytes = e->value_bytes();
        auto result = hll::count(mbytes);
        ++_stat._hit;
        return reply_builder::build(result);
    });
}

future<scattered_message_ptr> database::pfmerge(redis_key rk, uint8_t* merged_sources, size_t size)
{
    ++_stat._pfmerge;
    return with_allocator(allocator(), [this, rk = std::move(rk), merged_sources, size] {
        auto e = _cache.find(rk);
        if (e == nullptr) {
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::hll_initializer());
            _cache.insert(entry);
            --_stat._total_hll_entries;
            e = entry;
        }
        if (e->type_of_hll() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& mbytes = e->value_bytes();
        hll::merge(mbytes, merged_sources, size);
        return reply_builder::build(msg_ok);
    });
}


future<> database::stop()
{
    return make_ready_future<>();
}
}
