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
#include "core/bitops.hh"
#include "core/slab.hh"
#include "core/align.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include <unistd.h>
#include <cstdlib>
#include "base.hh"
namespace redis {

namespace stdx = std::experimental;

struct args_collection;
class db;
class item;
using item_ptr = foreign_ptr<lw_shared_ptr<item>>;
using message = scattered_message<char>;
class redis_service {
private:
    inline unsigned get_cpu(const sstring& key) {
        return std::hash<sstring>()(key) % smp::count;
    }
    distributed<db>& _db_peers;
public:
    redis_service(distributed<db>& db) : _db_peers(db) 
    {
    }

    // [TEST APIs]
    future<sstring> echo(args_collection& args);
    // [COUNTER APIs]
    future<message> incr(args_collection& args);
    future<message> decr(args_collection& args);
    future<message> incrby(args_collection& args);
    future<message> decrby(args_collection& args);

    // [STRING APIs]
    future<message> mset(args_collection& args);
    future<message> set(args_collection& args);
    future<message> del(args_collection& args);
    future<message> exists(args_collection& args);
    future<message> append(args_collection& args);
    future<message> strlen(args_collection& args);
    future<message> get(args_collection& args);
    future<message> mget(args_collection& args);

    // [LIST APIs]
    future<message> lpush(args_collection& arg);
    future<message> lpushx(args_collection& args);
    future<message> rpush(args_collection& arg);
    future<message> rpushx(args_collection& args);
    future<message> lpop(args_collection& args);
    future<message> rpop(args_collection& args);
    future<message> llen(args_collection& args);
    future<message> lindex(args_collection& args);
    future<message> linsert(args_collection& args);
    future<message> lset(args_collection& args);
    future<message> lrange(args_collection& args);
    future<message> ltrim(args_collection& args);
    future<message> lrem(args_collection& args);

    // [HASH APIs]
    future<message> hdel(args_collection& args);
    future<message> hexists(args_collection& args);
    future<message> hset(args_collection& args);
    future<message> hmset(args_collection& args);
    future<message> hincrby(args_collection& args);
    future<message> hincrbyfloat(args_collection& args);
    future<message> hlen(args_collection& args);
    future<message> hstrlen(args_collection& args);
    future<message> hget(args_collection& args);
    future<message> hgetall(args_collection& args);
    future<message> hmget(args_collection& args);

    // [SET]
    future<message> sadd(args_collection& args);
    future<message> scard(args_collection& args);
    future<message> srem(args_collection& args);
    future<message> sismember(args_collection& args);
    future<message> smembers(args_collection& args);
    future<message> sdiff(args_collection& args);
    future<message> sdiff_store(args_collection& args);
    future<message> sinter(args_collection& args);
    future<message> sinter_store(args_collection& args);
    future<message> sunion(args_collection& args);
    future<message> sunion_store(args_collection& args);
    future<message> smove(args_collection& args);

    future<message> type(args_collection& args);
    future<message> expire(args_collection& args);
    future<message> persist(args_collection& args);
    future<message> pexpire(args_collection& args);
    future<message> ttl(args_collection& args);
    future<message> pttl(args_collection& args);

    // [ZSET]
    future<message> zadd(args_collection& args);
    future<message> zcard(args_collection& args);
    future<message> zrange(args_collection&, bool);
    future<message> zrangebyscore(args_collection&, bool);
    future<message> zcount(args_collection& args);
    future<message> zincrby(args_collection& args);
    future<message> zrank(args_collection&, bool);
    future<message> zrem(args_collection&);
    future<message> zscore(args_collection&);
    future<message> zunionstore(args_collection&);
    future<message> zinterstore(args_collection&);
    future<message> zdiffstore(args_collection&);
    future<message> zunion(args_collection&);
    future<message> zinter(args_collection&);
    future<message> zdiff(args_collection&);
    future<message> zrangebylex(args_collection&);
    future<message> zlexcount(args_collection&);
    future<message> zrevrangebylex(args_collection&);
    future<message> zremrangebyscore(args_collection&);
    future<message> zremrangebyrank(args_collection&);
private:
    future<std::pair<size_t, int>> zadds_impl(sstring& key, std::unordered_map<sstring, double>&& members, int flags);
    future<std::vector<item_ptr>> range_impl(const sstring& key, long begin, long end, bool reverse);
    future<int> exists_impl(sstring& key);
    future<int> srem_impl(sstring& key, sstring& member);
    future<int> sadd_impl(sstring& key, sstring& member);
    future<int> sadds_impl(sstring& key, std::vector<sstring>&& members);
    future<std::vector<item_ptr>> sdiff_impl(std::vector<sstring>&& keys);
    future<std::vector<item_ptr>> sinter_impl(std::vector<sstring>&& keys);
    future<std::vector<item_ptr>> sunion_impl(std::vector<sstring>&& keys);
    future<std::vector<item_ptr>> smembers_impl(sstring& key);
    future<message> pop_impl(args_collection& args, bool left);
    future<message> push_impl(args_collection& arg, bool force, bool left);
    future<int> push_impl(sstring& key, sstring& value, bool force, bool left);
    future<int> set_impl(sstring& key, sstring& value, long expir, uint8_t flag);
    future<item_ptr> get_impl(sstring& key);
    future<int> remove_impl(sstring& key);
    future<int> hdel_impl(sstring& key, sstring& field);
    future<message> counter_by(args_collection& args, bool incr, bool with_step);
    struct zset_args
    {
        sstring dest;
        size_t numkeys;
        std::vector<sstring> keys;
        std::vector<double> weights;
        int aggregate_flag;
    };
    bool parse_zset_args(args_collection& args, zset_args& uargs);

    using this_type = redis_service;
    static future<message> syntax_err_message() {
        message msg;
        msg.append_static(msg_syntax_err);
        return make_ready_future<message>(std::move(msg));
    }
    static future<message> nil_message() {
        message msg;
        msg.append_static(msg_nil);
        return make_ready_future<message>(std::move(msg));
    }
    static future<message> wrong_type_err_message() {
        message msg;
        msg.append_static(msg_type_err);
        return make_ready_future<message>(std::move(msg));
    }
    static future<message> ok_message() {
        message msg;
        msg.append_static(msg_ok);
        return make_ready_future<message>(std::move(msg));
    }
    static future<message> err_message() {
        message msg;
        msg.append_static(msg_err);
        return make_ready_future<message>(std::move(msg));
    }
    static future<message> one_message() {
        message msg;
        msg.append_static(msg_one);
        return make_ready_future<message>(std::move(msg));
    }
    static future<message> zero_message() {
        message msg;
        msg.append_static(msg_zero);
        return make_ready_future<message>(std::move(msg));
    }
    static future<message> size_message(long u)
    {
        scattered_message<char> msg;
        msg.append_static(msg_num_tag);
        msg.append(to_sstring(u));
        msg.append_static(msg_crlf);
        return make_ready_future<message>(std::move(msg));
    }
    static future<message> size_message(size_t u)
    {
        scattered_message<char> msg;
        msg.append_static(msg_num_tag);
        msg.append(to_sstring(u));
        msg.append_static(msg_crlf);
        return make_ready_future<message>(std::move(msg));
    }
    static future<message> size_message(int u)
    {
        scattered_message<char> msg;
        msg.append_static(msg_num_tag);
        msg.append(to_sstring(u));
        msg.append_static(msg_crlf);
        return make_ready_future<message>(std::move(msg));
    }
    static future<message> uint64_message(uint64_t u)
    {
        scattered_message<char> msg;
        msg.append_static(msg_num_tag);
        msg.append(to_sstring(u));
        msg.append_static(msg_crlf);
        return make_ready_future<message>(std::move(msg));
    }
    static future<message> int64_message(int64_t u)
    {
        scattered_message<char> msg;
        msg.append_static(msg_num_tag);
        msg.append(to_sstring(u));
        msg.append_static(msg_crlf);
        return make_ready_future<message>(std::move(msg));
    }
    template<bool string = false>
    static future<message> double_message(double u)
    {
        scattered_message<char> msg;
        if (!string) {
            msg.append_static(msg_num_tag);
            msg.append(to_sstring(u));
            msg.append_static(msg_crlf);
        }
        else {
            auto&& n = to_sstring(u);
            msg.append_static(msg_batch_tag);
            msg.append(to_sstring(n.size()));
            msg.append_static(msg_crlf);
            msg.append(std::move(n));
            msg.append_static(msg_crlf);
        }
        return make_ready_future<message>(std::move(msg));
    }
    static future<message> item_message(sstring&& u) 
    {
        scattered_message<char> msg;
        msg.append_static(msg_batch_tag);
        msg.append(to_sstring(u.size()));
        msg.append_static(msg_crlf);
        msg.append_static(std::move(u));
        msg.append_static(msg_crlf);
        return make_ready_future<message>(std::move(msg));
    }

    template<bool Key, bool Value>
    static future<message>  item_message(item_ptr&& u) 
    {
        scattered_message<char> msg;
        this_type::append_item<Key, Value>(msg, std::move(u));
        return make_ready_future<message>(std::move(msg));
    }

    template<bool Key, bool Value>
    static void  append_item(message& msg, item_ptr&& u) 
    {
        if (!u) {
            msg.append_static(msg_not_found);
        }
        else {
            if (Key) {
                msg.append(msg_batch_tag);
                msg.append(to_sstring(u->key_size()));
                msg.append_static(msg_crlf);
                sstring v{u->key().data(), u->key().size()};
                msg.append(std::move(v));
                msg.append_static(msg_crlf);
            }
            if (Value) {
                msg.append_static(msg_batch_tag);
                if (u->type() == REDIS_RAW_UINT64 || u->type() == REDIS_RAW_INT64) {
                    auto&& n = to_sstring(u->int64());
                    msg.append(to_sstring(n.size()));
                    msg.append_static(msg_crlf);
                    msg.append(std::move(n));
                    msg.append_static(msg_crlf);
                } else if (u->type() == REDIS_RAW_ITEM || u->type() == REDIS_RAW_STRING) {
                    msg.append(to_sstring(u->value_size()));
                    msg.append_static(msg_crlf);
                    msg.append_static(u->value());
                    msg.append_static(msg_crlf);
                } else if (u->type() == REDIS_RAW_DOUBLE) {
                    auto&& n = to_sstring(u->Double());
                    msg.append(to_sstring(n.size()));
                    msg.append_static(msg_crlf);
                    msg.append(std::move(n));
                    msg.append_static(msg_crlf);
                } else {
                    msg.append_static(msg_type_err);
                }
            }
            msg.on_delete([u = std::move(u)] {});
        }
    }

    template<bool Key, bool Value>
    static future<message> items_message(std::vector<item_ptr>&& items) 
    {
        message msg;
        msg.append(msg_sigle_tag);
        if (Key && Value)
            msg.append(std::move(to_sstring(items.size() * 2)));
        else
            msg.append(std::move(to_sstring(items.size())));
        msg.append_static(msg_crlf);
        for (size_t i = 0; i < items.size(); ++i) {
            if (Key) {
                msg.append(msg_batch_tag);
                msg.append(std::move(to_sstring(items[i]->key_size())));
                msg.append_static(msg_crlf);
                sstring v{items[i]->key().data(), items[i]->key().size()};
                msg.append(std::move(v));
                msg.append_static(msg_crlf);
            }
            if (Value) {
                if (!items[i]) {
                    msg.append_static(msg_not_found);
                }
                else {
                    msg.append(msg_batch_tag);
                    if (items[i]->type() == REDIS_RAW_UINT64 || items[i]->type() == REDIS_RAW_INT64) {
                        auto&& n = to_sstring(items[i]->int64());
                        msg.append(to_sstring(n.size()));
                        msg.append_static(msg_crlf);
                        msg.append(std::move(n));
                        msg.append_static(msg_crlf);
                    } else if (items[i]->type() == REDIS_RAW_ITEM || items[i]->type() == REDIS_RAW_STRING) {
                        msg.append(std::move(to_sstring(items[i]->value_size())));
                        msg.append_static(msg_crlf);
                        sstring v{items[i]->value().data(), items[i]->value().size()};
                        msg.append(std::move(v));
                        msg.append_static(msg_crlf);
                    } else if (items[i]->type() == REDIS_RAW_DOUBLE) {
                        auto&& n = to_sstring(items[i]->Double());
                        msg.append(to_sstring(n.size()));
                        msg.append_static(msg_crlf);
                        msg.append(std::move(n));
                        msg.append_static(msg_crlf);
                    } else {
                        msg.append_static(msg_type_err);
                    }
                }
            }
        }
        msg.on_delete([item = std::move(items)] {});
        return make_ready_future<message>(std::move(msg));
    }

    static future<message> type_message(int u) 
    {
        message msg;
        if (u == static_cast<int>(REDIS_RAW_STRING)) {
            msg.append_static(msg_type_string);
        }
        else if (u == static_cast<int>(REDIS_LIST)) {
            msg.append_static(msg_type_list);
        }
        else if (u == static_cast<int>(REDIS_DICT)) {
            msg.append_static(msg_type_hash);
        }
        else if (u == static_cast<int>(REDIS_SET)) {
            msg.append_static(msg_type_set);
        }
        else if (u == static_cast<int>(REDIS_ZSET)) {
            msg.append_static(msg_type_zset);
        }
        else {
            msg.append_static(msg_type_none);
        }
        return make_ready_future<message>(std::move(msg));
    }
};

} /* namespace redis */
