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
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
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
using item_ptr = foreign_ptr<boost::intrusive_ptr<item>>;

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
    future<uint64_t> incr(args_collection& args);
    future<uint64_t> decr(args_collection& args);
    future<uint64_t> incrby(args_collection& args);
    future<uint64_t> decrby(args_collection& args);

    // [STRING APIs]
    future<int> mset(args_collection& args);
    future<int> set(args_collection& args);
    future<int> del(args_collection& args);
    future<int> exists(args_collection& args);
    future<int> append(args_collection& args);
    future<int> strlen(args_collection& args);
    future<item_ptr> get(args_collection& args);
    future<std::vector<item_ptr>> mget(args_collection& args);

    // [LIST APIs]
    future<int> lpush(args_collection& arg);
    future<int> lpushx(args_collection& args);
    future<int> rpush(args_collection& arg);
    future<int> rpushx(args_collection& args);
    future<item_ptr> lpop(args_collection& args);
    future<item_ptr> rpop(args_collection& args);
    future<int> llen(args_collection& args);
    future<item_ptr> lindex(args_collection& args);
    future<int> linsert(args_collection& args);
    future<int> lset(args_collection& args);
    future<std::vector<item_ptr>> lrange(args_collection& args);
    future<int> ltrim(args_collection& args);
    future<int> lrem(args_collection& args);

    // [HASH APIs]
    future<int> hdel(args_collection& args);
    future<int> hexists(args_collection& args);
    future<int> hset(args_collection& args);
    future<int> hmset(args_collection& args);
    future<int> hincrby(args_collection& args);
    future<double> hincrbyfloat(args_collection& args);
    future<int> hlen(args_collection& args);
    future<int> hstrlen(args_collection& args);
    future<item_ptr> hget(args_collection& args);
    future<std::vector<item_ptr>> hgetall(args_collection& args);
    future<std::vector<item_ptr>> hmget(args_collection& args);

    // [SET]
    future<int> sadd(args_collection& args);
    future<int> scard(args_collection& args);
    future<int> srem(args_collection& args);
    future<int> sismember(args_collection& args);
    future<std::vector<item_ptr>> smembers(args_collection& args);
    future<std::vector<item_ptr>> sdiff(args_collection& args);
    future<std::vector<item_ptr>> sdiff_store(args_collection& args);
    future<std::vector<item_ptr>> sinter(args_collection& args);
    future<std::vector<item_ptr>> sinter_store(args_collection& args);
    future<std::vector<item_ptr>> sunion(args_collection& args);
    future<std::vector<item_ptr>> sunion_store(args_collection& args);
    future<int> smove(args_collection& args);
private:
    future<int> srem_impl(sstring& key, sstring& member);
    future<int> sadd_impl(sstring& key, sstring& member);
    future<int> sadds_impl(sstring& key, std::vector<sstring>&& members);
    future<std::vector<item_ptr>> sdiff_impl(std::vector<sstring>&& keys);
    future<std::vector<item_ptr>> sinter_impl(std::vector<sstring>&& keys);
    future<std::vector<item_ptr>> sunion_impl(std::vector<sstring>&& keys);
    future<std::vector<item_ptr>> smembers_impl(sstring& key);
    future<item_ptr> pop_impl(args_collection& args, bool left);
    future<int> push_impl(args_collection& arg, bool force, bool left);
    future<int> push_impl(sstring& key, sstring& value, bool force, bool left);
    future<int> set_impl(sstring& key, sstring& value, long expir, uint8_t flag);
    future<item_ptr> get_impl(sstring& key);
    future<int> remove_impl(sstring& key);
    future<uint64_t> counter_by(args_collection& args, bool incr, bool with_step);
};

} /* namespace redis */
