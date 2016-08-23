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
 *  Copyright (c) 2006-2010, Peng Jian, pstack@163.com. All rights reserved.
 *
 */
#include "redis.hh"
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
#include "redis_protocol.hh"
#include "system_stats.hh"
#include "db.hh"
using namespace net;

namespace bi = boost::intrusive;

namespace redis {

namespace stdx = std::experimental;
using item_ptr = foreign_ptr<boost::intrusive_ptr<item>>;

future<sstring> sharded_redis::echo(args_collection& args)
{
    if (args._command_args_count < 1) {
        return make_ready_future<sstring>("");
    }
    sstring& message  = args._command_args[0];
    return make_ready_future<sstring>(std::move(message));
}

future<int> sharded_redis::set_impl(sstring& key, size_t hash, sstring& val, long expir, uint8_t flag)
{
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().set(key, hash, val, expir, flag));
    }
    return _peers.invoke_on(cpu, &db::set<remote_origin_tag>, std::ref(key), hash, std::ref(val), expir, flag);
}

future<int> sharded_redis::set(args_collection& args)
{
    // parse args
    if (args._command_args_count < 2) {
        return make_ready_future<int>(-1);
    }
    sstring& key = args._command_args[0];
    auto     hash = std::hash<sstring>()(key); 
    sstring& val = args._command_args[1];
    long expir = 0;
    uint8_t flag = 0;
    // [EX seconds] [PS milliseconds] [NX] [XX]
    if (args._command_args_count > 2) {
        for (unsigned int i = 2; i < args._command_args_count; ++i) {
            sstring* v = (i == args._command_args_count - 1) ? nullptr : &(args._command_args[i + 1]);
            sstring& o = args._command_args[i];
            if (o.size() != 3) {
                return make_ready_future<int>(-1);
            }
            if ((o[0] == 'e' || o[0] == 'E') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_EX;
                if (v == nullptr) {
                    return make_ready_future<int>(-1);
                }
                expir = std::atol(v->c_str()) * 1000;
            }
            if ((o[0] == 'p' || o[0] == 'P') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_PX;
                if (v == nullptr) {
                    return make_ready_future<int>(-1);
                }
                expir = std::atol(v->c_str());
            }
            if ((o[0] == 'n' || o[0] == 'N') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_NX;
            }
            if ((o[0] == 'x' || o[0] == 'X') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_XX;
            }
        }
    }
    return set_impl(key, hash, val, expir, flag);
}


future<int> sharded_redis::remove_impl(const sstring& key, size_t hash) {
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().del(key, hash));
    }
    return _peers.invoke_on(cpu, &db::del<remote_origin_tag>, std::ref(key), hash);
}

future<int> sharded_redis::del(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    if (args._command_args.size() == 1) {
        sstring& key = args._command_args[0];
        auto hash = std::hash<sstring>()(key); 
        return remove_impl(key, hash);
    }
    else {
        int success_count = 0;
        return parallel_for_each(args._command_args.begin(), args._command_args.end(), [this, &success_count] (const auto& key) {
            auto hash = std::hash<sstring>()(key);
            return this->remove_impl(key, hash).then([this, &success_count] (auto r) {
                if (r) success_count++;
            });
        }).then([this, &success_count] () {
            return make_ready_future<int>(success_count); 
        });
    }
}

future<int> sharded_redis::mset(args_collection& args)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    if ((args._command_args.size() - 1) % 2 != 0) {
        return make_ready_future<int>(0);
    }

    size_t success_count = 0;
    size_t pair_size = args._command_args.size() / 2;
    return parallel_for_each(boost::irange<unsigned>(0, pair_size), [this, &args, &success_count] (unsigned i) {
        sstring& key = args._command_args[i * 2];
        sstring& val = args._command_args[i * 2 + 1];
        auto hash = std::hash<sstring>()(key);
        return this->set_impl(key, hash, val, 0, 0).then([this, &success_count] (auto r) {
            if (r) success_count++;
        });
    }).then([this, &success_count, pair_size] () {
        return make_ready_future<int>(success_count == pair_size ? 1 : 0); 
    });
}

future<item_ptr> sharded_redis::get_impl(const sstring& key, size_t hash)
{
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<item_ptr>(_peers.local().get(key, hash));
    }
    return _peers.invoke_on(cpu, &db::get<remote_origin_tag>, std::ref(key), hash);
}

future<item_ptr> sharded_redis::get(args_collection& args)
{
    if (args._command_args_count < 1) {
        return make_ready_future<item_ptr>(nullptr);
    }
    sstring& key = args._command_args[0];
    auto hash = std::hash<sstring>()(key); 

    return get_impl(key, hash);
}

future<std::vector<item_ptr>> sharded_redis::mget(args_collection& args)
{
    return make_ready_future<std::vector<item_ptr>>(); 
/*
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>(); 
    }
    std::vector<item_ptr> items;
    return parallel_for_each(args._command_args.begin(), args._command_args.end(), [this, &items] (const auto& key) {
        auto hash = std::hash<sstring>()(key);
        return this->get_impl(key, hash).then([this, &items] (auto it) {
            items.emplace_back(it);
        });
    }).then([this, items = std::move(items)] () {
        return make_ready_future<std::vector<item_ptr>>(std::forward(items)); 
    });
*/
}

future<int> sharded_redis::strlen(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().strlen(key, hash));
    }
    return _peers.invoke_on(cpu, &db::strlen<remote_origin_tag>, std::ref(key), hash);
}

future<int> sharded_redis::exists(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().exists(key, hash));
    }
    return _peers.invoke_on(cpu, &db::exists<remote_origin_tag>, std::ref(key), hash);
}

future<int> sharded_redis::append(args_collection& args)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return make_ready_future<int>(-1);
    }
    sstring& key = args._command_args[0];
    sstring& val = args._command_args[1];
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().append(key, hash, val));
    }
    return _peers.invoke_on(cpu, &db::append<remote_origin_tag>, std::ref(key), hash, std::ref(val));
}

future<int> sharded_redis::push_impl(sstring& key, sstring& val, bool force, bool left)
{
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().push(key, hash, val, force, left));
    }
    return _peers.invoke_on(cpu, &db::push<remote_origin_tag>, std::ref(key), hash, std::ref(val), force, left);
}

future<int> sharded_redis::push_impl(args_collection& args, bool force, bool left)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<int>(-1);
    }
    sstring& key = args._command_args[0];
    if (args._command_args_count == 2) {
        sstring& value = args._command_args[1];
        return push_impl(key, value, force, left);
    }
    else {
        int success_count = 0;
        std::vector<sstring> values;
        for (size_t i = 1; i < args._command_args.size(); ++i) values.emplace_back(args._command_args[i]);
        return parallel_for_each(values.begin(), values.end(), [this, force, left, &key, &success_count] (auto& value) {
            return this->push_impl(key, value, force, left).then([this, &success_count] (auto r) {
                if (r) success_count++;
            });
        }).then([&success_count] () {
            return make_ready_future<int>(success_count); 
        });
    }
}

future<int> sharded_redis::lpush(args_collection& args)
{
    return push_impl(args, true, true);
}

future<int> sharded_redis::lpushx(args_collection& args)
{
    return push_impl(args, false, true);
}

future<int> sharded_redis::rpush(args_collection& args)
{
    return push_impl(args, true, false);
}

future<int> sharded_redis::rpushx(args_collection& args)
{
    return push_impl(args, false, false);
}

future<item_ptr> sharded_redis::lpop(args_collection& args)
{
    return pop_impl(args, true);
}
future<item_ptr> sharded_redis::rpop(args_collection& args)
{
    return pop_impl(args, false);
}
future<item_ptr> sharded_redis::pop_impl(args_collection& args, bool left)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return make_ready_future<item_ptr>(nullptr);
    }
    sstring& key = args._command_args[0];
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<item_ptr>(_peers.local().pop(key, hash, left));
    }
    return _peers.invoke_on(cpu, &db::pop<remote_origin_tag>, std::ref(key), hash, left);
}

future<item_ptr> sharded_redis::lindex(args_collection& args)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return make_ready_future<item_ptr>(nullptr);
    }
    sstring& key = args._command_args[0];
    int idx = std::atoi(args._command_args[1].c_str());
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<item_ptr>(_peers.local().lindex(key, hash, idx));
    }
    return _peers.invoke_on(cpu, &db::lindex<remote_origin_tag>, std::ref(key), hash, idx);
}

future<int> sharded_redis::llen(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().llen(key, hash));
    }
    return _peers.invoke_on(cpu, &db::llen<remote_origin_tag>, std::ref(key), hash);
}

future<int> sharded_redis::linsert(args_collection& args)
{
    if (args._command_args_count <= 3 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    sstring& dir = args._command_args[1];
    sstring& pivot = args._command_args[2];
    sstring& value = args._command_args[3];
    std::transform(dir.begin(), dir.end(), dir.begin(), ::toupper);
    bool after = true;
    if (dir == "BEFORE") after = false;
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().linsert(key, hash, pivot, value, after));
    }
    return _peers.invoke_on(cpu, &db::linsert<remote_origin_tag>, std::ref(key), hash, std::ref(pivot), std::ref(value), after);
}

future<std::vector<item_ptr>> sharded_redis::lrange(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>();
    }
    sstring& key = args._command_args[0];
    sstring& s = args._command_args[1];
    sstring& e = args._command_args[2];
    int start = std::atoi(s.c_str());
    int end = std::atoi(e.c_str());
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<std::vector<item_ptr>>(_peers.local().lrange(key, hash, start, end));
    }
    return _peers.invoke_on(cpu, &db::lrange<remote_origin_tag>, std::ref(key), hash, start, end);
}

future<int> sharded_redis::lset(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    sstring& index = args._command_args[1];
    sstring& value = args._command_args[2];
    int idx = std::atoi(index.c_str());
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().lset(key, hash, idx, value));
    }
    return _peers.invoke_on(cpu, &db::lset<remote_origin_tag>, std::ref(key), hash, idx, std::ref(value));
}

future<int> sharded_redis::ltrim(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    int start =  std::atoi(args._command_args[1].c_str());
    int stop = std::atoi(args._command_args[2].c_str());
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().ltrim(key, hash, start, stop));
    }
    return _peers.invoke_on(cpu, &db::ltrim<remote_origin_tag>, std::ref(key), hash, start, stop);
}

future<int> sharded_redis::lrem(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    int count = std::atoi(args._command_args[1].c_str());
    sstring& value = args._command_args[2];
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().lrem(key, hash, count, value));
    }
    return _peers.invoke_on(cpu, &db::lrem<remote_origin_tag>, std::ref(key), hash, count, std::ref(value));
}

future<uint64_t> sharded_redis::incr(args_collection& args)
{
    return counter_by(args, true, false);
}

future<uint64_t> sharded_redis::incrby(args_collection& args)
{
    return counter_by(args, true, true);
}
future<uint64_t> sharded_redis::decr(args_collection& args)
{
    return counter_by(args, false, false);
}
future<uint64_t> sharded_redis::decrby(args_collection& args)
{
    return counter_by(args, false, true);
}

future<uint64_t> sharded_redis::counter_by(args_collection& args, bool incr, bool with_step)
{
    if (args._command_args_count < 1 || args._command_args.empty() || (with_step == true && args._command_args_count <= 1)) {
        return make_ready_future<uint64_t>(0);
    }
    sstring& key = args._command_args[0];
    uint64_t step = 1;
    if (with_step) {
        sstring& s = args._command_args[1];
        step = std::atol(s.c_str());
    }
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<uint64_t>(_peers.local().counter_by(key, hash, step, incr));
    }
    return _peers.invoke_on(cpu, &db::counter_by<remote_origin_tag>, std::ref(key), hash, step, incr);
}

future<int> sharded_redis::hdel(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().hdel(key, hash, field));
    }
    return _peers.invoke_on(cpu, &db::hdel<remote_origin_tag>, std::ref(key), hash, std::ref(field));
}

future<int> sharded_redis::hexists(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().hexists(key, hash, field));
    }
    return _peers.invoke_on(cpu, &db::hexists<remote_origin_tag>, std::ref(key), hash, std::ref(field));
}

future<int> sharded_redis::hset(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    sstring& val = args._command_args[2];
    auto hash = std::hash<sstring>()(key); 
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().hset(key, hash, field, val));
    }
    return _peers.invoke_on(cpu, &db::hset<remote_origin_tag>, std::ref(key), hash, std::ref(field), std::ref(val));
}

future<int> sharded_redis::hmset(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    unsigned int field_count = (args._command_args_count - 1) / 2;
    if (field_count == 1) {
        return hset(args);
    }
    sstring& key = args._command_args[0];
    std::unordered_map<sstring, sstring> key_values_collection;
    for (unsigned int i = 0; i < field_count; ++i) {
        key_values_collection.emplace(std::make_pair(args._command_args[i], args._command_args[i + 1]));
    }
    auto hash = std::hash<sstring>()(key);
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().hmset(key, hash, key_values_collection));
    }
    return _peers.invoke_on(cpu, &db::hmset<remote_origin_tag>, std::ref(key), hash, std::ref(key_values_collection));
}

future<int> sharded_redis::hincrby(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    sstring& val = args._command_args[2];
    int delta = std::atoi(val.c_str());
    auto hash = std::hash<sstring>()(key);
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().hincrby(key, hash, field, delta));
    }
    return _peers.invoke_on(cpu, &db::hincrby<remote_origin_tag>, std::ref(key), hash, std::ref(field), delta);
}

future<double> sharded_redis::hincrbyfloat(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<double>(-1);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    sstring& val = args._command_args[2];
    double delta = std::atof(val.c_str());
    auto hash = std::hash<sstring>()(key);
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<double>(_peers.local().hincrbyfloat(key, hash, field, delta));
    }
    return _peers.invoke_on(cpu, &db::hincrbyfloat<remote_origin_tag>, std::ref(key), hash, std::ref(field), delta);
}

future<int> sharded_redis::hlen(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return make_ready_future<int>(-1);
    }
    sstring& key = args._command_args[0];
    auto hash = std::hash<sstring>()(key);
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().hlen(key, hash));
    }
    return _peers.invoke_on(cpu, &db::hlen<remote_origin_tag>, std::ref(key), hash);
}

future<int> sharded_redis::hstrlen(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<int>(-1);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    auto hash = std::hash<sstring>()(key);
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_peers.local().hstrlen(key, hash, field));
    }
    return _peers.invoke_on(cpu, &db::hstrlen<remote_origin_tag>, std::ref(key), hash, std::ref(field));
}

future<item_ptr> sharded_redis::hget(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<item_ptr>();
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    auto hash = std::hash<sstring>()(key);
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<item_ptr>(_peers.local().hget(key, hash, field));
    }
    return _peers.invoke_on(cpu, &db::hget<remote_origin_tag>, std::ref(key), hash, std::ref(field));
}

future<std::vector<item_ptr>> sharded_redis::hgetall(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>();
    }
    sstring& key = args._command_args[0];
    auto hash = std::hash<sstring>()(key);
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<std::vector<item_ptr>>(_peers.local().hgetall(key, hash));
    }
    return _peers.invoke_on(cpu, &db::hgetall<remote_origin_tag>, std::ref(key), hash);
}

future<std::vector<item_ptr>> sharded_redis::hmget(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>();
    }
    sstring& key = args._command_args[0];
    std::unordered_set<sstring> fields;
    for (unsigned int i = 1; i < args._command_args_count; ++i) {
      fields.emplace(std::move(args._command_args[i]));
    }
    auto hash = std::hash<sstring>()(key);
    auto cpu = get_cpu(hash);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<std::vector<item_ptr>>(_peers.local().hmget(key, hash, fields));
    }
    return _peers.invoke_on(cpu, &db::hmget<remote_origin_tag>, std::ref(key), hash, std::ref(fields));
}

} /* namespace redis */
