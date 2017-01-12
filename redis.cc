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
 *  Copyright (c) 2016-2026, Peng Jian, pstack@163.com. All rights reserved.
 *
 */
#include "redis.hh"
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <unordered_set>
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
future<sstring> redis_service::echo(args_collection& args)
{
    if (args._command_args_count < 1) {
        return make_ready_future<sstring>("");
    }
    sstring& message  = args._command_args[0];
    return make_ready_future<sstring>(std::move(message));
}

future<int> redis_service::set_impl(sstring& key, sstring& val, long expir, uint8_t flag)
{
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().set(key, val, expir, flag));
    }
    return _db_peers.invoke_on(cpu, &db::set<remote_origin_tag>, std::ref(key), std::ref(val), expir, flag);
}

future<int> redis_service::set(args_collection& args)
{
    // parse args
    if (args._command_args_count < 2) {
        return make_ready_future<int>(-1);
    }
    sstring& key = args._command_args[0];
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
    return set_impl(key, val, expir, flag);
}

future<int> redis_service::remove_impl(sstring& key) {
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().del(key));
    }
    return _db_peers.invoke_on(cpu, &db::del, std::ref(key));
}

future<int> redis_service::del(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    if (args._command_args.size() == 1) {
        sstring& key = args._command_args[0];
        return remove_impl(key);
    }
    else {
        return make_ready_future<int>(0); 
    }
}

future<int> redis_service::mset(args_collection& args)
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
        return this->set_impl(key, val, 0, 0).then([this, &success_count] (auto r) {
            if (r) success_count++;
        });
    }).then([this, &success_count, pair_size] () {
        return make_ready_future<int>(success_count == pair_size ? 1 : 0); 
    });
}

future<item_ptr> redis_service::get_impl(sstring& key)
{
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<item_ptr>(_db_peers.local().get(key));
    }
    return _db_peers.invoke_on(cpu, &db::get, std::ref(key));
}

future<item_ptr> redis_service::get(args_collection& args)
{
    if (args._command_args_count < 1) {
        return make_ready_future<item_ptr>(nullptr);
    }
    sstring& key = args._command_args[0];
    return get_impl(key);
}

future<std::vector<item_ptr>> redis_service::mget(args_collection& args)
{
    return make_ready_future<std::vector<item_ptr>>(); 
}

future<int> redis_service::strlen(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().strlen(key));
    }
    return _db_peers.invoke_on(cpu, &db::strlen, std::ref(key));
}

future<int> redis_service::exists(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().exists(key));
    }
    return _db_peers.invoke_on(cpu, &db::exists, std::ref(key));
}

future<int> redis_service::append(args_collection& args)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return make_ready_future<int>(-1);
    }
    sstring& key = args._command_args[0];
    sstring& val = args._command_args[1];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().append(key, val));
    }
    return _db_peers.invoke_on(cpu, &db::append<remote_origin_tag>, std::ref(key), std::ref(val));
}

future<int> redis_service::push_impl(sstring& key, sstring& val, bool force, bool left)
{
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().push(key, val, force, left));
    }
    return _db_peers.invoke_on(cpu, &db::push<remote_origin_tag>, std::ref(key), std::ref(val), force, left);
}

future<int> redis_service::push_impl(args_collection& args, bool force, bool left)
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

future<int> redis_service::lpush(args_collection& args)
{
    return push_impl(args, true, true);
}

future<int> redis_service::lpushx(args_collection& args)
{
    return push_impl(args, false, true);
}

future<int> redis_service::rpush(args_collection& args)
{
    return push_impl(args, true, false);
}

future<int> redis_service::rpushx(args_collection& args)
{
    return push_impl(args, false, false);
}

future<item_ptr> redis_service::lpop(args_collection& args)
{
    return pop_impl(args, true);
}
future<item_ptr> redis_service::rpop(args_collection& args)
{
    return pop_impl(args, false);
}
future<item_ptr> redis_service::pop_impl(args_collection& args, bool left)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return make_ready_future<item_ptr>(nullptr);
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<item_ptr>(_db_peers.local().pop(key, left));
    }
    return _db_peers.invoke_on(cpu, &db::pop, std::ref(key), left);
}

future<item_ptr> redis_service::lindex(args_collection& args)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return make_ready_future<item_ptr>(nullptr);
    }
    sstring& key = args._command_args[0];
    int idx = std::atoi(args._command_args[1].c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<item_ptr>(_db_peers.local().lindex(key, idx));
    }
    return _db_peers.invoke_on(cpu, &db::lindex, std::ref(key), idx);
}

future<int> redis_service::llen(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().llen(key));
    }
    return _db_peers.invoke_on(cpu, &db::llen, std::ref(key));
}

future<int> redis_service::linsert(args_collection& args)
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
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().linsert(key, pivot, value, after));
    }
    return _db_peers.invoke_on(cpu, &db::linsert<remote_origin_tag>, std::ref(key), std::ref(pivot), std::ref(value), after);
}

future<std::vector<item_ptr>> redis_service::lrange(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>();
    }
    sstring& key = args._command_args[0];
    sstring& s = args._command_args[1];
    sstring& e = args._command_args[2];
    int start = std::atoi(s.c_str());
    int end = std::atoi(e.c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<std::vector<item_ptr>>(_db_peers.local().lrange(key, start, end));
    }
    return _db_peers.invoke_on(cpu, &db::lrange, std::ref(key), start, end);
}

future<int> redis_service::lset(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    sstring& index = args._command_args[1];
    sstring& value = args._command_args[2];
    int idx = std::atoi(index.c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().lset(key, idx, value));
    }
    return _db_peers.invoke_on(cpu, &db::lset<remote_origin_tag>, std::ref(key), idx, std::ref(value));
}

future<int> redis_service::ltrim(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    int start =  std::atoi(args._command_args[1].c_str());
    int stop = std::atoi(args._command_args[2].c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().ltrim(key, start, stop));
    }
    return _db_peers.invoke_on(cpu, &db::ltrim, std::ref(key), start, stop);
}

future<int> redis_service::lrem(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    int count = std::atoi(args._command_args[1].c_str());
    sstring& value = args._command_args[2];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().lrem(key, count, value));
    }
    return _db_peers.invoke_on(cpu, &db::lrem, std::ref(key), count, std::ref(value));
}

future<uint64_t> redis_service::incr(args_collection& args)
{
    return counter_by(args, true, false);
}

future<uint64_t> redis_service::incrby(args_collection& args)
{
    return counter_by(args, true, true);
}
future<uint64_t> redis_service::decr(args_collection& args)
{
    return counter_by(args, false, false);
}
future<uint64_t> redis_service::decrby(args_collection& args)
{
    return counter_by(args, false, true);
}

future<uint64_t> redis_service::counter_by(args_collection& args, bool incr, bool with_step)
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
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<uint64_t>(_db_peers.local().counter_by(key, step, incr));
    }
    return _db_peers.invoke_on(cpu, &db::counter_by<remote_origin_tag>, std::ref(key), step, incr);
}

future<int> redis_service::hdel(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().hdel(key, field));
    }
    return _db_peers.invoke_on(cpu, &db::hdel, std::ref(key), std::ref(field));
}

future<int> redis_service::hexists(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().hexists(key, field));
    }
    return _db_peers.invoke_on(cpu, &db::hexists, std::ref(key), std::ref(field));
}

future<int> redis_service::hset(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    sstring& val = args._command_args[2];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().hset(key, field, val));
    }
    return _db_peers.invoke_on(cpu, &db::hset<remote_origin_tag>, std::ref(key), std::ref(field), std::ref(val));
}

future<int> redis_service::hmset(args_collection& args)
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
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().hmset(key, key_values_collection));
    }
    return _db_peers.invoke_on(cpu, &db::hmset<remote_origin_tag>, std::ref(key), std::ref(key_values_collection));
}

future<int> redis_service::hincrby(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    sstring& val = args._command_args[2];
    int delta = std::atoi(val.c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().hincrby(key, field, delta));
    }
    return _db_peers.invoke_on(cpu, &db::hincrby<remote_origin_tag>, std::ref(key), std::ref(field), delta);
}

future<double> redis_service::hincrbyfloat(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<double>(-1);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    sstring& val = args._command_args[2];
    double delta = std::atof(val.c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<double>(_db_peers.local().hincrbyfloat(key, field, delta));
    }
    return _db_peers.invoke_on(cpu, &db::hincrbyfloat<remote_origin_tag>, std::ref(key), std::ref(field), delta);
}

future<int> redis_service::hlen(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return make_ready_future<int>(-1);
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().hlen(key));
    }
    return _db_peers.invoke_on(cpu, &db::hlen, std::ref(key));
}

future<int> redis_service::hstrlen(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<int>(-1);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().hstrlen(key, field));
    }
    return _db_peers.invoke_on(cpu, &db::hstrlen, std::ref(key), std::ref(field));
}

future<item_ptr> redis_service::hget(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<item_ptr>();
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<item_ptr>(_db_peers.local().hget(key, field));
    }
    return _db_peers.invoke_on(cpu, &db::hget, std::ref(key), std::ref(field));
}

future<std::vector<item_ptr>> redis_service::hgetall(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<std::vector<item_ptr>>(_db_peers.local().hgetall(key));
    }
    return _db_peers.invoke_on(cpu, &db::hgetall, std::ref(key));
}

future<std::vector<item_ptr>> redis_service::hmget(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>();
    }
    sstring& key = args._command_args[0];
    std::unordered_set<sstring> fields;
    for (unsigned int i = 1; i < args._command_args_count; ++i) {
      fields.emplace(std::move(args._command_args[i]));
    }
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<std::vector<item_ptr>>(_db_peers.local().hmget(key, fields));
    }
    return _db_peers.invoke_on(cpu, &db::hmget, std::ref(key), std::ref(fields));
}

future<std::vector<item_ptr>> redis_service::smembers_impl(sstring& key)
{
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<std::vector<item_ptr>>(_db_peers.local().smembers(key));
    }
    return _db_peers.invoke_on(cpu, &db::smembers, std::ref(key));
}
future<std::vector<item_ptr>> redis_service::smembers(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>();
    }
    sstring& key = args._command_args[0];
    return smembers_impl(key);
}

future<int> redis_service::sadds_impl(sstring& key, std::vector<sstring>&& members)
{
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().sadds(key, std::move(members)));
    }
    return _db_peers.invoke_on(cpu, &db::sadds, std::ref(key), std::move(members));
}

future<int> redis_service::sadd(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<int>();
    }
    sstring& key = args._command_args[0];
    std::vector<sstring> members;
    for (uint32_t i = 1; i < args._command_args_count; ++i) members.emplace_back(std::move(args._command_args[i]));
    return sadds_impl(key, std::move(members));
}

future<int> redis_service::scard(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return make_ready_future<int>();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().scard(key));
    }
    return _db_peers.invoke_on(cpu, &db::scard, std::ref(key));
}
future<int> redis_service::sismember(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<int>();
    }
    sstring& key = args._command_args[0];
    sstring& member = args._command_args[1];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().sismember(key, std::move(member)));
    }
    return _db_peers.invoke_on(cpu, &db::sismember, std::ref(key), std::move(member));
}

future<int> redis_service::srem(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<int>();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    std::vector<sstring> members;
    for (uint32_t i = 1; i < args._command_args_count; ++i) members.emplace_back(std::move(args._command_args[i]));
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().srems(key, std::move(members)));
    }
    return _db_peers.invoke_on(cpu, &db::srems, std::ref(key), std::move(members));
}

future<std::vector<item_ptr>> redis_service::sdiff_store(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>();
    }
    sstring& dest = args._command_args[0];
    std::vector<sstring> keys;
    for (size_t i = 1; i < args._command_args.size(); ++i) {
        keys.emplace_back(std::move(args._command_args[i]));
    }
    struct diff_store_state {
        std::vector<item_ptr> result;
        std::vector<sstring> keys;
        sstring dest;
    };
    return do_with(diff_store_state{{}, std::move(keys), std::move(dest)}, [this] (auto& state) {
        return this->sdiff_impl(std::move(state.keys)).then([this, &state] (auto&& items) {
            std::vector<sstring> members;
            for (item_ptr& item : items) {
               sstring member(item->key().data(), item->key().size());
               members.emplace_back(std::move(member));
            }
            state.result = std::move(items);
            return this->sadds_impl(state.dest, std::move(members)).then([&state] (int discard) {
                (void) discard;
                auto&& result = std::move(state.result);
                return make_ready_future<std::vector<item_ptr>>(std::move(result));
            });
        });
    });
}


future<std::vector<item_ptr>> redis_service::sdiff(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>();
    }
    if (args._command_args_count == 1) {
        return smembers(args);
    }
    return sdiff_impl(std::move(args._command_args));
}

future<std::vector<item_ptr>> redis_service::sdiff_impl(std::vector<sstring>&& keys)
{
    using item_unordered_map = std::unordered_map<unsigned, std::vector<item_ptr>>;
    struct sdiff_state {
        item_unordered_map items_set;
        std::vector<sstring> keys;
    };
    uint32_t count = static_cast<uint32_t>(keys.size());
    return do_with(sdiff_state{item_unordered_map{}, std::move(keys)}, [this, count] (auto& state) {
        return parallel_for_each(boost::irange<unsigned>(0, count), [this, &state] (unsigned k) {
            sstring& key = state.keys[k];
            return this->smembers_impl(key).then([&state, index = k] (auto&& sub) {
                state.items_set[index] = std::move(sub);
            });
        }).then([&state, count] {
            auto& temp = state.items_set[0];
            std::unordered_map<std::experimental::string_view, unsigned> tokens;
            for (item_ptr& item : temp) {
                tokens[item->key()] = 1;
            }
            for (uint32_t i = 1; i < count; ++i) {
                auto&& next_items = std::move(state.items_set[i]);
                for (item_ptr& item : next_items) {
                     if (std::find_if(temp.begin(), temp.end(), [&item] (auto& o) { return item->key() == o->key(); }) != temp.end()) {
                        tokens[item->key()]++; 
                     }
                }
            }
            std::vector<item_ptr> result;
            for (item_ptr& item : temp) {
                if (tokens[item->key()] == 1) {
                    result.emplace_back(std::move(item));
                }
            }
            /*
            auto&& result = state.items_set[0];
            for (uint32_t i = 1; i < count && !result.empty(); ++i) {
                auto&& next_items = std::move(state.items_set[i]);
                for (item_ptr& item : next_items) {
                    auto removed = std::remove_if(result.begin(), result.end(), [&item] (auto& o) { return item->key() == o->key(); });
                    if (removed != result.end()) {
                        (*removed)->intrusive_ptr_add_ref();
                        result.erase(removed, result.end());
                    }
                }
            }
            */

            return make_ready_future<std::vector<item_ptr>>(std::move(result));
       });
   });
}

future<std::vector<item_ptr>> redis_service::sinter_impl(std::vector<sstring>&& keys)
{
    using item_unordered_map = std::unordered_map<unsigned, std::vector<item_ptr>>;
    struct sinter_state {
        item_unordered_map items_set;
        std::vector<sstring> keys;
    };
    uint32_t count = static_cast<uint32_t>(keys.size());
    return do_with(sinter_state{item_unordered_map{}, std::move(keys)}, [this, count] (auto& state) {
        return parallel_for_each(boost::irange<unsigned>(0, count), [this, &state] (unsigned k) {
            sstring& key = state.keys[k];
            return this->smembers_impl(key).then([&state, index = k] (auto&& sub) {
                state.items_set[index] = std::move(sub);
            });
        }).then([&state, count] {
            auto&& result = std::move(state.items_set[0]);
            for (uint32_t i = 1; i < count; ++i) {
                std::vector<item_ptr> temp;    
                auto&& next_items = std::move(state.items_set[i]);
                if (result.empty() || next_items.empty()) {
                    return make_ready_future<std::vector<item_ptr>>();
                }
                for (item_ptr& item : next_items) {
                    if (std::find_if(result.begin(), result.end(), [&item] (item_ptr& o) {
                        return item->key_size() == o->key_size() && item->key() == o->key();
                    }) != result.end()) {
                        temp.emplace_back(std::move(item));
                    }
                }
                result = std::move(temp);
            }
            return make_ready_future<std::vector<item_ptr>>(std::move(result));
       });
   });
}

future<std::vector<item_ptr>> redis_service::sinter(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>();
    }
    if (args._command_args_count == 1) {
        return smembers(args);
    }
    return sinter_impl(std::move(args._command_args));
}

future<std::vector<item_ptr>> redis_service::sinter_store(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>();
    }
    sstring& dest = args._command_args[0];
    std::vector<sstring> keys;
    for (size_t i = 1; i < args._command_args.size(); ++i) {
        keys.emplace_back(std::move(args._command_args[i]));
    }
    struct inter_store_state {
        std::vector<item_ptr> result;
        std::vector<sstring> keys;
        sstring dest;
    };
    return do_with(inter_store_state{{}, std::move(keys), std::move(dest)}, [this] (auto& state) {
        return this->sinter_impl(std::move(state.keys)).then([this, &state] (auto&& items) {
            std::vector<sstring> members;
            for (item_ptr& item : items) {
               sstring member(item->key().data(), item->key().size());
               members.emplace_back(std::move(member));
            }
            state.result = std::move(items);
            return this->sadds_impl(state.dest, std::move(members)).then([&state] (int discard) {
                (void) discard;
                auto&& result = std::move(state.result);
                return make_ready_future<std::vector<item_ptr>>(std::move(result));
            });
        });
    });
}

future<std::vector<item_ptr>> redis_service::sunion_impl(std::vector<sstring>&& keys)
{
    struct union_state {
        std::vector<item_ptr> result;
        std::vector<sstring> keys;
    };
    uint32_t count = static_cast<uint32_t>(keys.size());
    return do_with(union_state{{}, std::move(keys)}, [this, count] (auto& state) {
        return parallel_for_each(boost::irange<unsigned>(0, count), [this, &state] (unsigned k) {
            sstring& key = state.keys[k];
            return this->smembers_impl(key).then([&state] (auto&& sub) {
                auto& result = state.result;
                for (auto& i : sub) {
                    if (std::find_if(result.begin(), result.end(), [&i] (auto& o) { return i->key_size() == o->key_size() && i->key() == o->key(); }) == result.end()) {
                        result.emplace_back(std::move(i));
                    }
                }
            });
        }).then([&state] {
            return make_ready_future<std::vector<item_ptr>>(std::move(state.result));
       });
   });
}

future<std::vector<item_ptr>> redis_service::sunion(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>();
    }
    if (args._command_args_count == 1) {
        return smembers(args);
    }
    return sunion_impl(std::move(args._command_args));
}

future<std::vector<item_ptr>> redis_service::sunion_store(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return make_ready_future<std::vector<item_ptr>>();
    }
    sstring& dest = args._command_args[0];
    std::vector<sstring> keys;
    for (size_t i = 1; i < args._command_args.size(); ++i) {
        keys.emplace_back(std::move(args._command_args[i]));
    }
    struct union_store_state {
        std::vector<item_ptr> result;
        std::vector<sstring> keys;
        sstring dest;
    };
    return do_with(union_store_state{{}, std::move(keys), std::move(dest)}, [this] (auto& state) {
        return this->sunion_impl(std::move(state.keys)).then([this, &state] (auto&& items) {
            std::vector<sstring> members;
            for (item_ptr& item : items) {
               sstring member(item->key().data(), item->key().size());
               members.emplace_back(std::move(member));
            }
            state.result = std::move(items);
            return this->sadds_impl(state.dest, std::move(members)).then([&state] (int discard) {
                (void) discard;
                auto&& result = std::move(state.result);
                return make_ready_future<std::vector<item_ptr>>(std::move(result));
            });
        });
    });
}

future<int> redis_service::srem_impl(sstring& key, sstring& member)
{
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().srem(key, member));
    }
    return _db_peers.invoke_on(cpu, &db::srem, std::ref(key), std::ref(member));
}

future<int> redis_service::sadd_impl(sstring& key, sstring& member)
{
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().sadd(key, member));
    }
    return _db_peers.invoke_on(cpu, &db::sadd, std::ref(key), std::ref(member));
}

future<int> redis_service::smove(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return make_ready_future<int>(0);
    }
    sstring& key = args._command_args[0];
    sstring& dest = args._command_args[1];
    sstring& member = args._command_args[2];
    struct smove_state {
        sstring key;
        sstring dest;
        sstring member;
    };
    return do_with(smove_state{std::move(key), std::move(dest), std::move(member)}, [this] (auto& state) {
        return this->srem_impl(state.key, state.member).then([this, &state] (auto r) {
            if (r == 1) {
                return this->sadd_impl(state.dest, state.member);
            }
            return make_ready_future<int>(r);
        });
   });
}
} /* namespace redis */
