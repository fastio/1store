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
namespace redis {

namespace stdx = std::experimental;
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

future<message> redis_service::set(args_collection& args)
{
    // parse args
    if (args._command_args_count < 2) {
        return syntax_err_message();
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
                return syntax_err_message();
            }
            if ((o[0] == 'e' || o[0] == 'E') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_EX;
                if (v == nullptr) {
                    return syntax_err_message();
                }
                expir = std::atol(v->c_str()) * 1000;
            }
            if ((o[0] == 'p' || o[0] == 'P') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_PX;
                if (v == nullptr) {
                    return syntax_err_message();
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
    return set_impl(key, val, expir, flag).then([] (auto r) {
        return r == 0 ? ok_message() : err_message();
    });
}

future<int> redis_service::remove_impl(sstring& key) {
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().del(key));
    }
    return _db_peers.invoke_on(cpu, &db::del, std::ref(key));
}

future<message> redis_service::del(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return syntax_err_message();
    }
    if (args._command_args.size() == 1) {
        sstring& key = args._command_args[0];
        return remove_impl(key).then([] (auto r) {
            return r == 1 ? one_message() : zero_message();
        });
    }
    else {
        struct mdel_state {
            std::vector<sstring> keys;
            size_t success_count;
        };
        std::vector<sstring> keys;
        for (size_t i = 0; i < args._command_args_count; ++i) {
            keys.emplace_back(args._command_args[i]);
        }
        return do_with(mdel_state{std::move(keys), 0}, [this] (auto& state) {
            return parallel_for_each(std::begin(state.keys), std::end(state.keys), [this, &state] (auto& key) {
                return this->remove_impl(key).then([&state] (auto r) {
                    if (r == 1) state.success_count++;
                });
            }).then([&state] {
                return size_message(state.success_count);
            });
        });
    }
}

future<message> redis_service::mset(args_collection& args)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    if (args._command_args.size() % 2 != 0) {
        return syntax_err_message();
    }
    struct mset_state {
        std::vector<std::pair<sstring, sstring>> key_value_pairs;
        size_t success_count;
    };
    std::vector<std::pair<sstring, sstring>> key_value_pairs;
    auto pair_size = args._command_args.size() / 2;
    for (size_t i = 0; i < pair_size; ++i) {
        key_value_pairs.emplace_back(std::make_pair(std::move(args._command_args[i * 2]), std::move(args._command_args[i * 2 + 1])));
    }
    return do_with(mset_state{std::move(key_value_pairs), 0}, [this] (auto& state) {
        return parallel_for_each(std::begin(state.key_value_pairs), std::end(state.key_value_pairs), [this, &state] (auto& entry) {
            sstring& key = entry.first;
            sstring& value = entry.second;
            return this->set_impl(key, value, 0, 0).then([&state] (auto) {
                state.success_count++ ;
            });
        }).then([&state] {
            return state.key_value_pairs.size() == state.success_count ? ok_message() : err_message();
        });
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

future<message> redis_service::get(args_collection& args)
{
    if (args._command_args_count < 1) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    return get_impl(key).then([] (auto&& u) {
        return item_message<false, true>(std::move(u));
    });
}

future<message> redis_service::mget(args_collection& args)
{
    if (args._command_args_count < 1) {
        return syntax_err_message();
    }
    struct mget_state {
        std::vector<sstring> keys;
        message msg;
    };
    std::vector<sstring> keys;
    for (size_t i = 0; i < args._command_args.size(); ++i) {
        keys.emplace_back(std::move(args._command_args[i]));
    }
    return do_with(mget_state{std::move(keys), {}}, [this] (auto& state) {
        return parallel_for_each(std::begin(state.keys), std::end(state.keys), [this, &state] (sstring& key) {
            return this->get_impl(key).then([&state] (auto&& item) {
                this_type::append_item<false, true>(state.msg, std::move(item));
            });
        }).then([&state] {
            return make_ready_future<message>(std::move(state.msg));
        });
   });
}

future<message> redis_service::strlen(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().strlen(key));
    }
    return _db_peers.invoke_on(cpu, &db::strlen, std::ref(key)).then([] (size_t u) {
        return size_message(u);
    });
}

future<int> redis_service::exists_impl(sstring& key)
{
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().exists(key));
    }
    return _db_peers.invoke_on(cpu, &db::exists, std::ref(key));
}

future<message> redis_service::exists(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return syntax_err_message();
    }
    if (args._command_args_count == 1) {
        sstring& key = args._command_args[0];
        return exists_impl(key).then([] (auto u) {
            return size_message(u);
        });
    }
    else {
        struct mexists_state {
            std::vector<sstring> keys;
            size_t success_count;
        };
        std::vector<sstring> keys;
        for (size_t i = 0; i < args._command_args_count; ++i) {
            keys.emplace_back(args._command_args[i]);
        }
        return do_with(mexists_state{std::move(keys), 0}, [this] (auto& state) {
            return parallel_for_each(std::begin(state.keys), std::end(state.keys), [this, &state] (auto& key) {
                return this->exists_impl(key).then([&state] (auto r) {
                    if (r == 1) state.success_count++;
                });
            }).then([&state] {
                return size_message(state.success_count);
            });
        });
    }
}

future<message> redis_service::append(args_collection& args)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& val = args._command_args[1];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().append(key, val));
    }
    return _db_peers.invoke_on(cpu, &db::append<remote_origin_tag>, std::ref(key), std::ref(val)).then([] (size_t u) {
        return size_message(u);
    });
}

future<int> redis_service::push_impl(sstring& key, sstring& val, bool force, bool left)
{
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().push(key, val, force, left));
    }
    return _db_peers.invoke_on(cpu, &db::push<remote_origin_tag>, std::ref(key), std::ref(val), force, left);
}

future<message> redis_service::push_impl(args_collection& args, bool force, bool left)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    if (args._command_args_count == 2) {
        sstring& value = args._command_args[1];
        return push_impl(key, value, force, left).then([] (auto u) {
            return size_message(u);
        });
    }
    else {
        struct push_state {
            sstring key;
            std::vector<sstring> values;
            size_t success_count;
        };
        std::vector<sstring> values;
        for (size_t i = 1; i < args._command_args.size(); ++i) values.emplace_back(args._command_args[i]);
        return do_with(push_state{std::move(key), std::move(values), 0}, [this, force, left] (auto& state) {
            return parallel_for_each(std::begin(state.values), std::end(state.values), [this, &state, force, left] (auto& value) {
                return this->push_impl(state.key, value, force, left).then([&state] (auto r) {
                    if (r) state.success_count++;
                });
            }).then([&state] () {
                return size_message(state.success_count);
            });
        });
    }
}

future<message> redis_service::lpush(args_collection& args)
{
    return push_impl(args, true, true);
}

future<message> redis_service::lpushx(args_collection& args)
{
    return push_impl(args, false, true);
}

future<message> redis_service::rpush(args_collection& args)
{
    return push_impl(args, true, false);
}

future<message> redis_service::rpushx(args_collection& args)
{
    return push_impl(args, false, false);
}

future<message> redis_service::lpop(args_collection& args)
{
    return pop_impl(args, true);
}

future<message> redis_service::rpop(args_collection& args)
{
    return pop_impl(args, false);
}

future<message> redis_service::pop_impl(args_collection& args, bool left)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return item_message<false, true>(std::move(_db_peers.local().pop(key, left)));
    }
    return _db_peers.invoke_on(cpu, &db::pop, std::ref(key), left).then([] (auto&& item) {
        return item_message<false, true>(std::move(item));
    });
}

future<message> redis_service::lindex(args_collection& args)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    int idx = std::atoi(args._command_args[1].c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return item_message<false, true>(std::move(_db_peers.local().lindex(key, idx)));
    }
    return _db_peers.invoke_on(cpu, &db::lindex, std::ref(key), idx).then([] (auto&& item) {
        return item_message<false, true>(std::move(item));
    });
}

future<message> redis_service::llen(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().llen(key));
    }
    return _db_peers.invoke_on(cpu, &db::llen, std::ref(key)).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::linsert(args_collection& args)
{
    if (args._command_args_count <= 3 || args._command_args.empty()) {
        return syntax_err_message();
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
        return size_message(_db_peers.local().linsert(key, pivot, value, after));
    }
    return _db_peers.invoke_on(cpu, &db::linsert<remote_origin_tag>, std::ref(key), std::ref(pivot), std::ref(value), after).then([] (auto u) {
        return size_message(u);
    });
}

future<message> redis_service::lrange(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& s = args._command_args[1];
    sstring& e = args._command_args[2];
    int start = std::atoi(s.c_str());
    int end = std::atoi(e.c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return items_message<false, true>(std::move(_db_peers.local().lrange(key, start, end)));
    }
    return _db_peers.invoke_on(cpu, &db::lrange, std::ref(key), start, end).then([] (auto&& items) {
        return items_message<false, true>(std::move(items));
    });
}

future<message> redis_service::lset(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& index = args._command_args[1];
    sstring& value = args._command_args[2];
    int idx = std::atoi(index.c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().lset(key, idx, value));
    }
    return _db_peers.invoke_on(cpu, &db::lset<remote_origin_tag>, std::ref(key), idx, std::ref(value)).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::ltrim(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    int start =  std::atoi(args._command_args[1].c_str());
    int stop = std::atoi(args._command_args[2].c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().ltrim(key, start, stop));
    }
    return _db_peers.invoke_on(cpu, &db::ltrim, std::ref(key), start, stop).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::lrem(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    int count = std::atoi(args._command_args[1].c_str());
    sstring& value = args._command_args[2];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().lrem(key, count, value));
    }
    return _db_peers.invoke_on(cpu, &db::lrem, std::ref(key), count, std::ref(value)).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::incr(args_collection& args)
{
    return counter_by(args, true, false);
}

future<message> redis_service::incrby(args_collection& args)
{
    return counter_by(args, true, true);
}
future<message> redis_service::decr(args_collection& args)
{
    return counter_by(args, false, false);
}
future<message> redis_service::decrby(args_collection& args)
{
    return counter_by(args, false, true);
}

future<message> redis_service::counter_by(args_collection& args, bool incr, bool with_step)
{
    if (args._command_args_count < 1 || args._command_args.empty() || (with_step == true && args._command_args_count <= 1)) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    uint64_t step = 1;
    if (with_step) {
        sstring& s = args._command_args[1];
        step = std::atol(s.c_str());
    }
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return uint64_message(_db_peers.local().counter_by(key, step, incr));
    }
    return _db_peers.invoke_on(cpu, &db::counter_by<remote_origin_tag>, std::ref(key), step, incr).then([] (uint64_t u) {
        return uint64_message(u);
    });
}

future<int> redis_service::hdel_impl(sstring& key, sstring& field)
{
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().hdel(key, field));
    }
    return _db_peers.invoke_on(cpu, &db::hdel, std::ref(key), std::ref(field));
}

future<message> redis_service::hdel(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    if (args._command_args_count == 2) {
        sstring& key = args._command_args[0];
        sstring& field = args._command_args[1];
        return hdel_impl(key, field).then([] (auto u) {
            return size_message(u);
        });
    }
    else {
        struct hdel_state {
            sstring key;
            std::vector<sstring> fields;
            size_t success_count;
        };
        std::vector<sstring> fields;
        sstring& key = args._command_args[0];
        for (size_t i = 1; i < args._command_args.size(); ++i) fields.emplace_back(args._command_args[i]);
        return do_with(hdel_state{std::move(key), std::move(fields), 0}, [this] (auto& state) {
            return parallel_for_each(std::begin(state.fields), std::end(state.fields), [this, &state] (auto& field) {
                return this->hdel_impl(state.key, field).then([&state] (auto r) {
                    if (r) state.success_count++;
                });
            }).then([&state] () {
                return size_message(state.success_count);
            });
        });
    }
}

future<message> redis_service::hexists(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().hexists(key, field));
    }
    return _db_peers.invoke_on(cpu, &db::hexists, std::ref(key), std::ref(field)).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::hset(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    sstring& val = args._command_args[2];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().hset(key, field, val));
    }
    return _db_peers.invoke_on(cpu, &db::hset<remote_origin_tag>, std::ref(key), std::ref(field), std::ref(val)).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::hmset(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
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
        return size_message(_db_peers.local().hmset(key, key_values_collection));
    }
    return _db_peers.invoke_on(cpu, &db::hmset<remote_origin_tag>, std::ref(key), std::ref(key_values_collection)).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::hincrby(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    sstring& val = args._command_args[2];
    int delta = std::atoi(val.c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return int64_message(_db_peers.local().hincrby(key, field, delta));
    }
    return _db_peers.invoke_on(cpu, &db::hincrby<remote_origin_tag>, std::ref(key), std::ref(field), delta).then([] (int64_t u) {
        return int64_message(u);
    });
}

future<message> redis_service::hincrbyfloat(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    sstring& val = args._command_args[2];
    double delta = std::atof(val.c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return double_message(_db_peers.local().hincrbyfloat(key, field, delta));
    }
    return _db_peers.invoke_on(cpu, &db::hincrbyfloat<remote_origin_tag>, std::ref(key), std::ref(field), delta).then([] (double u) {
        return double_message(u);
    });
}

future<message> redis_service::hlen(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().hlen(key));
    }
    return _db_peers.invoke_on(cpu, &db::hlen, std::ref(key)).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::hstrlen(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().hstrlen(key, field));
    }
    return _db_peers.invoke_on(cpu, &db::hstrlen, std::ref(key), std::ref(field)).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::hget(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return item_message<false, true>(std::move(_db_peers.local().hget(key, field)));
    }
    return _db_peers.invoke_on(cpu, &db::hget, std::ref(key), std::ref(field)).then([] (auto&& u) {
        return item_message<false, true>(std::move(u));
    });
}

future<message> redis_service::hgetall(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return items_message<true, true>(std::move(_db_peers.local().hgetall(key)));
    }
    return _db_peers.invoke_on(cpu, &db::hgetall, std::ref(key)).then([] (auto&& items) {
        return items_message<true, true>(std::move(items));
    });
}

future<message> redis_service::hmget(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    std::unordered_set<sstring> fields;
    for (unsigned int i = 1; i < args._command_args_count; ++i) {
      fields.emplace(std::move(args._command_args[i]));
    }
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return items_message<false, true>(std::move(_db_peers.local().hmget(key, fields)));
    }
    return _db_peers.invoke_on(cpu, &db::hmget, std::ref(key), std::ref(fields)).then([] (auto&& items) {
        return items_message<false, true>(std::move(items));
    });
}

future<std::vector<item_ptr>> redis_service::smembers_impl(sstring& key)
{
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<std::vector<item_ptr>>(_db_peers.local().smembers(key));
    }
    return _db_peers.invoke_on(cpu, &db::smembers, std::ref(key));
}
future<message> redis_service::smembers(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    return smembers_impl(key).then([] (auto&& items) {
        return items_message<true, false>(std::move(items));
    });
}

future<int> redis_service::sadds_impl(sstring& key, std::vector<sstring>&& members)
{
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db_peers.local().sadds(key, std::move(members)));
    }
    return _db_peers.invoke_on(cpu, &db::sadds<remote_origin_tag>, std::ref(key), std::move(members));
}

future<message> redis_service::sadd(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    std::vector<sstring> members;
    for (uint32_t i = 1; i < args._command_args_count; ++i) members.emplace_back(std::move(args._command_args[i]));
    return sadds_impl(key, std::move(members)).then([] (auto u) {
        return size_message(u);
    });
}

future<message> redis_service::scard(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().scard(key));
    }
    return _db_peers.invoke_on(cpu, &db::scard, std::ref(key)).then([] (auto u) {
        return size_message(u);
    });
}
future<message> redis_service::sismember(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& member = args._command_args[1];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().sismember(key, std::move(member)));
    }
    return _db_peers.invoke_on(cpu, &db::sismember, std::ref(key), std::move(member)).then([] (auto u) {
        return size_message(u);
    });
}

future<message> redis_service::srem(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    std::vector<sstring> members;
    for (uint32_t i = 1; i < args._command_args_count; ++i) members.emplace_back(std::move(args._command_args[i]));
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().srems(key, std::move(members)));
    }
    return _db_peers.invoke_on(cpu, &db::srems, std::ref(key), std::move(members)).then([] (auto u) {
        return size_message(u);
    });
}

future<message> redis_service::sdiff_store(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
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
            return this->sadds_impl(state.dest, std::move(members)).then([&state] (auto u) {
                (void) u;
                return items_message<true, false>(std::move(state.result));
            });
        });
    });
}


future<message> redis_service::sdiff(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    if (args._command_args_count == 1) {
        return smembers(args);
    }
    return sdiff_impl(std::move(args._command_args)).then([] (auto&& items) {
        return items_message<true, false>(std::move(items));
    });
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

future<message> redis_service::sinter(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    if (args._command_args_count == 1) {
        return smembers(args);
    }
    return sinter_impl(std::move(args._command_args)).then([] (auto&& items) {
        return items_message<true, false>(std::move(items));
    });
}

future<message> redis_service::sinter_store(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
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
            return this->sadds_impl(state.dest, std::move(members)).then([&state] (auto u) {
                (void) u;
                return items_message<true, false>(std::move(state.result));
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

future<message> redis_service::sunion(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    if (args._command_args_count == 1) {
        return smembers(args);
    }
    return sunion_impl(std::move(args._command_args)).then([] (auto&& items) {
        return items_message<true, false>(std::move(items));
    });
}

future<message> redis_service::sunion_store(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
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
            return this->sadds_impl(state.dest, std::move(members)).then([&state] (auto u) {
                (void) u;
                return items_message<true, false>(std::move(state.result));
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

future<message> redis_service::smove(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
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
                return this->sadd_impl(state.dest, state.member).then([] (auto u) {
                    return size_message(u);
                });
            }
            return size_message(r);
        });
   });
}

future<message> redis_service::type(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return type_message(_db_peers.local().type(key));
    }
    return _db_peers.invoke_on(cpu, &db::type, std::ref(key)).then([] (auto u) {
        return type_message(u);
    });
}

future<message> redis_service::expire(args_collection& args)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    long expir = std::atol(args._command_args[1].c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().expire(key, expir * 1000));
    }
    return _db_peers.invoke_on(cpu, &db::expire, std::ref(key), expir).then([] (auto u) {
        return size_message(u);
    });
}

future<message> redis_service::pexpire(args_collection& args)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    long expir = std::atol(args._command_args[1].c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().expire(key, expir));
    }
    return _db_peers.invoke_on(cpu, &db::expire, std::ref(key), expir).then([] (auto u) {
        return size_message(u);
    });
}

future<message> redis_service::pttl(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().pttl(key));
    }
    return _db_peers.invoke_on(cpu, &db::pttl, std::ref(key)).then([] (auto u) {
        return size_message(u);
    });
}

future<message> redis_service::ttl(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().ttl(key));
    }
    return _db_peers.invoke_on(cpu, &db::ttl, std::ref(key)).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::persist(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().persist(key));
    }
    return _db_peers.invoke_on(cpu, &db::persist, std::ref(key)).then([] (auto u) {
        return size_message(u);
    });
}

future<message> redis_service::zadd(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    std::string un = args._command_args[1];
    std::transform(un.begin(), un.end(), un.begin(), ::tolower);
    auto zadd_flags = ZADD_NONE;
    size_t first_score_index = 2;
    if (un == "nx") {
        zadd_flags |= ZADD_NX;
    }
    else if (un == "xx") {
        zadd_flags |= ZADD_XX;
    }
    else if (un == "ch") {
        zadd_flags |= ZADD_CH;
    }
    else if (un == "incr") {
        zadd_flags |= ZADD_INCR;
    }
    else {
        first_score_index = 1;
    }
    auto cpu = get_cpu(key);
    if (zadd_flags & ZADD_INCR) {
        if (args._command_args_count - first_score_index > 2) {
            return syntax_err_message();
        }
        sstring& member = args._command_args[first_score_index];
        sstring& delta = args._command_args[first_score_index + 1];
        double score = std::stod(delta.c_str());
        if (engine().cpu_id() == cpu) {
            auto&& u = _db_peers.local().zincrby(key, std::move(member), score);
            return u.second ? double_message(u.first) : err_message();
        }
        else {
            return _db_peers.invoke_on(cpu, &db::zincrby<remote_origin_tag>, std::ref(key), std::move(member), score).then([] (auto&& u) {
                return u.second ? double_message(u.first) : err_message();
            });
        }
    }
    else {
        if ((args._command_args_count - first_score_index) % 2 != 0 || ((zadd_flags & ZADD_NX) && (zadd_flags & ZADD_XX))) {
            return syntax_err_message();
        }
    }
    std::unordered_map<sstring, double> members;
    for (size_t i = first_score_index; i < args._command_args_count; i += 2) {
        sstring& score_ = args._command_args[i];
        sstring& member = args._command_args[i + 1];
        double score = std::stod(score_.c_str());
        members.emplace(std::pair<sstring, double>(member, score));
    }
    if (engine().cpu_id() == cpu) {
        auto&& u = _db_peers.local().zadds(key, std::move(members), zadd_flags);
        if (u.second == REDIS_OK) {
            return size_message(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    }
    else {
        return _db_peers.invoke_on(cpu, &db::zadds<remote_origin_tag>, std::ref(key), std::move(members), zadd_flags).then([] (auto&& u) {
            if (u.second == REDIS_OK) {
                return size_message(u.first);
            }
            else if (u.second == REDIS_WRONG_TYPE) {
                return wrong_type_err_message();
            }
            else {
                return err_message();
            }
        });
    }
}

future<message> redis_service::zcard(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().zcard(key));
    }
    return _db_peers.invoke_on(cpu, &db::zcard, std::ref(key)).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::zrange(args_collection& args, bool reverse)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    size_t begin = std::stol(args._command_args[1].c_str());
    size_t end = std::stol(args._command_args[2].c_str());
    bool with_score = false;
    auto ws = args._command_args[3];
    std::transform(ws.begin(), ws.end(), ws.begin(), ::toupper);
    if (args._command_args_count == 4 && ws == "WITHSCORES") {
        with_score = true;
    }
    if (engine().cpu_id() == cpu) {
        auto&& items = _db_peers.local().zrange(key, begin, end, reverse);
        return with_score ? items_message<true, true>(std::move(items)) : items_message<true, false>(std::move(items)); 
    }
    return _db_peers.invoke_on(cpu, &db::zrange, std::ref(key), begin, end, reverse).then([with_score] (auto&& items) {
        return with_score ? items_message<true, true>(std::move(items)) : items_message<true, false>(std::move(items)); 
    });
}

future<message> redis_service::zrangebyscore(args_collection& args, bool reverse)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    double min = std::stol(args._command_args[1].c_str());
    double max = std::stol(args._command_args[2].c_str());
    bool with_score = false;
    if (args._command_args_count == 4 && args._command_args[3] == "WITHSCORES") {
        with_score = true;
    }
    if (engine().cpu_id() == cpu) {
        auto&& items = _db_peers.local().zrangebyscore(key, min, max, reverse);
        return with_score ? items_message<true, true>(std::move(items)) : items_message<true, false>(std::move(items)); 
    }
    return _db_peers.invoke_on(cpu, &db::zrangebyscore, std::ref(key), min, max, reverse).then([with_score] (auto&& items) {
        return with_score ? items_message<true, true>(std::move(items)) : items_message<true, false>(std::move(items)); 
    });
}

future<message> redis_service::zcount(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    double min = std::stod(args._command_args[1].c_str());
    double max = std::stod(args._command_args[2].c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().zcount(key, min, max));
    }
    return _db_peers.invoke_on(cpu, &db::zcount, std::ref(key), min, max).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::zincrby(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& member = args._command_args[1];
    double delta = std::stod(args._command_args[2].c_str());
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        auto&& result = _db_peers.local().zincrby(key, std::move(member), delta);
        if (result.second) {
            return double_message(result.first);
        }
        return wrong_type_err_message();
    }
    return _db_peers.invoke_on(cpu, &db::zincrby, std::ref(key), std::move(member), delta).then([] (auto u) {
        if (u.second) {
            return double_message(u.first);
        }
        return wrong_type_err_message();
    });
}

future<message> redis_service::zrank(args_collection& args, bool reverse)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& member = args._command_args[1];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        auto u = _db_peers.local().zrank(key, std::move(member), reverse);
        return u >= 0 ? size_message(u) : nil_message();
    }
    return _db_peers.invoke_on(cpu, &db::zrank, std::ref(key), std::move(member), reverse).then([] (auto u) {
        return u >= 0 ? size_message(u) : nil_message();
    });
}

future<message> redis_service::zrem(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    std::vector<sstring> members;
    for (size_t i = 1; i < args._command_args_count; ++i) {
        sstring& member = args._command_args[i];
        members.emplace_back(std::move(member));
    }
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        return size_message(_db_peers.local().zrem(key, std::move(members)));
    }
    else {
        return _db_peers.invoke_on(cpu, &db::zrem, std::ref(key), std::move(members)).then([] (auto u) {
            return size_message(u);
        });
    }
}

future<message> redis_service::zscore(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& member = args._command_args[1];
    auto cpu = get_cpu(key);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db_peers.local().zscore(key, std::move(member));
        return u.second ? double_message(u.first) : nil_message();
    }
    else {
        return _db_peers.invoke_on(cpu, &db::zscore, std::ref(key), std::move(member)).then([] (auto&& u) {
            return u.second ? double_message(u.first) : nil_message();
        });
    }
}

future<message> redis_service::zunionstore(args_collection&)
{
    return syntax_err_message();
}

future<message> redis_service::zinterstore(args_collection&)
{
    return syntax_err_message();
}

future<message> redis_service::zdiffstore(args_collection&)
{
    return syntax_err_message();
}

future<message> redis_service::zunion(args_collection&)
{
    return syntax_err_message();
}

future<message> redis_service::zinter(args_collection&)
{
    return syntax_err_message();
}

future<message> redis_service::zdiff(args_collection&)
{
    return syntax_err_message();
}

future<message> redis_service::zrangebylex(args_collection&)
{
    return syntax_err_message();
}

future<message> redis_service::zlexcount(args_collection&)
{
    return syntax_err_message();
}

} /* namespace redis */
