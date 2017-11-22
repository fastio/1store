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
*  Copyright (c) 2016-2026, Peng Jian, pengjian.uestc@gmail.com. All rights reserved.
*
*/
#include "redis.hh"
#include <iomanip>
#include <sstream>
#include <algorithm>
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
#include "util/log.hh"
#include <unistd.h>
#include <cstdlib>
#include "db.hh"
#include "reply_builder.hh"
#include  <experimental/vector>
#include "core/metrics.hh"
using namespace net;
namespace redis {


//using logger =  seastar::logger;
//static logger redis_log ("redis");

namespace stdx = std::experimental;

future<bytes> redis_service::echo(request_wrapper& req)
{
    if (req._args_count < 1) {
        return make_ready_future<bytes>("");
    }
    bytes& message  = req._args[0];
    return make_ready_future<bytes>(std::move(message));
}

future<scattered_message_ptr> redis_service::ping(request_wrapper& req)
{
    return reply_builder::build(msg_pong);
}

future<scattered_message_ptr> redis_service::command(request_wrapper& req)
{
    return reply_builder::build(msg_ok);
}

future<bool> redis_service::set_impl(bytes& key, bytes& val, long expir, uint8_t flag)
{
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::set_direct, std::move(rk), std::ref(val), expir, flag).then([] (auto&& m) {
        return m == REDIS_OK;
    });
}

future<scattered_message_ptr> redis_service::set(request_wrapper& req)
{
    // parse args
    if (req._args_count < 2) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& val = req._args[1];
    long expir = 0;
    uint8_t flag = FLAG_SET_NO;
    // [EX seconds] [PS milliseconds] [NX] [XX]
    if (req._args_count > 2) {
        for (unsigned int i = 2; i < req._args_count; ++i) {
            bytes* v = (i == req._args_count - 1) ? nullptr : &(req._args[i + 1]);
            bytes& o = req._args[i];
            if (o.size() != 2) {
                return reply_builder::build(msg_syntax_err);
            }
            if ((o[0] == 'e' || o[0] == 'E') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_EX;
                if (v == nullptr) {
                    return reply_builder::build(msg_syntax_err);
                }
                try {
                    expir = std::atol(v->c_str()) * 1000;
                } catch (const std::invalid_argument&) {
                    return reply_builder::build(msg_syntax_err);
                }
                i++;
            }
            if ((o[0] == 'p' || o[0] == 'P') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_PX;
                if (v == nullptr) {
                    return reply_builder::build(msg_syntax_err);
                }
                try {
                    expir = std::atol(v->c_str());
                } catch (const std::invalid_argument&) {
                    return reply_builder::build(msg_syntax_err);
                }
                i++;
            }
            if ((o[0] == 'n' || o[0] == 'N') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_NX;
            }
            if ((o[0] == 'x' || o[0] == 'X') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_XX;
            }
        }
    }
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::set, std::move(rk), std::ref(val), expir, flag);
}

future<bool> redis_service::remove_impl(bytes& key) {
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::del_direct, std::move(rk));
}

future<scattered_message_ptr> redis_service::del(request_wrapper& req)
{
    if (req._args_count <= 0 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    if (req._args.size() == 1) {
        bytes& key = req._args[0];
        return remove_impl(key).then([] (auto r) {
            return reply_builder::build(r ? msg_one : msg_zero);
        });
    }
    else {
        struct mdel_state {
            std::vector<bytes>& keys;
            size_t success_count;
        };
        for (size_t i = 0; i < req._args_count; ++i) {
            req._tmp_keys.emplace_back(req._args[i]);
        }
        return do_with(mdel_state{req._tmp_keys, 0}, [this] (auto& state) {
            return parallel_for_each(std::begin(state.keys), std::end(state.keys), [this, &state] (auto& key) {
                return this->remove_impl(key).then([&state] (auto r) {
                    if (r) state.success_count++;
                });
            }).then([&state] {
                return reply_builder::build(state.success_count);
            });
        });
    }
}

future<scattered_message_ptr> redis_service::mset(request_wrapper& req)
{
    if (req._args_count <= 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    if (req._args.size() % 2 != 0) {
        return reply_builder::build(msg_syntax_err);
    }
    struct mset_state {
        std::vector<std::pair<bytes, bytes>>& key_value_pairs;
        size_t success_count;
    };
    auto pair_size = req._args.size() / 2;
    for (size_t i = 0; i < pair_size; ++i) {
        req._tmp_key_value_pairs.emplace_back(std::make_pair(std::move(req._args[i * 2]), std::move(req._args[i * 2 + 1])));
    }
    return do_with(mset_state{std::ref(req._tmp_key_value_pairs), 0}, [this] (auto& state) {
        return parallel_for_each(std::begin(state.key_value_pairs), std::end(state.key_value_pairs), [this, &state] (auto& entry) {
            bytes& key = entry.first;
            bytes& value = entry.second;
            redis_key rk {std::ref(key)};
            return get_database().invoke_on(this->get_cpu(rk), &database::set_direct, std::move(rk), std::ref(value), 0, FLAG_SET_NO).then([&state] (auto m) {
                if (m) state.success_count++ ;
            });
        }).then([&state] {
            return reply_builder::build(state.key_value_pairs.size() == state.success_count ? msg_ok : msg_err);
        });
   });
}

future<scattered_message_ptr> redis_service::get(request_wrapper& req)
{
    if (req._args_count < 1) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::get, std::move(rk));
}

future<scattered_message_ptr> redis_service::mget(request_wrapper& req)
{
    if (req._args_count < 1) {
        return reply_builder::build(msg_syntax_err);
    }
    using return_type = foreign_ptr<lw_shared_ptr<bytes>>;
    struct mget_state {
        std::vector<bytes> keys;
        std::vector<return_type> values;
    };
    for (size_t i = 0; i < req._args_count; ++i) {
        req._tmp_keys.emplace_back(req._args[i]);
    }
    return do_with(mget_state{std::move(req._tmp_keys), {}}, [this] (auto& state) {
        return parallel_for_each(std::begin(state.keys), std::end(state.keys), [this, &state] (bytes& key) {
            redis_key rk { std::ref(key) };
            return get_database().invoke_on(this->get_cpu(rk), &database::get_direct, std::move(rk)).then([&state] (auto&& m) {
                if (m) {
                   state.values.emplace_back(std::move(m));
                }
            });
        }).then([&state] {
            return reply_builder::build(state.values);
        });
    });
}

future<scattered_message_ptr> redis_service::strlen(request_wrapper& req)
{
    if (req._args_count <= 0 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::strlen, std::ref(rk));
}

future<bool> redis_service::exists_impl(bytes& key)
{
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::exists_direct, std::move(rk));
}

future<scattered_message_ptr> redis_service::exists(request_wrapper& req)
{
    if (req._args_count <= 0 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    if (req._args_count == 1) {
        bytes& key = req._args[0];
        return exists_impl(key).then([] (auto r) {
            return reply_builder::build( r ? msg_one : msg_zero);
        });
    }
    else {
        struct mexists_state {
            std::vector<bytes>& keys;
            size_t success_count;
        };
        for (size_t i = 0; i < req._args_count; ++i) {
            req._tmp_keys.emplace_back(req._args[i]);
        }
        return do_with(mexists_state{std::ref(req._tmp_keys), 0}, [this] (auto& state) {
            return parallel_for_each(std::begin(state.keys), std::end(state.keys), [this, &state] (auto& key) {
                return this->exists_impl(key).then([&state] (auto r) {
                    if (r) state.success_count++;
                });
            }).then([&state] {
                return reply_builder::build(state.success_count);
            });
        });
    }
}

future<scattered_message_ptr> redis_service::append(request_wrapper& req)
{
    if (req._args_count <= 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& val = req._args[1];
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::append, std::move(rk), std::ref(val));
}

future<scattered_message_ptr> redis_service::push_impl(bytes& key, bytes& val, bool force, bool left)
{
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::push, std::move(rk), std::ref(val), force, left);
}

future<scattered_message_ptr> redis_service::push_impl(bytes& key, std::vector<bytes>& vals, bool force, bool left)
{
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::push_multi, std::move(rk), std::ref(vals), force, left);
}

future<scattered_message_ptr> redis_service::push_impl(request_wrapper& req, bool force, bool left)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    if (req._args_count == 2) {
        bytes& value = req._args[1];
        return push_impl(key, value, force, left);
    }
    else {
        for (size_t i = 1; i < req._args.size(); ++i) req._tmp_keys.emplace_back(req._args[i]);
        return push_impl(key, req._tmp_keys, force, left);
    }
}

future<scattered_message_ptr> redis_service::lpush(request_wrapper& req)
{
    return push_impl(req, true, false);
}

future<scattered_message_ptr> redis_service::lpushx(request_wrapper& req)
{
    return push_impl(req, false, false);
}

future<scattered_message_ptr> redis_service::rpush(request_wrapper& req)
{
    return push_impl(req, true, true);
}

future<scattered_message_ptr> redis_service::rpushx(request_wrapper& req)
{
    return push_impl(req, false, true);
}

future<scattered_message_ptr> redis_service::lpop(request_wrapper& req)
{
    return pop_impl(req, false);
}

future<scattered_message_ptr> redis_service::rpop(request_wrapper& req)
{
    return pop_impl(req, true);
}

future<scattered_message_ptr> redis_service::pop_impl(request_wrapper& req, bool left)
{
    if (req._args_count <= 0 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::pop, std::move(rk), left);
}

future<scattered_message_ptr> redis_service::lindex(request_wrapper& req)
{
    if (req._args_count <= 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    int idx = std::atoi(req._args[1].c_str());
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::lindex, std::move(rk), idx);
}

future<scattered_message_ptr> redis_service::llen(request_wrapper& req)
{
    if (req._args_count <= 0 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    auto cpu = get_cpu(key);
    redis_key rk {std::ref(key)};
    return get_database().invoke_on(cpu, &database::llen, std::move(rk));
}

future<scattered_message_ptr> redis_service::linsert(request_wrapper& req)
{
    if (req._args_count <= 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& dir = req._args[1];
    bytes& pivot = req._args[2];
    bytes& value = req._args[3];
    std::transform(dir.begin(), dir.end(), dir.begin(), ::toupper);
    bool after = true;
    if (dir == "BEFORE") after = false;
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::linsert, std::move(rk), std::ref(pivot), std::ref(value), after);
}

future<scattered_message_ptr> redis_service::lrange(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& s = req._args[1];
    bytes& e = req._args[2];
    int start = std::atoi(s.c_str());
    int end = std::atoi(e.c_str());
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::lrange, std::move(rk), start, end);
}

future<scattered_message_ptr> redis_service::lset(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& index = req._args[1];
    bytes& value = req._args[2];
    int idx = std::atoi(index.c_str());
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::lset, std::move(rk), idx, std::ref(value));
}

future<scattered_message_ptr> redis_service::ltrim(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    int start =  std::atoi(req._args[1].c_str());
    int stop = std::atoi(req._args[2].c_str());
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::ltrim, std::move(rk), start, stop);
}

future<scattered_message_ptr> redis_service::lrem(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    int count = std::atoi(req._args[1].c_str());
    bytes& value = req._args[2];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::lrem, std::move(rk), count, std::ref(value));
}

future<scattered_message_ptr> redis_service::incr(request_wrapper& req)
{
    return counter_by(req, true, false);
}

future<scattered_message_ptr> redis_service::incrby(request_wrapper& req)
{
    return counter_by(req, true, true);
}
future<scattered_message_ptr> redis_service::decr(request_wrapper& req)
{
    return counter_by(req, false, false);
}
future<scattered_message_ptr> redis_service::decrby(request_wrapper& req)
{
    return counter_by(req, false, true);
}

future<scattered_message_ptr> redis_service::counter_by(request_wrapper& req, bool incr, bool with_step)
{
    if (req._args_count < 1 || req._args.empty() || (with_step == true && req._args_count <= 1)) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    uint64_t step = 1;
    if (with_step) {
        bytes& s = req._args[1];
        try {
            step = std::atol(s.c_str());
        } catch (const std::invalid_argument&) {
            return reply_builder::build(msg_syntax_err);
        }
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::counter_by, std::move(rk), step, incr);
}

future<scattered_message_ptr> redis_service::hdel(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& field = req._args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    if (req._args_count == 2) {
        return get_database().invoke_on(cpu, &database::hdel, std::move(rk), std::ref(field));
    }
    else {
        for (size_t i = 1; i < req._args.size(); ++i) req._tmp_keys.emplace_back(req._args[i]);
        auto& keys = req._tmp_keys;
        return get_database().invoke_on(cpu, &database::hdel_multi, std::move(rk), std::ref(keys));
    }
}

future<scattered_message_ptr> redis_service::hexists(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& field = req._args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::hexists, std::move(rk), std::ref(field));
}

future<scattered_message_ptr> redis_service::hset(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& field = req._args[1];
    bytes& val = req._args[2];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::hset, std::move(rk), std::ref(field), std::ref(val));
}

future<scattered_message_ptr> redis_service::hmset(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    unsigned int field_count = (req._args_count - 1) / 2;
    bytes& key = req._args[0];
    for (unsigned int i = 0; i < field_count; ++i) {
        req._tmp_key_values.emplace(std::make_pair(req._args[i], req._args[i + 1]));
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::hmset, std::move(rk), std::ref(req._tmp_key_values));
}

future<scattered_message_ptr> redis_service::hincrby(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& field = req._args[1];
    bytes& val = req._args[2];
    int delta = std::atoi(val.c_str());
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::hincrby, std::move(rk), std::ref(field), delta);
}

future<scattered_message_ptr> redis_service::hincrbyfloat(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& field = req._args[1];
    bytes& val = req._args[2];
    double delta = std::atof(val.c_str());
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::hincrbyfloat, std::move(rk), std::ref(field), delta);
}

future<scattered_message_ptr> redis_service::hlen(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::hlen, std::move(rk));
}

future<scattered_message_ptr> redis_service::hstrlen(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& field = req._args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::hstrlen, std::move(rk), std::ref(field));
}

future<scattered_message_ptr> redis_service::hget(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& field = req._args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::hget, std::move(rk), std::ref(field));
}

future<scattered_message_ptr> redis_service::hgetall(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::hgetall, std::move(rk));
}

future<scattered_message_ptr> redis_service::hgetall_keys(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::hgetall_keys, std::move(rk));
}

future<scattered_message_ptr> redis_service::hgetall_values(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::hgetall_values, std::move(rk));
}

future<scattered_message_ptr> redis_service::hmget(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    for (unsigned int i = 1; i < req._args_count; ++i) {
        req._tmp_keys.emplace_back(std::move(req._args[i]));
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    auto& keys = req._tmp_keys;
    return get_database().invoke_on(cpu, &database::hmget, std::move(rk), std::ref(keys));
}

future<scattered_message_ptr> redis_service::smembers_impl(bytes& key)
{
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::smembers, std::move(rk));
}

future<scattered_message_ptr> redis_service::smembers(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    return smembers_impl(key);
}

future<scattered_message_ptr> redis_service::sadds_impl(bytes& key, std::vector<bytes>& members)
{
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::sadds, std::move(rk), std::ref(members));
}

future<scattered_message_ptr> redis_service::sadds_impl_return_keys(bytes& key, std::vector<bytes>& members)
{
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::sadds_direct, std::move(rk), std::ref(members)).then([&members] (auto m) {
        if (m)
           return reply_builder::build(members);
        return reply_builder::build(msg_err);
    });
}

future<scattered_message_ptr> redis_service::sadd(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    for (uint32_t i = 1; i < req._args_count; ++i) req._tmp_keys.emplace_back(std::move(req._args[i]));
    auto& keys = req._tmp_keys;
    return sadds_impl(key, std::ref(keys));
}

future<scattered_message_ptr> redis_service::scard(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::scard, std::move(rk));
}
future<scattered_message_ptr> redis_service::sismember(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& member = req._args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::sismember, std::move(rk), std::ref(member));
}

future<scattered_message_ptr> redis_service::srem(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    for (uint32_t i = 1; i < req._args_count; ++i) req._tmp_keys.emplace_back(std::move(req._args[i]));
    auto& keys = req._tmp_keys;
    return get_database().invoke_on(cpu, &database::srems, std::move(rk), std::ref(keys));
}

future<scattered_message_ptr> redis_service::sdiff_store(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& dest = req._args[0];
    for (size_t i = 1; i < req._args.size(); ++i) {
        req._tmp_keys.emplace_back(std::move(req._args[i]));
    }
    return sdiff_impl(req._tmp_keys, &dest);
}

future<scattered_message_ptr> redis_service::sdiff(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    if (req._args_count == 1) {
        return smembers(req);
    }
    return sdiff_impl(std::ref(req._args), nullptr);
}

future<scattered_message_ptr> redis_service::sdiff_impl(std::vector<bytes>& keys, bytes* dest)
{
    using item_unordered_map = std::unordered_map<unsigned, std::vector<bytes>>;
    struct sdiff_state {
        item_unordered_map items_set;
        std::vector<bytes>& keys;
        bytes* dest = nullptr;
        std::vector<bytes> result;
    };
    uint32_t count = static_cast<uint32_t>(keys.size());
    return do_with(sdiff_state{item_unordered_map{}, std::ref(keys), dest, {}}, [this, count] (auto& state) {
        return parallel_for_each(boost::irange<unsigned>(0, count), [this, &state] (unsigned k) {
            bytes& key = state.keys[k];
            redis_key rk { std::ref(key) };
            auto cpu = this->get_cpu(rk);
            return get_database().invoke_on(cpu, &database::smembers_direct, std::move(rk)).then([&state, index = k] (auto&& members) {
                state.items_set[index] = std::move(*members);
            });
        }).then([this,&state, count] {
            auto& temp = state.items_set[0];
            for (uint32_t i = 1; i < count; ++i) {
                auto&& next_items = std::move(state.items_set[i]);
                for (auto& item : next_items) {
                    stdx::erase_if(temp, [&item] (auto& o) { return item == o; });
                }
            }
            std::vector<bytes>& result = temp;
            if (state.dest) {
                return this->sadds_impl_return_keys(*state.dest, result);
            }
            return reply_builder::build(result);
        });
    });
}

future<scattered_message_ptr> redis_service::sinter_impl(std::vector<bytes>& keys, bytes* dest)
{
    using item_unordered_map = std::unordered_map<unsigned, std::vector<bytes>>;
    struct sinter_state {
        item_unordered_map items_set;
        std::vector<bytes>& keys;
        bytes* dest = nullptr;
    };
    uint32_t count = static_cast<uint32_t>(keys.size());
    return do_with(sinter_state{item_unordered_map{}, std::ref(keys), dest}, [this, count] (auto& state) {
        return parallel_for_each(boost::irange<unsigned>(0, count), [this, &state] (unsigned k) {
            bytes& key = state.keys[k];
            redis_key rk { std::ref(key) };
            auto cpu = this->get_cpu(rk);
            return get_database().invoke_on(cpu, &database::smembers_direct, std::move(rk)).then([&state, index = k] (auto&& members) {
                state.items_set[index] = std::move(*members);
            });
        }).then([this, &state, count] {
            auto& result = state.items_set[0];
            for (uint32_t i = 1; i < count; ++i) {
                auto& next_items = state.items_set[i];
                if (result.empty() || next_items.empty()) {
                    return reply_builder::build(result);
                }
                std::vector<bytes> temp;
                for (auto& item : next_items) {
                    if (std::find_if(result.begin(), result.end(), [&item] (auto& o) { return item == o; }) != result.end()) {
                        temp.emplace_back(std::move(item));
                    }
                }
                result = std::move(temp);
            }
            if (state.dest) {
                return this->sadds_impl_return_keys(*state.dest, result);
            }
            return reply_builder::build(result);
       });
   });
}

future<scattered_message_ptr> redis_service::sinter(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    if (req._args_count == 1) {
        return smembers(req);
    }
    return sinter_impl(req._args, nullptr);
}

future<scattered_message_ptr> redis_service::sinter_store(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& dest = req._args[0];
    for (size_t i = 1; i < req._args.size(); ++i) {
        req._tmp_keys.emplace_back(std::move(req._args[i]));
    }
    return sinter_impl(req._tmp_keys, &dest);
}

future<scattered_message_ptr> redis_service::sunion_impl(std::vector<bytes>& keys, bytes* dest)
{
    struct union_state {
        std::vector<bytes> result;
        std::vector<bytes>& keys;
        bytes* dest = nullptr;
    };
    uint32_t count = static_cast<uint32_t>(keys.size());
    return do_with(union_state{{}, std::ref(keys), dest}, [this, count] (auto& state) {
        return parallel_for_each(boost::irange<unsigned>(0, count), [this, &state] (unsigned k) {
            bytes& key = state.keys[k];
            redis_key rk { std::ref(key) };
            auto cpu = this->get_cpu(rk);
            return get_database().invoke_on(cpu, &database::smembers_direct, std::move(rk)).then([&state] (auto&& members) {
                auto& result = state.result;
                for (auto& item : *members) {
                    if (std::find_if(result.begin(), result.end(), [&item] (auto& o) { return o == item; }) == result.end()) {
                        result.emplace_back(std::move(item));
                    }
                }
            });
        }).then([this, &state] {
            auto& result = state.result;
            if (state.dest) {
                return this->sadds_impl_return_keys(*state.dest, result);
            }
            return reply_builder::build(result);
        });
    });
}

future<scattered_message_ptr> redis_service::sunion(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    if (req._args_count == 1) {
        return smembers(req);
    }
    return sunion_impl(req._args, nullptr);
}

future<scattered_message_ptr> redis_service::sunion_store(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& dest = req._args[0];
    for (size_t i = 1; i < req._args.size(); ++i) {
        req._tmp_keys.emplace_back(std::move(req._args[i]));
    }
    return sunion_impl(req._tmp_keys, &dest);
}

future<bool> redis_service::srem_direct(bytes& key, bytes& member)
{
    redis_key rk {std::ref(key) };
    auto cpu = get_cpu(rk);
    return   get_database().invoke_on(cpu, &database::srem_direct, rk, std::ref(member));
}

future<bool> redis_service::sadd_direct(bytes& key, bytes& member)
{
    redis_key rk {std::ref(key) };
    auto cpu = get_cpu(rk);
    return  get_database().invoke_on(cpu, &database::sadd_direct, rk, std::ref(member));
}

future<scattered_message_ptr> redis_service::smove(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& dest = req._args[1];
    bytes& member = req._args[2];
    struct smove_state {
        bytes& src;
        bytes& dst;
        bytes& member;
    };
    return do_with(smove_state{std::ref(key), std::ref(dest), std::ref(member)}, [this] (auto& state) {
        return this->srem_direct(state.src, state.member).then([this, &state] (auto u) {
            if (u) {
                return this->sadd_direct(state.dst, state.member).then([] (auto m) {
                    return reply_builder::build(m ? msg_one : msg_zero);
                });
            }
            return reply_builder::build(msg_zero);
        });
    });
}

future<scattered_message_ptr> redis_service::srandmember(request_wrapper& req)
{
    if (req._args_count <= 0 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    size_t count = 1;
    if (req._args_count > 1) {
        try {
            count  = std::stol(req._args[1].c_str());
        } catch (const std::invalid_argument&) {
            return reply_builder::build(msg_syntax_err);
        }
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::srandmember, rk, count);
}

future<scattered_message_ptr> redis_service::spop(request_wrapper& req)
{
    if (req._args_count <= 0 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    size_t count = 1;
    if (req._args_count > 1) {
        try {
            count  = std::stol(req._args[1].c_str());
        } catch (const std::invalid_argument&) {
            return reply_builder::build(msg_syntax_err);
        }
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::spop, rk, count);
}

future<scattered_message_ptr> redis_service::type(request_wrapper& req)
{
    if (req._args_count <= 0 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::type, std::move(rk));
}

future<scattered_message_ptr> redis_service::expire(request_wrapper& req)
{
    if (req._args_count <= 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    long expir = 0;
    try {
        expir = std::atol(req._args[1].c_str());
        expir *= 1000;
    } catch (const std::invalid_argument&) {
        return reply_builder::build(msg_syntax_err);
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::expire, std::move(rk), expir);
}

future<scattered_message_ptr> redis_service::pexpire(request_wrapper& req)
{
    if (req._args_count <= 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    long expir = 0;
    try {
        expir = std::atol(req._args[1].c_str());
    } catch (const std::invalid_argument&) {
        return reply_builder::build(msg_syntax_err);
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::expire, std::move(rk), expir);
}

future<scattered_message_ptr> redis_service::pttl(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::pttl, std::move(rk));
}

future<scattered_message_ptr> redis_service::ttl(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::ttl, std::move(rk));
}

future<scattered_message_ptr> redis_service::persist(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::persist, std::move(rk));
}

future<scattered_message_ptr> redis_service::zadd(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    std::string un = req._args[1];
    std::transform(un.begin(), un.end(), un.begin(), ::tolower);
    int zadd_flags = ZADD_CH;
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
    if (zadd_flags & ZADD_INCR) {
        if (req._args_count - first_score_index > 2) {
            return reply_builder::build(msg_syntax_err);
        }
        redis_key rk{std::ref(key)};
        auto cpu = get_cpu(rk);
        bytes& member = req._args[first_score_index + 1];
        bytes& delta = req._args[first_score_index];
        double score = 0;
        try {
            score = std::stod(delta.c_str());
        } catch (const std::invalid_argument&) {
            return reply_builder::build(msg_syntax_err);
        }
        return get_database().invoke_on(cpu, &database::zincrby, std::move(rk), std::ref(member), score);
    }
    else {
        if ((req._args_count - first_score_index) % 2 != 0 || ((zadd_flags & ZADD_NX) && (zadd_flags & ZADD_XX))) {
            return reply_builder::build(msg_syntax_err);
        }
    }
    for (size_t i = first_score_index; i < req._args_count; i += 2) {
        bytes& score_ = req._args[i];
        bytes& member = req._args[i + 1];
        double score = 0;
        try {
            score = std::stod(score_.c_str());
        } catch (const std::invalid_argument&) {
            return reply_builder::build(msg_syntax_err);
        }
        req._tmp_key_scores.emplace(std::pair<bytes, double>(member, score));
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::zadds, std::move(rk), std::ref(req._tmp_key_scores), zadd_flags);
}

future<scattered_message_ptr> redis_service::zcard(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::zcard, std::move(rk));
}

future<scattered_message_ptr> redis_service::zrange(request_wrapper& req, bool reverse)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    long begin = 0, end = 0;
    try {
        begin = std::stoi(req._args[1].c_str());
        end = std::stoi(req._args[2].c_str());
    } catch (const std::invalid_argument&) {
        return reply_builder::build(msg_syntax_err);
    }
    bool with_score = false;
    if (req._args_count == 4) {
        auto ws = req._args[3];
        std::transform(ws.begin(), ws.end(), ws.begin(), ::toupper);
        if (ws == "WITHSCORES") {
            with_score = true;
        }
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::zrange, std::move(rk), begin, end, reverse, with_score);
}

future<scattered_message_ptr> redis_service::zrangebyscore(request_wrapper& req, bool reverse)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    double min = 0, max = 0;
    try {
        min = std::stod(req._args[1].c_str());
        max = std::stod(req._args[2].c_str());
    } catch (const std::invalid_argument&) {
        return reply_builder::build(msg_syntax_err);
    }
    bool with_score = false;
    if (req._args_count == 4) {
        auto ws = req._args[3];
        std::transform(ws.begin(), ws.end(), ws.begin(), ::toupper);
        if (ws == "WITHSCORES") {
            with_score = true;
        }
    }
    return get_database().invoke_on(cpu, &database::zrangebyscore, std::move(rk), min, max, reverse, with_score);
}

future<scattered_message_ptr> redis_service::zcount(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    double min = 0, max = 0;
    try {
        min = std::stod(req._args[1].c_str());
        max = std::stod(req._args[2].c_str());
    } catch (const std::invalid_argument&) {
        return reply_builder::build(msg_syntax_err);
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::zcount, std::move(rk), min, max);
}

future<scattered_message_ptr> redis_service::zincrby(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& member = req._args[2];
    double delta = 0;
    try {
        delta = std::stod(req._args[1].c_str());
    } catch (const std::invalid_argument&) {
        return reply_builder::build(msg_syntax_err);
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::zincrby, std::move(rk), std::ref(member), delta);
}

future<scattered_message_ptr> redis_service::zrank(request_wrapper& req, bool reverse)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& member = req._args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::zrank, std::move(rk), std::ref(member), reverse);
}

future<scattered_message_ptr> redis_service::zrem(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    for (size_t i = 1; i < req._args_count; ++i) {
        bytes& member = req._args[i];
        req._tmp_keys.emplace_back(std::move(member));
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::zrem, std::move(rk), std::ref(req._tmp_keys));
}

future<scattered_message_ptr> redis_service::zscore(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& member = req._args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::zscore, std::move(rk), std::ref(member));
}

bool redis_service::parse_zset_args(request_wrapper& req, zset_args& ureq)
{
    if (req._args_count < 3 || req._args.empty()) {
        return false;
    }
    ureq.dest = std::move(req._args[0]);
    try {
        ureq.numkeys = std::stol(req._args[1].c_str());
    } catch(const std::invalid_argument&) {
        return false;
    }
    size_t index = static_cast<size_t>(ureq.numkeys) + 2;
    if (req._args_count < index) {
        return false;
    }
    for (size_t i = 2; i < index; ++i) {
        ureq.keys.emplace_back(std::move(req._args[i]));
    }
    bool has_weights = false, has_aggregate = false;
    if (index < req._args_count) {
        for (; index < req._args_count; ++index) {
            bytes& syntax = req._args[index];
            if (syntax == "WEIGHTS") {
                index ++;
                if (index + ureq.numkeys > req._args_count) {
                    return false;
                }
                has_weights = true;
                size_t i = index;
                index += ureq.numkeys;
                for (; i < index; ++i) {
                    try {
                        ureq.weights.push_back(std::stod(req._args[i].c_str()));
                    } catch (const std::invalid_argument&) {
                        return false;
                    }
                }
            }
            if (syntax == "AGGREGATE") {
                if (index + 1 > req._args_count) {
                    return false;
                }
                index++;
                bytes& aggre = req._args[index];
                if (aggre == "SUM") {
                    ureq.aggregate_flag |= ZAGGREGATE_SUM;
                }
                else if (aggre == "MIN") {
                    ureq.aggregate_flag |= ZAGGREGATE_MIN;
                }
                else if (aggre == "MAX") {
                    ureq.aggregate_flag |= ZAGGREGATE_MAX;
                }
                else {
                    return false;
                }
                has_aggregate = true;
            }
        }
    }
    if (has_weights == false) {
        for (size_t i = 0; i < ureq.numkeys; ++i) {
            ureq.weights.push_back(1);
        }
    }
    if (has_aggregate == false) {
        ureq.aggregate_flag = ZAGGREGATE_SUM;
    }
    return true;
}

future<scattered_message_ptr> redis_service::zunionstore(request_wrapper& req)
{
    zset_args uargs;
    if (parse_zset_args(req, uargs) == false) {
        return reply_builder::build(msg_syntax_err);
    }
    struct zunion_store_state {
        std::vector<std::pair<bytes, double>> wkeys;
        bytes dest;
        std::unordered_map<bytes, double> result;
        int aggregate_flag;
    };
    std::vector<std::pair<bytes, double>> wkeys;
    for (size_t i = 0; i < uargs.numkeys; ++i) {
        wkeys.emplace_back(std::pair<bytes, double>(std::move(uargs.keys[i]), uargs.weights[i]));
    }
    return do_with(zunion_store_state{std::move(wkeys), std::move(uargs.dest), {}, uargs.aggregate_flag}, [this] (auto& state) {
        return parallel_for_each(std::begin(state.wkeys), std::end(state.wkeys), [this, &state] (auto& entry) {
            redis_key rk{std::ref(entry.first)};
            auto cpu = rk.get_cpu();
            return get_database().invoke_on(cpu, &database::zrange_direct, std::move(rk), 0, -1).then([this, weight = entry.second, &state] (auto&& m) {
                auto& range_result = *m;
                auto& result = state.result;
                for (size_t i = 0; i < range_result.size(); ++i) {
                    auto& key = range_result[i].first;
                    auto& score = range_result[i].second;
                    if (result.find(key) != result.end()) {
                       result[key] = redis_service::score_aggregation(result[key], score * weight, state.aggregate_flag);
                    }
                    else {
                        result[key] = score;
                    }
                }
            });
        }).then([this, &state] () {
            redis_key rk{std::ref(state.dest)};
            auto cpu = rk.get_cpu();
            return get_database().invoke_on(cpu, &database::zadds, std::move(rk), std::ref(state.result), ZADD_CH);
        });
    });
}

future<scattered_message_ptr> redis_service::zinterstore(request_wrapper& req)
{
        return reply_builder::build(msg_syntax_err);
/*
    zset_args uargs;
    if (parse_zset_args(req, uargs) == false) {
        return reply_builder::build(msg_syntax_err);
    }
    struct zinter_store_state {
        std::vector<std::pair<bytes, double>> wkeys;
        bytes dest;
        std::unordered_map<bytes, double> result;
        int aggregate_flag;
    };
    std::vector<std::pair<bytes, double>> wkeys;
    for (size_t i = 0; i < uargs.numkeys; ++i) {
        wkeys.emplace_back(std::pair<bytes, double>(std::move(uargs.keys[i]), uargs.weights[i]));
    }
    return do_with(zinter_store_state{std::move(wkeys), std::move(uargs.dest), {}, uargs.aggregate_flag}, [this] (auto& state) {
        redis_key rk{std::ref(state.wkeys[0].first)};
        return get_database().invoke_on(rk.get_cpu(), &database::zrange_direct, std::move(rk), 0, -1).then([this, &state, weight = state.wkeys[0].second] (auto&& m) {
            auto& range_result = *m;
            auto& result = state.result;
            for (size_t i = 0; i < range_result.size(); ++i) {
                result[range_result[i].first] = range_result[i].second * weight;
            }
            return make_ready_future<bool>(!result.empty() && state.wkeys.size() > 1);
        }).then([this, &state] (bool continue_next) {
            if (!continue_next) {
                return make_ready_future<scattered_message_ptr>();
            }
            else {
                return parallel_for_each(boost::irange<size_t>(1, state.wkeys.size()), [this, &state] (size_t k) {
                    auto& entry = state.wkeys[k];
                    redis_key rk{std::ref(entry.first)};
                    auto cpu = rk.get_cpu();
                    return get_database().invoke_on(cpu, &database::zrange_direct, std::move(rk), 0, -1).then([this, &state, weight = entry.second] (auto&& m) {
                        auto& range_result = *m;
                        auto& result = state.result;
                        std::unordered_map<bytes, double> new_result;
                        for (size_t i = 0; i < range_result.size(); ++i) {
                            auto& key = range_result[i].first;
                            auto& score = range_result[i].second;
                            auto it = result.find(key);
                            if (it != result.end()) {
                                auto& old_score = it->second;
                                new_result[key] = redis_service::score_aggregation(old_score, score * weight, state.aggregate_flag);
                            }
                        }
                        state.result = std::move(new_result);
                    });
                }).then([this] () {
                    return make_ready_future<scattered_message_ptr>();
                });
            }
        }).then([this, &state] {
            redis_key rk{std::ref(state.dest)};
            auto cpu = rk.get_cpu();
            return get_database().invoke_on(cpu, &database::zadds, std::move(rk), std::ref(state.result), ZADD_CH);
        });
    });
*/
}

future<scattered_message_ptr> redis_service::zremrangebyscore(request_wrapper& req)
{
    // ZREMRANGEBYSCORE key min max
    // Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
    // Integer reply: the number of elements removed.
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    double min = 0, max = 0;
    try {
        min = std::stod(req._args[1].c_str());
        max = std::stod(req._args[2].c_str());
    } catch (const std::invalid_argument&) {
        return reply_builder::build(msg_syntax_err);
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::zremrangebyscore, std::move(rk), min, max);
}

future<scattered_message_ptr> redis_service::zremrangebyrank(request_wrapper& req)
{
    // ZREMRANGEBYRANK key begin end
    // Removes all elements in the sorted set stored at key with a rank between start and end (inclusive).
    // Integer reply: the number of elements removed.
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    long begin = 0, end = 0;
    try {
        begin = std::stol(req._args[1].c_str());
        end = std::stol(req._args[2].c_str());
    } catch(const std::invalid_argument& e) {
        return reply_builder::build(msg_syntax_err);
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::zremrangebyrank, std::move(rk), begin, end);
}

future<scattered_message_ptr> redis_service::zdiffstore(request_wrapper&)
{
    return reply_builder::build(msg_syntax_err);
}

future<scattered_message_ptr> redis_service::zunion(request_wrapper& req)
{
    return reply_builder::build(msg_syntax_err);
}

future<scattered_message_ptr> redis_service::zinter(request_wrapper& req)
{
    return reply_builder::build(msg_syntax_err);
}

future<scattered_message_ptr> redis_service::zdiff(request_wrapper& req)
{
    return reply_builder::build(msg_syntax_err);
}

future<scattered_message_ptr> redis_service::zrangebylex(request_wrapper& req)
{
    return reply_builder::build(msg_syntax_err);
}

future<scattered_message_ptr> redis_service::zlexcount(request_wrapper& req)
{
    return reply_builder::build(msg_syntax_err);
}

future<scattered_message_ptr> redis_service::select(request_wrapper& req)
{
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    size_t index = 0;
    try {
        index = std::stol(req._args[0].c_str());
    } catch (const std::invalid_argument&) {
        return reply_builder::build(msg_err);
    }
    if (index >= smp::count) {
        return reply_builder::build(msg_syntax_err);
    }
    return do_with(size_t {0}, [this, index] (auto& count) {
        return parallel_for_each(boost::irange<unsigned>(0, smp::count), [this, index, &count] (unsigned cpu) {
            return get_database().invoke_on(cpu, &database::select, index).then([&count] (auto&& u) {
                if (u) {
                    count++;
                }
            });
        }).then([&count] () {
            return reply_builder::build((count == smp::count) ? msg_ok : msg_err);
        });
    });
}


future<scattered_message_ptr> redis_service::geoadd(request_wrapper& req)
{
    if (req._args_count < 4 || (req._args_count - 1) % 3 != 0 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    for (size_t i = 1; i < req._args_count; i += 3) {
        bytes& longitude = req._args[i];
        bytes& latitude = req._args[i + 1];
        bytes& member = req._args[i + 2];
        double longitude_ = 0, latitude_ = 0, score = 0;
        try {
            longitude_ = std::stod(longitude.c_str());
            latitude_ = std::stod(latitude.c_str());
        } catch (const std::invalid_argument&) {
            return reply_builder::build(msg_syntax_err);
        }
        if (geo::encode_to_geohash(longitude_, latitude_, score) == false) {
            return reply_builder::build(msg_err);
        }
        req._tmp_key_scores.emplace(std::pair<bytes, double>(member, score));
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::zadds, std::move(rk), std::ref(req._tmp_key_scores), ZADD_CH);
}

future<scattered_message_ptr> redis_service::geodist(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes& lpos = req._args[1];
    bytes& rpos = req._args[2];
    int geodist_flag = GEODIST_UNIT_M;
    if (req._args_count == 4) {
        bytes& unit = req._args[3];
        if (unit == "km") {
            geodist_flag = GEODIST_UNIT_KM;
        }
        else if (unit == "mi") {
            geodist_flag = GEODIST_UNIT_MI;
        }
        else if (unit == "ft") {
            geodist_flag = GEODIST_UNIT_FT;
        }
        else {
            return reply_builder::build(msg_syntax_err);
        }
    }
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::geodist, std::move(rk), std::ref(lpos), std::ref(rpos), geodist_flag);
}

future<scattered_message_ptr> redis_service::geohash(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    for (size_t i = 1; i < req._args_count; ++i) {
        req._tmp_keys.emplace_back(std::move(req._args[i]));
    }
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::geohash, std::move(rk), std::ref(req._tmp_keys));
}

future<scattered_message_ptr> redis_service::geopos(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    std::vector<bytes> members;
    for (size_t i = 1; i < req._args_count; ++i) {
        req._tmp_keys.emplace_back(std::move(req._args[i]));
    }
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::geopos, std::move(rk), std::ref(members));
}

future<scattered_message_ptr> redis_service::georadius(request_wrapper& req, bool member)
{
    size_t option_index = member ? 4 : 5;
    if (req._args_count < option_index || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    bytes unit{}, member_key{};
    double log = 0, lat = 0, radius = 0;
    if (!member) {
        bytes& longitude = req._args[1];
        bytes& latitude = req._args[2];
        bytes& rad = req._args[3];
        unit = std::move(req._args[4]);
        try {
            log = std::stod(longitude.c_str());
            lat = std::stod(latitude.c_str());
            radius = std::stod(rad.c_str());
        } catch (const std::invalid_argument&) {
            return reply_builder::build(msg_syntax_err);
        }
    }
    else {
        member_key = std::move(req._args[1]);
        bytes& rad = req._args[2];
        unit = std::move(req._args[3]);
        try {
            radius = std::stod(rad.c_str());
        } catch (const std::invalid_argument&) {
            return reply_builder::build(msg_syntax_err);
        }
    }

    int flags = 0;
    size_t count = 0, stored_key_index = 0;
    if (req._args_count > option_index) {
        for (size_t i = option_index; i < req._args_count; ++i) {
            bytes& cc = req._args[i];
            std::transform(cc.begin(), cc.end(), cc.begin(), ::toupper);
            if (cc == "WITHCOORD") {
                flags |= GEORADIUS_WITHCOORD;
            }
            else if (cc == "WITHDIST") {
                flags |= GEORADIUS_WITHDIST;
            }
            else if (cc == "WITHHASH") {
                flags |= GEORADIUS_WITHHASH;
            }
            else if (cc == "COUNT") {
                flags |= GEORADIUS_COUNT;
                if (i + 1 == req._args_count) {
                    return reply_builder::build(msg_syntax_err);
                }
                bytes& c = req._args[++i];
                try {
                    count = std::stol(c.c_str());
                } catch (const std::invalid_argument&) {
                    return reply_builder::build(msg_syntax_err);
                }
            }
            else if (cc == "ASC") {
                flags |= GEORADIUS_ASC;
            }
            else if (cc == "DESC") {
                flags |= GEORADIUS_DESC;
            }
            else if (cc == "STORE") {
                flags |= GEORADIUS_STORE_SCORE;
                if (i + 1 == req._args_count) {
                    return reply_builder::build(msg_syntax_err);
                }
                ++i;
                stored_key_index = i;
            }
            else if (cc == "STOREDIST") {
                flags |= GEORADIUS_STORE_DIST;
                if (i + 1 == req._args_count) {
                    return reply_builder::build(msg_syntax_err);
                }
                ++i;
                stored_key_index = i;
            }
            else {
                return reply_builder::build(msg_syntax_err);
            }
        }
    }
    if (((flags & GEORADIUS_STORE_SCORE) || (flags & GEORADIUS_STORE_DIST)) && (stored_key_index == 0 || stored_key_index >= req._args_count)) {
        return reply_builder::build(msg_syntax_err);
    }
    std::transform(unit.begin(), unit.end(), unit.begin(), ::tolower);
    if (unit == "m") {
        flags |= GEO_UNIT_M;
    }
    else if (unit == "km") {
        flags |= GEO_UNIT_KM;
    }
    else if (unit == "mi") {
        flags |= GEO_UNIT_MI;
    }
    else if (unit == "ft") {
        flags |= GEO_UNIT_FT;
    }
    else {
        return reply_builder::build(msg_syntax_err);
    }
    geo::to_meters(radius, flags);

    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    auto points_ready = !member ? get_database().invoke_on(cpu, &database::georadius_coord_direct, std::move(rk), log, lat, radius, count, flags)
                                : get_database().invoke_on(cpu, &database::georadius_member_direct, std::move(rk), std::ref(member_key), radius, count, flags);
    return  points_ready.then([this, flags, &req, stored_key_index] (auto&& data) {
        using data_type = std::vector<std::tuple<bytes, double, double, double, double>>;
        using return_type = std::pair<std::vector<std::tuple<bytes, double, double, double, double>>, int>;
        return_type& return_data = *data;
        data_type& data_ = return_data.first;
        if (return_data.second == REDIS_WRONG_TYPE) {
            return reply_builder::build(msg_type_err);
        }
        else if (return_data.second == REDIS_ERR) {
            return reply_builder::build(msg_nil);
        }
        bool store_with_score = flags & GEORADIUS_STORE_SCORE, store_with_dist = flags & GEORADIUS_STORE_DIST;
        if (store_with_score || store_with_dist) {
            std::unordered_map<bytes, double> members;
            for (size_t i = 0; i < data_.size(); ++i) {
                auto& data_tuple = data_[i];
                auto& key = std::get<0>(data_tuple);
                auto  score = store_with_score ? std::get<1>(data_tuple) : std::get<2>(data_tuple);
                members[key] = score;
            }
            struct store_state
            {
                std::unordered_map<bytes, double> members;
                bytes& stored_key;
                data_type& data;
            };
            bytes& stored_key = req._args[stored_key_index];
            return do_with(store_state{std::move(members), std::ref(stored_key), std::ref(data_)}, [this, flags, &data_] (auto& state) {
                redis_key rk{std::ref(state.stored_key)};
                auto cpu = rk.get_cpu();
                return get_database().invoke_on(cpu, &database::zadds_direct, std::move(rk), std::ref(state.members), ZADD_CH).then([flags, &data_] (auto&& m) {
                   if (m)
                     return reply_builder::build(data_, flags);
                   else
                      return reply_builder::build(msg_err);
                });
            });
         }
         return reply_builder::build(data_, flags);
    });
}

future<scattered_message_ptr> redis_service::setbit(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    size_t offset = 0;
    int value = 0;
    try {
        offset = std::stol(req._args[1]);
        value = std::stoi(req._args[2]);
    } catch (const std::invalid_argument&) {
        return reply_builder::build(msg_syntax_err);
    }
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::setbit, std::move(rk), offset, value == 1);
}

future<scattered_message_ptr> redis_service::getbit(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    size_t offset = 0;
    try {
        offset = std::stol(req._args[1]);
    } catch (const std::invalid_argument&) {
        return reply_builder::build(msg_syntax_err);
    }
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::getbit, std::move(rk), offset);
}

future<scattered_message_ptr> redis_service::bitcount(request_wrapper& req)
{
    if (req._args_count < 3 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    long start = 0, end = 0;
    try {
        start = std::stol(req._args[1]);
        end = std::stol(req._args[2]);
    } catch (const std::invalid_argument&) {
        return reply_builder::build(msg_syntax_err);
    }
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::bitcount, std::move(rk), start, end);
}

future<scattered_message_ptr> redis_service::bitop(request_wrapper& req)
{
    return reply_builder::build(msg_nil);
}

future<scattered_message_ptr> redis_service::bitpos(request_wrapper& req)
{
    return reply_builder::build(msg_nil);
}

future<scattered_message_ptr> redis_service::bitfield(request_wrapper& req)
{
    return reply_builder::build(msg_nil);
}

future<scattered_message_ptr> redis_service::pfadd(request_wrapper& req)
{
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    bytes& key = req._args[0];
    for (size_t i = 1; i < req._args_count; ++i) {
        req._tmp_keys.emplace_back(req._args[i]);
    }
    redis_key rk {std::ref(key)};
    auto& elements = req._tmp_keys;
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::pfadd, rk, std::ref(elements));
}

future<scattered_message_ptr> redis_service::pfcount(request_wrapper& req)
{
        return reply_builder::build(msg_syntax_err);
    /*
    if (req._args_count < 1 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    if (req._args_count == 1) {
        bytes& key = req._args[0];
        redis_key rk {std::ref(key)};
        auto cpu = get_cpu(rk);
        return get_database().invoke_on(cpu, &database::pfcount, std::move(rk));
    }
    else {
        struct merge_state {
            std::vector<bytes>& keys;
            uint8_t merged_sources[HLL_BYTES_SIZE];
        };
        for (size_t i = 0; i < req._args_count; ++i) {
            req._tmp_keys.emplace_back(req._args[i]);
        }
        return do_with(merge_state{std::ref(req._tmp_keys), { 0 }}, [this] (auto& state) {
            return parallel_for_each(std::begin(state.keys), std::end(state.keys), [this, &state] (auto& key) {
                redis_key rk { std::ref(key) };
                auto cpu = this->get_cpu(rk);
                return get_database().invoke_on(cpu, &database::get_hll_direct, std::move(rk)).then([&state] (auto&& u) {
                    if (u) {
                        hll::merge(state.merged_sources, HLL_BYTES_SIZE, *u);
                    }
                    return make_ready_future<scattered_message_ptr>();
                });
            }).then([this, &state] {
                auto card = hll::count(state.merged_sources, HLL_BYTES_SIZE);
                return reply_builder::build(card);
            });
        });
    }
    */
}

future<scattered_message_ptr> redis_service::pfmerge(request_wrapper& req)
{
        return reply_builder::build(msg_syntax_err);
    /*
    if (req._args_count < 2 || req._args.empty()) {
        return reply_builder::build(msg_syntax_err);
    }
    struct merge_state {
        bytes dest;
        std::vector<bytes>& keys;
        uint8_t merged_sources[HLL_BYTES_SIZE];
    };
    for (size_t i = 1; i < req._args_count; ++i) {
        req._tmp_keys.emplace_back(req._args[i]);
    }
    return do_with(merge_state{std::move(req._args[0]), std::ref(req._tmp_keys), { 0 }}, [this] (auto& state) {
        return parallel_for_each(std::begin(state.keys), std::end(state.keys), [this, &state] (auto& key) {
            redis_key rk { std::ref(key) };
            auto cpu = this->get_cpu(rk);
            return get_database().invoke_on(cpu, &database::get_hll_direct, std::move(rk)).then([&state] (auto&& u) {
                if (u) {
                    hll::merge(state.merged_sources, HLL_BYTES_SIZE, *u);
                }
                return make_ready_future<scattered_message_ptr>();
            });
        }).then([this, &state] {
            redis_key rk { std::ref(state.dest) };
            auto cpu = this->get_cpu(rk);
            return get_database().invoke_on(cpu, &database::pfmerge, std::move(rk), state.merged_sources, HLL_BYTES_SIZE);
        });
    });
   */
}
} /* namespace redis */
