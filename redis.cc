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
#include "redis_protocol.hh"
#include "system_stats.hh"
#include "db.hh"
#include "reply_builder.hh"
#include  <experimental/vector>
using namespace net;
namespace redis {


using logger =  seastar::logger;
static logger redis_log ("redis");

namespace stdx = std::experimental;
future<sstring> redis_service::echo(args_collection& args)
{
    if (args._command_args_count < 1) {
        return make_ready_future<sstring>("");
    }
    sstring& message  = args._command_args[0];
    return make_ready_future<sstring>(std::move(message));
}

future<bool> redis_service::set_impl(sstring& key, sstring& val, long expir, uint8_t flag)
{
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::set_direct, std::move(rk), std::ref(val), expir, flag).then([] (auto&& m) {
        return m == REDIS_OK;
    });
}

future<> redis_service::set(args_collection& args, output_stream<char>& out)
{
    // parse args
    if (args._command_args_count < 2) {
        return out.write(msg_syntax_err);
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
                return out.write(msg_syntax_err);
            }
            if ((o[0] == 'e' || o[0] == 'E') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_EX;
                if (v == nullptr) {
                    return out.write(msg_syntax_err);
                }
                expir = std::atol(v->c_str()) * 1000;
            }
            if ((o[0] == 'p' || o[0] == 'P') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_PX;
                if (v == nullptr) {
                    return out.write(msg_syntax_err);
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
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::set, std::move(rk), std::ref(val), expir, flag).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });;
}

future<bool> redis_service::remove_impl(sstring& key) {
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::del_direct, std::move(rk));
}

future<> redis_service::del(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    if (args._command_args.size() == 1) {
        sstring& key = args._command_args[0];
        return remove_impl(key).then([&out] (auto r) {
            return out.write( r ? msg_one : msg_zero);
        });
    }
    else {
        struct mdel_state {
            std::vector<sstring>& keys;
            size_t success_count;
        };
        for (size_t i = 0; i < args._command_args_count; ++i) {
            args._tmp_keys.emplace_back(args._command_args[i]);
        }
        return do_with(mdel_state{args._tmp_keys, 0}, [this, &out] (auto& state) {
            return parallel_for_each(std::begin(state.keys), std::end(state.keys), [this, &state] (auto& key) {
                return this->remove_impl(key).then([&state] (auto r) {
                    if (r) state.success_count++;
                });
            }).then([&state, &out] {
                return reply_builder::build_local(out, state.success_count);
            });
        });
    }
}

future<> redis_service::mset(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    if (args._command_args.size() % 2 != 0) {
        return out.write(msg_syntax_err);
    }
    struct mset_state {
        std::vector<std::pair<sstring, sstring>>& key_value_pairs;
        size_t success_count;
    };
    auto pair_size = args._command_args.size() / 2;
    for (size_t i = 0; i < pair_size; ++i) {
        args._tmp_key_value_pairs.emplace_back(std::make_pair(std::move(args._command_args[i * 2]), std::move(args._command_args[i * 2 + 1])));
    }
    return do_with(mset_state{std::ref(args._tmp_key_value_pairs), 0}, [this, &out] (auto& state) {
        return parallel_for_each(std::begin(state.key_value_pairs), std::end(state.key_value_pairs), [this, &state] (auto& entry) {
            sstring& key = entry.first;
            sstring& value = entry.second;
            return this->set_impl(key, value, 0, 0).then([&state] (auto u) {
                if (u) state.success_count++ ;
            });
        }).then([&state, &out] {
            return out.write(state.key_value_pairs.size() == state.success_count ? msg_ok : msg_err);
        });
   });
}

    /*
future<item_ptr> redis_service::get_impl(sstring& key)
{
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<item_ptr>(_db.local().get(std::move(rk)));
    }
    return _db.invoke_on(cpu, &database::get, std::move(rk));
    return make_ready_future<item_ptr>();
}
    */

future<> redis_service::get(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::get, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::mget(args_collection& args, output_stream<char>& out)
{
    /*
    if (args._command_args_count < 1) {
        return out.write(msg_syntax_err);
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
   */
    return make_ready_future<>();
}

future<> redis_service::strlen(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::strlen, std::ref(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<bool> redis_service::exists_impl(sstring& key)
{
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::exists_direct, std::move(rk));
}

future<> redis_service::exists(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    if (args._command_args_count == 1) {
        sstring& key = args._command_args[0];
        return exists_impl(key).then([&out] (auto r) {
            return out.write( r ? msg_one : msg_zero);
        });
    }
    else {
        struct mexists_state {
            std::vector<sstring>& keys;
            size_t success_count;
        };
        for (size_t i = 0; i < args._command_args_count; ++i) {
            args._tmp_keys.emplace_back(args._command_args[i]);
        }
        return do_with(mexists_state{std::ref(args._tmp_keys), 0}, [this, &out] (auto& state) {
            return parallel_for_each(std::begin(state.keys), std::end(state.keys), [this, &state] (auto& key) {
                return this->exists_impl(key).then([&state] (auto r) {
                    if (r) state.success_count++;
                });
            }).then([&state, &out] {
                return reply_builder::build_local(out, state.success_count);
            });
        });
    }
}

future<> redis_service::append(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& val = args._command_args[1];
    redis_key rk { std::ref(key) };
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::append, std::move(rk), std::ref(val)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::push_impl(sstring& key, sstring& val, bool force, bool left, output_stream<char>& out)
{
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::push, std::move(rk), std::ref(val), force, left).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::push_impl(sstring& key, std::vector<sstring>& vals, bool force, bool left, output_stream<char>& out)
{
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::push_multi, std::move(rk), std::ref(vals), force, left).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::push_impl(args_collection& args, bool force, bool left, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    if (args._command_args_count == 2) {
        sstring& value = args._command_args[1];
        return push_impl(key, value, force, left, out);
    }
    else {
        for (size_t i = 1; i < args._command_args.size(); ++i) args._tmp_keys.emplace_back(args._command_args[i]);
        return push_impl(key, args._tmp_keys, force, left, out);
    }
}

future<> redis_service::lpush(args_collection& args, output_stream<char>& out)
{
    return push_impl(args, true, false, out);
}

future<> redis_service::lpushx(args_collection& args, output_stream<char>& out)
{
    return push_impl(args, false, false, out);
}

future<> redis_service::rpush(args_collection& args, output_stream<char>& out)
{
    return push_impl(args, true, true, out);
}

future<> redis_service::rpushx(args_collection& args, output_stream<char>& out)
{
    return push_impl(args, false, true, out);
}

future<> redis_service::lpop(args_collection& args, output_stream<char>& out)
{
    return pop_impl(args, false, out);
}

future<> redis_service::rpop(args_collection& args, output_stream<char>& out)
{
    return pop_impl(args, true, out);
}

future<> redis_service::pop_impl(args_collection& args, bool left, output_stream<char>& out)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::pop, std::move(rk), left).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::lindex(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    int idx = std::atoi(args._command_args[1].c_str());
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::lindex, std::move(rk), idx).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::llen(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    redis_key rk {std::ref(key)};
    return _db.invoke_on(cpu, &database::llen, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::linsert(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count <= 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& dir = args._command_args[1];
    sstring& pivot = args._command_args[2];
    sstring& value = args._command_args[3];
    std::transform(dir.begin(), dir.end(), dir.begin(), ::toupper);
    bool after = true;
    if (dir == "BEFORE") after = false;
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::linsert, std::move(rk), std::ref(pivot), std::ref(value), after).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::lrange(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& s = args._command_args[1];
    sstring& e = args._command_args[2];
    int start = std::atoi(s.c_str());
    int end = std::atoi(e.c_str());
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::lrange, std::move(rk), start, end).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::lset(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& index = args._command_args[1];
    sstring& value = args._command_args[2];
    int idx = std::atoi(index.c_str());
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::lset, std::move(rk), idx, std::ref(value)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::ltrim(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    int start =  std::atoi(args._command_args[1].c_str());
    int stop = std::atoi(args._command_args[2].c_str());
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::ltrim, std::move(rk), start, stop).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::lrem(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    int count = std::atoi(args._command_args[1].c_str());
    sstring& value = args._command_args[2];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::lrem, std::move(rk), count, std::ref(value)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::incr(args_collection& args, output_stream<char>& out)
{
    return counter_by(args, true, false, out);
}

future<> redis_service::incrby(args_collection& args, output_stream<char>& out)
{
    return counter_by(args, true, true, out);
}
future<> redis_service::decr(args_collection& args, output_stream<char>& out)
{
    return counter_by(args, false, false, out);
}
future<> redis_service::decrby(args_collection& args, output_stream<char>& out)
{
    return counter_by(args, false, true, out);
}

future<> redis_service::counter_by(args_collection& args, bool incr, bool with_step, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty() || (with_step == true && args._command_args_count <= 1)) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    uint64_t step = 1;
    if (with_step) {
        sstring& s = args._command_args[1];
        try {
            step = std::atol(s.c_str());
        } catch (const std::invalid_argument&) {
            return out.write(msg_syntax_err);
        }
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::counter_by, std::move(rk), step, incr).then([&out] (auto&& m) {
            return out.write(std::move(*m));
    });
}

future<> redis_service::hdel(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    if (args._command_args_count == 2) {
        return _db.invoke_on(cpu, &database::hdel, std::move(rk), std::ref(field)).then([&out] (auto&& m) {
            return out.write(std::move(*m));
        });
    }
    else {
        for (size_t i = 1; i < args._command_args.size(); ++i) args._tmp_keys.emplace_back(args._command_args[i]);
        auto& keys = args._tmp_keys;
        return _db.invoke_on(cpu, &database::hdel_multi, std::move(rk), std::ref(keys)).then([&out] (auto&& m) {
            return out.write(std::move(*m));
        });
    }
}

future<> redis_service::hexists(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::hexists, std::move(rk), std::ref(field)).then([&out] (auto&& m) {
        out.write(std::move(*m));
    });
}

future<> redis_service::hset(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    sstring& val = args._command_args[2];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::hset, std::move(rk), std::ref(field), std::ref(val)).then([&out] (auto&& m) {
        out.write(std::move(*m));
    });
}

future<> redis_service::hmset(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    unsigned int field_count = (args._command_args_count - 1) / 2;
    sstring& key = args._command_args[0];
    for (unsigned int i = 0; i < field_count; ++i) {
        args._tmp_key_values.emplace(std::make_pair(args._command_args[i], args._command_args[i + 1]));
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::hmset, std::move(rk), std::ref(args._tmp_key_values)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::hincrby(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    sstring& val = args._command_args[2];
    int delta = std::atoi(val.c_str());
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::hincrby, std::move(rk), std::ref(field), delta).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::hincrbyfloat(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    sstring& val = args._command_args[2];
    double delta = std::atof(val.c_str());
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::hincrbyfloat, std::move(rk), std::ref(field), delta).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::hlen(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::hlen, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::hstrlen(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::hstrlen, std::move(rk), std::ref(field)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::hget(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& field = args._command_args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::hget, std::move(rk), std::ref(field)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::hgetall(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::hgetall, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::hgetall_keys(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::hgetall_keys, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::hgetall_values(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::hgetall_values, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::hmget(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    for (unsigned int i = 1; i < args._command_args_count; ++i) {
        args._tmp_keys.emplace_back(std::move(args._command_args[i]));
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    auto& keys = args._tmp_keys;
    return _db.invoke_on(cpu, &database::hmget, std::move(rk), std::ref(keys)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::smembers_impl(sstring& key, output_stream<char>& out)
{
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::smembers, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::smembers(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    return smembers_impl(key, out);
}

future<> redis_service::sadds_impl(sstring& key, std::vector<sstring>& members, output_stream<char>& out)
{
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::sadds, std::move(rk), std::ref(members)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::sadds_impl_return_keys(sstring& key, std::vector<sstring>& members, output_stream<char>& out)
{
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::sadds_direct, std::move(rk), std::ref(members)).then([&out, &members] (auto m) {
        if (m)
           return reply_builder::build_local(out, members);
        return reply_builder::build_local(out, msg_err);
    });
}

future<> redis_service::sadd(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    for (uint32_t i = 1; i < args._command_args_count; ++i) args._tmp_keys.emplace_back(std::move(args._command_args[i]));
    auto& keys = args._tmp_keys;
    return sadds_impl(key, std::ref(keys), out);
}

future<> redis_service::scard(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::scard, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}
future<> redis_service::sismember(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& member = args._command_args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::sismember, std::move(rk), std::ref(member)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::srem(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    for (uint32_t i = 1; i < args._command_args_count; ++i) args._tmp_keys.emplace_back(std::move(args._command_args[i]));
    auto& keys = args._tmp_keys;
    return _db.invoke_on(cpu, &database::srems, std::move(rk), std::ref(keys)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::sdiff_store(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& dest = args._command_args[0];
    for (size_t i = 1; i < args._command_args.size(); ++i) {
        args._tmp_keys.emplace_back(std::move(args._command_args[i]));
    }
    return sdiff_impl(args._tmp_keys, &dest, out);
}

future<> redis_service::sdiff(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    if (args._command_args_count == 1) {
        return smembers(args, out);
    }
    return sdiff_impl(std::ref(args._command_args), nullptr, out);
}

future<> redis_service::sdiff_impl(std::vector<sstring>& keys, sstring* dest, output_stream<char>& out)
{
    using item_unordered_map = std::unordered_map<unsigned, std::vector<sstring>>;
    struct sdiff_state {
        item_unordered_map items_set;
        std::vector<sstring>& keys;
        sstring* dest = nullptr;
        std::vector<sstring> result;
    };
    uint32_t count = static_cast<uint32_t>(keys.size());
    return do_with(sdiff_state{item_unordered_map{}, std::ref(keys), dest, {}}, [&out, this, count] (auto& state) {
        return parallel_for_each(boost::irange<unsigned>(0, count), [this, &state] (unsigned k) {
            sstring& key = state.keys[k];
            redis_key rk { std::ref(key) };
            auto cpu = this->get_cpu(rk);
            return _db.invoke_on(cpu, &database::smembers_direct, std::move(rk)).then([&state, index = k] (auto&& members) {
                state.items_set[index] = std::move(*members);
            });
        }).then([this, &out, &state, count] {
            auto& temp = state.items_set[0];
            for (uint32_t i = 1; i < count; ++i) {
                auto&& next_items = std::move(state.items_set[i]);
                for (auto& item : next_items) {
                    stdx::erase_if(temp, [&item] (auto& o) { return item == o; });
                }
            }
            std::vector<sstring>& result = temp;
            if (state.dest) {
                return this->sadds_impl_return_keys(*state.dest, result, out);
            }
            return reply_builder::build_local(out, result);
        });
    });
}

future<> redis_service::sinter_impl(std::vector<sstring>& keys, sstring* dest, output_stream<char>& out)
{
    using item_unordered_map = std::unordered_map<unsigned, std::vector<sstring>>;
    struct sinter_state {
        item_unordered_map items_set;
        std::vector<sstring>& keys;
        sstring* dest = nullptr;
    };
    uint32_t count = static_cast<uint32_t>(keys.size());
    return do_with(sinter_state{item_unordered_map{}, std::ref(keys), dest}, [this, &out, count] (auto& state) {
        return parallel_for_each(boost::irange<unsigned>(0, count), [this, &state] (unsigned k) {
            sstring& key = state.keys[k];
            redis_key rk { std::ref(key) };
            auto cpu = this->get_cpu(rk);
            return _db.invoke_on(cpu, &database::smembers_direct, std::move(rk)).then([&state, index = k] (auto&& members) {
                state.items_set[index] = std::move(*members);
            });
        }).then([this, &out, &state, count] {
            auto& result = state.items_set[0];
            for (uint32_t i = 1; i < count; ++i) {
                auto& next_items = state.items_set[i];
                if (result.empty() || next_items.empty()) {
                    return reply_builder::build_local(out, result);
                }
                std::vector<sstring> temp;
                for (auto& item : next_items) {
                    if (std::find_if(result.begin(), result.end(), [&item] (auto& o) { return item == o; }) != result.end()) {
                        temp.emplace_back(std::move(item));
                    }
                }
                result = std::move(temp);
            }
            if (state.dest) {
                return this->sadds_impl_return_keys(*state.dest, result, out);
            }
            return reply_builder::build_local(out, result);
       });
   });
}

future<> redis_service::sinter(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    if (args._command_args_count == 1) {
        return smembers(args, out);
    }
    return sinter_impl(args._command_args, nullptr, out);
}

future<> redis_service::sinter_store(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& dest = args._command_args[0];
    for (size_t i = 1; i < args._command_args.size(); ++i) {
        args._tmp_keys.emplace_back(std::move(args._command_args[i]));
    }
    return sinter_impl(args._tmp_keys, &dest, out);
}

future<> redis_service::sunion_impl(std::vector<sstring>& keys, sstring* dest, output_stream<char>& out)
{
    struct union_state {
        std::vector<sstring> result;
        std::vector<sstring>& keys;
        sstring* dest = nullptr;
    };
    uint32_t count = static_cast<uint32_t>(keys.size());
    return do_with(union_state{{}, std::ref(keys), dest}, [this, &out, count] (auto& state) {
        return parallel_for_each(boost::irange<unsigned>(0, count), [this, &out, &state] (unsigned k) {
            sstring& key = state.keys[k];
            redis_key rk { std::ref(key) };
            auto cpu = this->get_cpu(rk);
            return _db.invoke_on(cpu, &database::smembers_direct, std::move(rk)).then([&state] (auto&& members) {
                auto& result = state.result;
                for (auto& item : *members) {
                    if (std::find_if(result.begin(), result.end(), [&item] (auto& o) { return o == item; }) == result.end()) {
                        result.emplace_back(std::move(item));
                    }
                }
            });
        }).then([this, &state, &out] {
            auto& result = state.result;
            if (state.dest) {
            std::cout << "result size: " << result.size();
                return this->sadds_impl_return_keys(*state.dest, result, out);
            }
            return reply_builder::build_local(out, result);
        });
    });
}

future<> redis_service::sunion(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    if (args._command_args_count == 1) {
        return smembers(args, out);
    }
    return sunion_impl(args._command_args, nullptr, out);
}

future<> redis_service::sunion_store(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& dest = args._command_args[0];
    for (size_t i = 1; i < args._command_args.size(); ++i) {
        args._tmp_keys.emplace_back(std::move(args._command_args[i]));
    }
    return sunion_impl(args._tmp_keys, &dest, out);
}

future<bool> redis_service::srem_direct(sstring& key, sstring& member)
{
    redis_key rk {std::ref(key) };
    auto cpu = get_cpu(rk);
    return   _db.invoke_on(cpu, &database::srem_direct, rk, std::ref(member));
}

future<bool> redis_service::sadd_direct(sstring& key, sstring& member)
{
    redis_key rk {std::ref(key) };
    auto cpu = get_cpu(rk);
    return  _db.invoke_on(cpu, &database::sadd_direct, rk, std::ref(member));
}

future<> redis_service::smove(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& dest = args._command_args[1];
    sstring& member = args._command_args[2];
    struct smove_state {
        sstring& src;
        sstring& dst;
        sstring& member;
    };
    return do_with(smove_state{std::ref(key), std::ref(dest), std::ref(member)}, [this, &out] (auto& state) {
        return this->srem_direct(state.src, state.member).then([this, &state, &out] (auto u) {
            if (u) {
                return this->sadd_direct(state.dst, state.member).then([&out] (auto m) {
                    return out.write(m ? msg_one : msg_zero);
                });
            }
            return out.write(msg_zero);
        });
    });
}

future<> redis_service::srandmember(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    size_t count = 1;
    if (args._command_args_count > 1) {
        try {
            count  = std::stol(args._command_args[1].c_str());
        } catch (const std::invalid_argument&) {
            return out.write(msg_syntax_err);
        }
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::srandmember, rk, count).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::spop(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    size_t count = 1;
    if (args._command_args_count > 1) {
        try {
            count  = std::stol(args._command_args[1].c_str());
        } catch (const std::invalid_argument&) {
            return out.write(msg_syntax_err);
        }
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::spop, rk, count).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}
future<> redis_service::type(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::type, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::expire(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    long expir = std::atol(args._command_args[1].c_str());
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::expire, std::move(rk), expir).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::pexpire(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    long expir = std::atol(args._command_args[1].c_str());
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::expire, std::move(rk), expir).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::pttl(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::pttl, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::ttl(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::ttl, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::persist(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::persist, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::zadd(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    std::string un = args._command_args[1];
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
        if (args._command_args_count - first_score_index > 2) {
            return out.write(msg_syntax_err);
        }
        redis_key rk{std::ref(key)};
        auto cpu = get_cpu(rk);
        sstring& member = args._command_args[first_score_index + 1];
        sstring& delta = args._command_args[first_score_index];
        double score = 0;
        try {
            score = std::stod(delta.c_str());
        } catch (const std::invalid_argument&) {
            return out.write(msg_syntax_err);
        }
        return _db.invoke_on(cpu, &database::zincrby, std::move(rk), std::ref(member), score).then([&out] (auto&& m) {
            return out.write(std::move(*m));
        });
    }
    else {
        if ((args._command_args_count - first_score_index) % 2 != 0 || ((zadd_flags & ZADD_NX) && (zadd_flags & ZADD_XX))) {
            return out.write(msg_syntax_err);
        }
    }
    for (size_t i = first_score_index; i < args._command_args_count; i += 2) {
        sstring& score_ = args._command_args[i];
        sstring& member = args._command_args[i + 1];
        double score = 0;
        try {
            score = std::stod(score_.c_str());
        } catch (const std::invalid_argument&) {
            return out.write(msg_syntax_err);
        }
        args._tmp_key_scores.emplace(std::pair<sstring, double>(member, score));
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::zadds, std::move(rk), std::ref(args._tmp_key_scores), zadd_flags).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::zcard(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::zcard, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<std::vector<std::pair<sstring, double>>> redis_service::range_impl(sstring& key, long begin, long end, bool reverse)
{
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::zrange_direct, std::move(rk), size_t(begin), size_t(end), reverse);
}

future<> redis_service::zrange(args_collection& args, bool reverse, output_stream<char>& out)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    long begin = 0, end = 0;
    try {
        begin = std::stoi(args._command_args[1].c_str());
        end = std::stoi(args._command_args[2].c_str());
    } catch (const std::invalid_argument&) {
        return out.write(msg_syntax_err);
    }
    bool with_score = false;
    if (args._command_args_count == 4) {
        auto ws = args._command_args[3];
        std::transform(ws.begin(), ws.end(), ws.begin(), ::toupper);
        if (ws == "WITHSCORES") {
            with_score = true;
        }
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::zrange, std::move(rk), begin, end, reverse, with_score).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::zrangebyscore(args_collection& args, bool reverse, output_stream<char>& out)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    double min = 0, max = 0;
    try {
        min = std::stod(args._command_args[1].c_str());
        max = std::stod(args._command_args[2].c_str());
    } catch (const std::invalid_argument&) {
        return out.write(msg_syntax_err);
    }
    bool with_score = false;
    if (args._command_args_count == 4) {
        auto ws = args._command_args[3];
        std::transform(ws.begin(), ws.end(), ws.begin(), ::toupper);
        if (ws == "WITHSCORES") {
            with_score = true;
        }
    }
    return _db.invoke_on(cpu, &database::zrangebyscore, std::move(rk), min, max, reverse, with_score).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::zcount(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    double min = 0, max = 0;
    try {
        min = std::stod(args._command_args[1].c_str());
        max = std::stod(args._command_args[2].c_str());
    } catch (const std::invalid_argument&) {
        return out.write(msg_syntax_err);
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::zcount, std::move(rk), min, max).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::zincrby(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& member = args._command_args[2];
    double delta = 0;
    try {
        delta = std::stod(args._command_args[1].c_str());
    } catch (const std::invalid_argument&) {
        return out.write(msg_syntax_err);
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::zincrby, std::move(rk), std::ref(member), delta).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::zrank(args_collection& args, bool reverse, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& member = args._command_args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::zrank, std::move(rk), std::ref(member), reverse).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::zrem(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    for (size_t i = 1; i < args._command_args_count; ++i) {
        sstring& member = args._command_args[i];
        args._tmp_keys.emplace_back(std::move(member));
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::zrem, std::move(rk), std::ref(args._tmp_keys)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::zscore(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    sstring& member = args._command_args[1];
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::zscore, std::move(rk), std::ref(member)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

bool redis_service::parse_zset_args(args_collection& args, zset_args& uargs)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return false;
    }
    uargs.dest = std::move(args._command_args[0]);
    try {
        uargs.numkeys = std::stol(args._command_args[1].c_str());
    } catch(const std::invalid_argument&) {
        return false;
    }
    size_t index = static_cast<size_t>(uargs.numkeys) + 2;
    if (args._command_args_count < index) {
        return false;
    }
    for (size_t i = 2; i < index; ++i) {
        uargs.keys.emplace_back(std::move(args._command_args[i]));
    }
    bool has_weights = false, has_aggregate = false;
    if (index < args._command_args_count) {
        sstring& weight_syntax = args._command_args[index];
        if (weight_syntax == "WEIGHTS") {
            index ++;
            if (index + uargs.numkeys < args._command_args_count) {
                return false;
            }
            has_weights = true;
            size_t i = index;
            index += uargs.numkeys;
            for (; i < index; ++i) {
                try {
                    uargs.weights.push_back(std::stod(args._command_args[i].c_str()));
                } catch (const std::invalid_argument&) {
                    return false;
                }
            }
        }
        if (index < args._command_args_count) {
            if (index + 2 < args._command_args_count) {
                sstring& aggregate = args._command_args[index];
                if (aggregate == "ARGGREGATE") {
                    has_aggregate = true;
                }
                index++;
                sstring& aggre = args._command_args[index];
                if (aggre == "SUM") {
                    uargs.aggregate_flag |= ZAGGREGATE_SUM;
                }
                else if (aggre == "MIN") {
                    uargs.aggregate_flag |= ZAGGREGATE_MIN;
                }
                else if (aggre == "MAX") {
                    uargs.aggregate_flag |= ZAGGREGATE_MAX;
                }
                else {
                    return false;
                }
                has_aggregate = true;
            }
            else {
                return false;
            }
        }
    }
    if (has_weights == false) {
        for (size_t i = 0; i < uargs.numkeys; ++i) {
            uargs.weights.push_back(1);
        }
    }
    if (has_aggregate == false) {
        uargs.aggregate_flag = ZAGGREGATE_SUM;
    }
    return true;
}

future<> redis_service::zunionstore(args_collection& args, output_stream<char>& out)
{
        return out.write(msg_syntax_err);
/*
    zset_args uargs;
    if (parse_zset_args(args, uargs) == false) {
        return out.write(msg_syntax_err);
    }
    struct zunion_store_state {
        std::vector<std::pair<sstring, double>> wkeys;
        sstring dest;
        std::vector<std::unordered_map<sstring, double>> result;
        int aggregate_flag;
    };
    std::vector<std::pair<sstring, double>> wkeys;
    for (size_t i = 0; i < uargs.numkeys; ++i) {
        wkeys.emplace_back(std::pair<sstring, double>(std::move(uargs.keys[i]), uargs.weights[i]));
    }
    return do_with(zunion_store_state{std::move(wkeys), std::move(uargs.dest), {}, uargs.aggregate_flag}, [this] (auto& state) {
        return parallel_for_each(std::begin(state.wkeys), std::end(state.wkeys), [this, &state] (auto& entry) {
            sstring& key = entry.first;
            double weight = entry.second;
            return this->range_impl(key, 0, -1, false).then([this, &state, &key, weight] (auto&& u) {
                    std::unordered_map<sstring, double> result;
                    for (size_t i = 0; i < u.size(); ++i) {
                        auto& entry  = u[i]->key();
                        result.emplace(entry.first, entry.second);
                    }
                    state.result.emplace_back(std::move(result));
                }
            });
        }).then([this, &state] () {
            std::unordered_map<sstring, double> result;
            for (size_t i = 0; i < state.result.size(); ++i) {
               auto& member_scores = state.result[i];
               for (auto& e : member_scores) {
                   auto& member = e.first;
                   auto  score = e.second;
                   auto exists = result.find(member);
                   if (exists != result.end()) {
                       result[member] = score;
                   }
                   else {
                       if (state.aggregate_flag == ZAGGREGATE_MIN) {
                           result[member] = std::min(result[member], score);
                       }
                       else if(state.aggregate_flag == ZAGGREGATE_SUM) {
                           result[member] += score;
                       }
                       else {
                           result[member] = std::max(result[member], score);
                       }
                   }
               }
            }
            return this->zadds_impl(state.dest, std::move(result), 0).then([] (auto&& u) {
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
        });
    });
*/
}

future<> redis_service::zinterstore(args_collection& args, output_stream<char>& out)
{
        return out.write(msg_syntax_err);
/*
    zset_args uargs;
    if (parse_zset_args(args, uargs) == false) {
        return syntax_err_message();
    }
    struct zinter_store_state {
        std::vector<sstring> keys;
        std::vector<double> weights;
        sstring dest;
        std::unordered_map<size_t, std::unordered_map<sstring, double>> result;
        int aggregate_flag;
    };
    size_t count = uargs.keys.size();
    return do_with(zinter_store_state{std::move(uargs.keys), std::move(uargs.weights), std::move(uargs.dest), {}, uargs.aggregate_flag}, [this, count] (auto& state) {
        return parallel_for_each(boost::irange<size_t>(0, count), [this, &state] (size_t k) {
            sstring& key = state.keys[k];
            double weight = state.weights[k];
            return this->range_impl(key, static_cast<size_t>(0), static_cast<size_t>(-1), false).then([this, &state, &key, weight, index = k] (auto&& u) {
                if (u.second == REDIS_OK) {
                    auto& items = u.first;
                    std::unordered_map<sstring, double> result;
                    for (size_t i = 0; i < items.size(); ++i) {
                        const auto& member = items[i]->key();
                        result.emplace(sstring(member.data(), member.size()), items[i]->Double() * weight);
                    }
                    state.result[index] = std::move(result);
                }
            });
        }).then([this, &state, count] {
            auto&& result = std::move(state.result[0]);
            for (uint32_t i = 1; i < count; ++i) {
                std::unordered_map<sstring, double> temp;
                auto&& next_items = std::move(state.result[i]);
                if (result.empty() || next_items.empty()) {
                    result.clear();
                    break;
                }
                for (auto& e : next_items) {
                    auto exists_member = std::find_if(result.begin(), result.end(), [&e] (auto& o) { return e.first == o.first; });
                    if (exists_member != result.end()) {
                       if (state.aggregate_flag == ZAGGREGATE_MIN) {
                           temp[exists_member->first] = std::min(exists_member->second, e.second);
                       }
                       else if(state.aggregate_flag == ZAGGREGATE_SUM) {
                           temp[exists_member->first] += e.second;
                       }
                       else {
                           temp[exists_member->first] = std::max(exists_member->second, e.second);
                       }
                    }
                }
                result = std::move(temp);
            }
            return this->zadds_impl(state.dest, std::move(result), 0).then([] (auto&& u) {
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
        });
    });
*/
}

future<> redis_service::zremrangebyscore(args_collection& args, output_stream<char>& out)
{
    // ZREMRANGEBYSCORE key min max
    // Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
    // Integer reply: the number of elements removed.
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    double min = 0, max = 0;
    try {
        min = std::stod(args._command_args[1].c_str());
        max = std::stod(args._command_args[2].c_str());
    } catch (const std::invalid_argument&) {
        return out.write(msg_syntax_err);
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::zremrangebyscore, std::move(rk), min, max).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}

future<> redis_service::zremrangebyrank(args_collection& args, output_stream<char>& out)
{
    // ZREMRANGEBYRANK key begin end
    // Removes all elements in the sorted set stored at key with a rank between start and end (inclusive).
    // Integer reply: the number of elements removed.
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    long begin = 0, end = 0;
    try {
        begin = std::stol(args._command_args[1].c_str());
        end = std::stol(args._command_args[2].c_str());
    } catch(const std::invalid_argument& e) {
        return out.write(msg_syntax_err);
    }
    redis_key rk{std::ref(key)};
    auto cpu = get_cpu(rk);
    return _db.invoke_on(cpu, &database::zremrangebyrank, std::move(rk), begin, end).then([&out] (auto&& m) {
       return out.write(std::move(*m));
    });
}

future<> redis_service::zdiffstore(args_collection&, output_stream<char>& out)
{
    return out.write(msg_syntax_err);
}

future<> redis_service::zunion(args_collection& args, output_stream<char>& out)
{
    return out.write(msg_syntax_err);
}

future<> redis_service::zinter(args_collection& args, output_stream<char>& out)
{
    return out.write(msg_syntax_err);
}

future<> redis_service::zdiff(args_collection& args, output_stream<char>& out)
{
    return out.write(msg_syntax_err);
}

future<> redis_service::zrangebylex(args_collection& args, output_stream<char>& out)
{
    return out.write(msg_syntax_err);
}

future<> redis_service::zlexcount(args_collection& args, output_stream<char>& out)
{
    return out.write(msg_syntax_err);
}

future<> redis_service::select(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return out.write(msg_syntax_err);
    }
    size_t index = 0;
    try {
        index = std::stol(args._command_args[0].c_str());
    } catch (const std::invalid_argument&) {
        return out.write(msg_err);
    }
    if (index >= smp::count) {
        return out.write(msg_err);
    }
    return do_with(size_t {0}, [this, index, &out] (auto& count) {
        return parallel_for_each(boost::irange<unsigned>(0, smp::count), [this, index, &count] (unsigned cpu) {
            return _db.invoke_on(cpu, &database::select, index).then([&count] (auto&& u) {
                if (u) {
                    count++;
                }
            });
        }).then([&count, &out] () {
            return out.write((count == smp::count) ? msg_ok : msg_err);
        });
    });
}

/*
future<message> redis_service::geoadd(args_collection& args)
{
    if (args._command_args_count < 4 || (args._command_args_count - 1) % 3 != 0 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    std::unordered_map<sstring, double> members;
    for (size_t i = 1; i < args._command_args_count; i += 3) {
        sstring& longitude = args._command_args[i];
        sstring& latitude = args._command_args[i + 1];
        sstring& member = args._command_args[i + 2];
        double longitude_ = 0, latitude_ = 0, score = 0;
        try {
            longitude_ = std::stod(longitude.c_str());
            latitude_ = std::stod(latitude.c_str());
        } catch (const std::invalid_argument&) {
            return syntax_err_message();
        }
        if (geo::encode_to_geohash(longitude_, latitude_, score) == false) {
            return err_message();
        }
        members.emplace(std::pair<sstring, double>(member, score));
    }
    return zadds_impl(key, std::move(members), 0).then([] (auto&& u) {
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

future<message> redis_service::geodist(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& lpos = args._command_args[1];
    sstring& rpos = args._command_args[2];
    int geodist_flag = GEODIST_UNIT_M;
    if (args._command_args_count == 4) {
        sstring& unit = args._command_args[3];
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
            return syntax_err_message();
        }
    }
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().geodist(std::move(rk), std::move(lpos), std::move(rpos), geodist_flag);
        if (u.second == REDIS_OK) {
            return double_message<true>(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return nil_message();
        }
    }
    return _db.invoke_on(cpu, &database::geodist, std::move(rk), std::move(lpos), std::move(rpos), geodist_flag).then([] (auto&& u) {
        if (u.second == REDIS_OK) {
            return double_message<true>(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return nil_message();
        }
    });
}

future<message> redis_service::geohash(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    std::vector<sstring> members;
    for (size_t i = 1; i < args._command_args_count; ++i) {
        members.emplace_back(std::move(args._command_args[i]));
    }
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().geohash(std::move(rk), std::move(members));
        if (u.second == REDIS_OK) {
            return strings_message(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    }
    return _db.invoke_on(cpu, &database::geohash, std::move(rk), std::move(members)).then([] (auto&& u) {
        if (u.second == REDIS_OK) {
            return strings_message(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    });
}

future<message> redis_service::geopos(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    std::vector<sstring> members;
    for (size_t i = 1; i < args._command_args_count; ++i) {
        members.emplace_back(std::move(args._command_args[i]));
    }
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().geopos(std::move(rk), std::move(members));
        if (u.second == REDIS_OK) {
            return double_array_message(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    }
    return _db.invoke_on(cpu, &database::geopos, std::move(rk), std::move(members)).then([] (auto&& u) {
        if (u.second == REDIS_OK) {
            return double_array_message(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    });
}

future<message> redis_service::georadius(args_collection& args, bool member)
{
    size_t option_index = member ? 4 : 5;
    if (args._command_args_count < option_index || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring unit{}, member_key{};
    double log = 0, lat = 0, radius = 0;
    if (!member) {
        sstring& longitude = args._command_args[1];
        sstring& latitude = args._command_args[2];
        sstring& rad = args._command_args[3];
        unit = std::move(args._command_args[4]);
        try {
            log = std::stod(longitude.c_str());
            lat = std::stod(latitude.c_str());
            radius = std::stod(rad.c_str());
        } catch (const std::invalid_argument&) {
            return syntax_err_message();
        }
    }
    else {
        member_key = std::move(args._command_args[1]);
        sstring& rad = args._command_args[2];
        unit = std::move(args._command_args[3]);
        try {
            radius = std::stod(rad.c_str());
        } catch (const std::invalid_argument&) {
            return syntax_err_message();
        }
    }

    int flags = 0;
    size_t count = 0, stored_key_index = 0;
    if (args._command_args_count > option_index) {
        for (size_t i = option_index; i < args._command_args_count; ++i) {
            sstring& cc = args._command_args[i];
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
                if (i + 1 == args._command_args_count) {
                    return syntax_err_message();
                }
                sstring& c = args._command_args[++i];
                try {
                    count = std::stol(c.c_str());
                } catch (const std::invalid_argument&) {
                    return syntax_err_message();
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
                if (i + 1 == args._command_args_count) {
                    return syntax_err_message();
                }
                ++i;
                stored_key_index = i;
            }
            else if (cc == "STOREDIST") {
                flags |= GEORADIUS_STORE_DIST;
                if (i + 1 == args._command_args_count) {
                    return syntax_err_message();
                }
                ++i;
                stored_key_index = i;
            }
            else {
                return syntax_err_message();
            }
        }
    }
    if (((flags & GEORADIUS_STORE_SCORE) || (flags & GEORADIUS_STORE_DIST)) && (stored_key_index == 0 || stored_key_index >= args._command_args_count)) {
        return syntax_err_message();
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
        return syntax_err_message();
    }
    geo::to_meters(radius, flags);

    auto points_ready =  !member ? fetch_points_by_coord_radius(key, log, lat, radius, count, flags) : fetch_points_by_coord_radius(key, member_key, radius, count, flags);
    return  points_ready.then([this, flags, &args, stored_key_index] (georadius_result_type&& u) {
        using data_type = std::vector<std::tuple<sstring, double, double, double, double>>;
        if (u.second == REDIS_OK) {
            bool store_with_score = flags & GEORADIUS_STORE_SCORE, store_with_dist = flags & GEORADIUS_STORE_DIST;
            if (store_with_score || store_with_dist) {
                std::unordered_map<sstring, double> members;
                data_type& data = u.first;
                for (size_t i = 0; i < data.size(); ++i) {
                   auto& data_tuple = data[i];
                   auto& key = std::get<0>(data_tuple);
                   auto  score = store_with_score ? std::get<1>(data_tuple) : std::get<2>(data_tuple);
                   members.emplace(std::pair<sstring, double>(std::ref(key), score));
                }
                struct store_state
                {
                    std::unordered_map<sstring, double> members;
                    sstring stored_key;
                    data_type data;
                };
                sstring& stored_key = args._command_args[stored_key_index];
                return do_with(store_state{std::move(members), std::move(stored_key), std::move(u.first)}, [this, flags] (auto& state) {
                    return this->zadds_impl(state.stored_key, std::move(state.members), 0).then([this, &state, flags] (auto&& u) {
                       if (u.second == REDIS_OK) {
                           return geo_radius_message(state.data, flags);
                       }
                       else {
                          return err_message();
                       }
                    });
                });
            }
            return geo_radius_message(u.first, flags);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return nil_message();
        }
    });
}

using georadius_result_type = std::pair<std::vector<std::tuple<sstring, double, double, double, double>>, int>;
future<georadius_result_type> redis_service::fetch_points_by_coord_radius(sstring& key, double log, double lat, double radius, size_t count, int flags)
{
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().georadius_coord(std::move(rk), log, lat, radius, count, flags);
        return make_ready_future<georadius_result_type>(std::move(u));
    }
    return _db.invoke_on(cpu, &database::georadius_coord, std::move(rk), log, lat, radius, count, flags);
}

future<georadius_result_type> redis_service::fetch_points_by_coord_radius(sstring& key, sstring& member_key, double radius, size_t count, int flags)
{
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().georadius_member(std::move(rk), std::move(member_key), radius, count, flags);
        return make_ready_future<georadius_result_type>(std::move(u));
    }
    return _db.invoke_on(cpu, &database::georadius_member, std::move(rk), std::move(member_key), radius, count, flags);
}

future<message> redis_service::setbit(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    size_t offset = 0;
    int value = 0;
    try {
        offset = std::stol(args._command_args[1]);
        value = std::stoi(args._command_args[2]);
    } catch (const std::invalid_argument&) {
        return syntax_err_message();
    }
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().setbit(std::move(rk), offset, value == 1);
        if (u.second == REDIS_OK) {
            return u.first ? one_message() : zero_message();
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    }
    return _db.invoke_on(cpu, &database::setbit, std::move(rk), offset, value == 1).then([] (auto&& u) {
        if (u.second == REDIS_OK) {
            return u.first ? one_message() : zero_message();
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    });
}

future<message> redis_service::getbit(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    size_t offset = 0;
    try {
        offset = std::stol(args._command_args[1]);
    } catch (const std::invalid_argument&) {
        return syntax_err_message();
    }
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().getbit(std::move(rk), offset);
        if (u.second == REDIS_OK) {
            return u.first ? one_message() : zero_message();
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    }
    return _db.invoke_on(cpu, &database::getbit, std::move(rk), offset).then([] (auto&& u) {
        if (u.second == REDIS_OK) {
            return u.first ? one_message() : zero_message();
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    });
}
future<message> redis_service::bitcount(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    long start = 0, end = 0;
    try {
        start = std::stol(args._command_args[1]);
        end = std::stol(args._command_args[2]);
    } catch (const std::invalid_argument&) {
        return syntax_err_message();
    }
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().bitcount(std::move(rk), start, end);
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
    return _db.invoke_on(cpu, &database::bitcount, std::move(rk), start, end).then([] (auto&& u) {
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
future<message> redis_service::bitop(args_collection& args)
{
    return err_message();
}
future<message> redis_service::bitpos(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& b = args._command_args[1];
    long start = 0, end = 0;
    bool bit = false;
    if (args._command_args_count == 4) {
        try {
            auto bb = std::stoi(b);
            if (bb != 0 && bb != 1) {
                return syntax_err_message();
            }
            bit = bb == 1;
            start = std::stol(args._command_args[2]);
            end = std::stol(args._command_args[3]);
        } catch (const std::invalid_argument&) {
            return syntax_err_message();
        }
    }
    redis_key rk {std::ref(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().bitpos(std::move(rk), bit, start, end);
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
    return _db.invoke_on(cpu, &database::bitpos, std::move(rk), bit, start, end).then([] (auto&& u) {
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

future<message> redis_service::bitfield(args_collection& args)
{
    return err_message();
}
*/
} /* namespace redis */
