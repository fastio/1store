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
#include <unistd.h>
#include <cstdlib>
#include "redis_protocol.hh"
#include "system_stats.hh"
#include "db.hh"
#include "reply_builder.hh"
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
    redis_key rk { std::move(key) };
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db.local().set(std::move(rk), std::move(val), expir, flag));
    }
    return _db.invoke_on(cpu, &database::set, std::move(rk), std::move(val), expir, flag);
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
    redis_key rk { std::move(key) };
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto u = _db.local().set(std::move(rk), std::move(val), expir, flag);
        return u == REDIS_OK ? out.write(msg_ok) : out.write(msg_err);
    }
    return _db.invoke_on(cpu, &database::set, std::move(rk), std::move(val), expir, flag).then([&out] (auto&& u) {
        return u == REDIS_OK ? out.write(msg_ok) : out.write(msg_err);
    });;
}

future<bool> redis_service::remove_impl(sstring& key) {
    redis_key rk { std::move(key) };
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<bool>(_db.local().del(std::move(rk)));
    }
    return _db.invoke_on(cpu, &database::del, std::move(rk));
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
            std::vector<sstring> keys;
            size_t success_count;
        };
        std::vector<sstring> keys;
        for (size_t i = 0; i < args._command_args_count; ++i) {
            keys.emplace_back(args._command_args[i]);
        }
        return do_with(mdel_state{std::move(keys), 0}, [this, &out] (auto& state) {
            return parallel_for_each(std::begin(state.keys), std::end(state.keys), [this, &state] (auto& key) {
                return this->remove_impl(key).then([&state] (auto r) {
                    if (r) state.success_count++;
                });
            }).then([&state, &out] {
                return reply_builder::build(out, state.success_count);
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
        std::vector<std::pair<sstring, sstring>> key_value_pairs;
        size_t success_count;
    };
    std::vector<std::pair<sstring, sstring>> key_value_pairs;
    auto pair_size = args._command_args.size() / 2;
    for (size_t i = 0; i < pair_size; ++i) {
        key_value_pairs.emplace_back(std::make_pair(std::move(args._command_args[i * 2]), std::move(args._command_args[i * 2 + 1])));
    }
    return do_with(mset_state{std::move(key_value_pairs), 0}, [this, &out] (auto& state) {
        return parallel_for_each(std::begin(state.key_value_pairs), std::end(state.key_value_pairs), [this, &state] (auto& entry) {
            sstring& key = entry.first;
            sstring& value = entry.second;
            return this->set_impl(key, value, 0, 0).then([&state] (auto) {
                state.success_count++ ;
            });
        }).then([&state, &out] {
            return out.write(state.key_value_pairs.size() == state.success_count ? msg_ok : msg_err);
        });
   });
}

future<item_ptr> redis_service::get_impl(sstring& key)
{
    /*
    redis_key rk {std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<item_ptr>(_db.local().get(std::move(rk)));
    }
    return _db.invoke_on(cpu, &database::get, std::move(rk));
    */
    return make_ready_future<item_ptr>();
}

future<> redis_service::get(args_collection& args, output_stream<char>& out)
{
    if (args._command_args_count < 1) {
        return out.write(msg_syntax_err);
    }
    sstring& key = args._command_args[0];
    redis_key rk {std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return _db.local().get(std::move(rk), std::ref(out));
    }
    return _db.invoke_on(cpu, &database::get, std::move(rk), std::ref(out));
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
    redis_key rk { std::move(key) };
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return _db.local().strlen(std::move(rk), std::ref(out));
    }
    return _db.invoke_on(cpu, &database::strlen, std::move(rk), std::ref(out));
}

future<bool> redis_service::exists_impl(sstring& key)
{
    redis_key rk { std::move(key) };
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<bool>(_db.local().exists(std::move(rk)));
    }
    return _db.invoke_on(cpu, &database::exists, std::move(rk));
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
            std::vector<sstring> keys;
            size_t success_count;
        };
        std::vector<sstring> keys;
        for (size_t i = 0; i < args._command_args_count; ++i) {
            keys.emplace_back(args._command_args[i]);
        }
        return do_with(mexists_state{std::move(keys), 0}, [this, &out] (auto& state) {
            return parallel_for_each(std::begin(state.keys), std::end(state.keys), [this, &state] (auto& key) {
                return this->exists_impl(key).then([&state] (auto r) {
                    if (r) state.success_count++;
                });
            }).then([&state, &out] {
                return reply_builder::build(out, state.success_count);
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
    redis_key rk {std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return _db.local().append(std::move(rk), std::move(val), std::ref(out));
    }
    return _db.invoke_on(cpu, &database::append, std::move(rk), std::move(val), std::ref(out));
}

future<std::pair<size_t, int>> redis_service::push_impl(sstring& key, sstring& val, bool force, bool left)
{
    redis_key rk {std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<std::pair<size_t, int>>(_db.local().push(std::move(rk), std::move(val), force, left));
    }
    return _db.invoke_on(cpu, &database::push<remote_origin_tag>, std::move(rk), std::move(val), force, left);
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
                return this->push_impl(state.key, value, force, left).then([&state] (auto&& u) {
                    if (u.second == REDIS_OK) state.success_count++;
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
    redis_key rk {std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().pop(std::move(rk), left);
        return item_message<false, true>(u);
    }
    return _db.invoke_on(cpu, &database::pop, std::move(rk), left).then([] (auto&& u) {
        return item_message<false, true>(u);
    });
}

future<message> redis_service::lindex(args_collection& args)
{
    if (args._command_args_count <= 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    int idx = std::atoi(args._command_args[1].c_str());
    redis_key rk {std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().lindex(std::move(rk), idx);
        return item_message<false, true>(u);
    }
    return _db.invoke_on(cpu, &database::lindex, std::move(rk), idx).then([] (auto&& u) {
        return item_message<false, true>(u);
    });
}

future<message> redis_service::llen(args_collection& args)
{
    if (args._command_args_count <= 0 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    auto cpu = get_cpu(key);
    redis_key rk {std::move(key)};
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().llen(std::move(rk));
        return size_message(u);
    }
    return _db.invoke_on(cpu, &database::llen, std::move(rk)).then([] (auto&& u) {
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
    redis_key rk {std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().linsert(std::move(rk), std::move(pivot), std::move(value), after);
        return size_message(u);
    }
    return _db.invoke_on(cpu, &database::linsert<remote_origin_tag>, std::move(rk), std::move(pivot), std::move(value), after).then([] (auto&& u) {
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
    redis_key rk {std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().lrange(std::move(rk), start, end);
        return items_message<false, true>(u);
    }
    return _db.invoke_on(cpu, &database::lrange, std::move(rk), start, end).then([] (auto&& u) {
        return items_message<false, true>(u);
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
    redis_key rk {std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().lset(std::move(rk), idx, std::move(value));
        return size_message(u);
    }
    return _db.invoke_on(cpu, &database::lset<remote_origin_tag>, std::move(rk), idx, std::move(value)).then([] (auto&& u) {
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
    redis_key rk {std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().ltrim(std::move(rk), start, stop);
        return size_message(u);
    }
    return _db.invoke_on(cpu, &database::ltrim, std::move(rk), start, stop).then([] (auto&& u) {
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().lrem(std::move(rk), count, std::move(value));
        return size_message(u);
    }
    return _db.invoke_on(cpu, &database::lrem, std::move(rk), count, std::move(value)).then([] (auto&& u) {
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
        try {
            step = std::atol(s.c_str());
        } catch (const std::invalid_argument&) {
            return syntax_err_message();
        }
    }
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().counter_by(std::move(rk), step, incr);
        if (u.second == REDIS_OK) {
            return uint64_message(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    }
    return _db.invoke_on(cpu, &database::counter_by<remote_origin_tag>, std::move(rk), step, incr).then([] (auto&& u) {
        if (u.second == REDIS_OK) {
            return uint64_message(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    });
}

future<int> redis_service::hdel_impl(sstring& key, sstring& field)
{
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db.local().hdel(std::move(rk), std::move(field)));
    }
    return _db.invoke_on(cpu, &database::hdel, std::move(rk), std::move(field));
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
                return this->hdel_impl(state.key, field).then([&state] (auto&& r) {
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().hexists(std::move(rk), std::move(field));
        return size_message(u);
    }
    return _db.invoke_on(cpu, &database::hexists, std::move(rk), std::move(field)).then([] (auto&& u) {
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return size_message(_db.local().hset(std::move(rk), std::move(field), std::move(val)));
    }
    return _db.invoke_on(cpu, &database::hset<remote_origin_tag>, std::move(rk), std::move(field), std::move(val)).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::hmset(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    unsigned int field_count = (args._command_args_count - 1) / 2;
    sstring& key = args._command_args[0];
    std::unordered_map<sstring, sstring> key_values_collection;
    for (unsigned int i = 0; i < field_count; ++i) {
        key_values_collection.emplace(std::make_pair(args._command_args[i], args._command_args[i + 1]));
    }
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto u = _db.local().hmset(std::move(rk), std::move(key_values_collection));
        return u == 0 ? ok_message() : err_message();
    }
    return _db.invoke_on(cpu, &database::hmset<remote_origin_tag>, std::move(rk), std::move(key_values_collection)).then([] (size_t u) {
        return u == 0 ? ok_message() : err_message();
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().hincrby(std::move(rk), std::move(field), delta);
        if (u.second == REDIS_OK) {
            return int64_message(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    }
    return _db.invoke_on(cpu, &database::hincrby<remote_origin_tag>, std::move(rk), std::move(field), delta).then([] (auto&& u) {
        if (u.second == REDIS_OK) {
            return int64_message(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().hincrbyfloat(std::move(rk), std::move(field), delta);
        if (u.second == REDIS_OK) {
            return double_message(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    }
    return _db.invoke_on(cpu, &database::hincrbyfloat<remote_origin_tag>, std::move(rk), std::move(field), delta).then([] (auto&& u) {
        if (u.second == REDIS_OK) {
            return double_message(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return err_message();
        }
    });
}

future<message> redis_service::hlen(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().hlen(std::move(rk));
        return size_message(u);
    }
    return _db.invoke_on(cpu, &database::hlen, std::move(rk)).then([] (auto&& u) {
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().hstrlen(std::move(rk), std::move(field));
        return size_message(u);
    }

    return _db.invoke_on(cpu, &database::hstrlen, std::move(key), std::move(field)).then([] (auto&& u) {
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().hget(std::move(rk), std::move(field));
        return item_message<false, true>(u);
    }
    return _db.invoke_on(cpu, &database::hget, std::move(rk), std::move(field)).then([] (auto&& u) {
        return item_message<false, true>(u);
    });
}

future<message> redis_service::hgetall(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().hgetall(std::move(rk));
        return items_message<true, true>(u);
    }
    return _db.invoke_on(cpu, &database::hgetall, std::move(rk)).then([] (auto&& u) {
        return items_message<true, true>(u);
    });
}

future<message> redis_service::hmget(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    std::vector<sstring> fields;
    for (unsigned int i = 1; i < args._command_args_count; ++i) {
      fields.emplace_back(std::move(args._command_args[i]));
    }
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().hmget(std::move(rk), std::move(fields));
        return items_message<false, true>(u);
    }
    return _db.invoke_on(cpu, &database::hmget, std::move(rk), std::move(fields)).then([] (auto&& u) {
        return items_message<false, true>(u);
    });
}

future<std::pair<std::vector<item_ptr>, int>> redis_service::smembers_impl(sstring& key)
{
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<std::pair<std::vector<item_ptr>, int>>(_db.local().smembers(std::move(rk)));
    }
    return _db.invoke_on(cpu, &database::smembers, std::move(rk));
}
future<message> redis_service::smembers(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    return smembers_impl(key).then([] (auto&& u) {
        if (u.second == REDIS_OK) {
            return items_message<true, false>(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return nil_message();
        }
    });
}

future<std::pair<size_t, int>> redis_service::sadds_impl(sstring& key, std::vector<sstring>&& members)
{
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<std::pair<size_t, int>>(_db.local().sadds(std::move(rk), std::move(members)));
    }
    return _db.invoke_on(cpu, &database::sadds<remote_origin_tag>, std::move(rk), std::move(members));
}

future<message> redis_service::sadd(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    std::vector<sstring> members;
    for (uint32_t i = 1; i < args._command_args_count; ++i) members.emplace_back(std::move(args._command_args[i]));
    return sadds_impl(key, std::move(members)).then([] (auto&& u) {
        return size_message(u);
    });
}

future<message> redis_service::scard(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().scard(std::move(rk));
        return size_message(u);
    }
    return _db.invoke_on(cpu, &database::scard, std::move(rk)).then([] (auto&& u) {
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().sismember(std::move(rk), std::move(member));
        return size_message(u);
    }
    return _db.invoke_on(cpu, &database::sismember, std::move(rk), std::move(member)).then([] (auto&& u) {
        return size_message(u);
    });
}

future<message> redis_service::srem(args_collection& args)
{
    if (args._command_args_count < 2 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    std::vector<sstring> members;
    for (uint32_t i = 1; i < args._command_args_count; ++i) members.emplace_back(std::move(args._command_args[i]));
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().srems(std::move(rk), std::move(members));
        return size_message(u);
    }
    return _db.invoke_on(cpu, &database::srems, std::move(rk), std::move(members)).then([] (auto&& u) {
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
            return this->sadds_impl(state.dest, std::move(members)).then([&state] (auto) {
                return items_message<true, false>(state.result);
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
    return sdiff_impl(std::move(args._command_args)).then([] (auto&& u) {
        return items_message<true, false>(u);
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
            return this->smembers_impl(key).then([&state, index = k] (auto&& u) {
                if (u.second == REDIS_OK) {
                    state.items_set[index] = std::move(u.first);
                }
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
            return this->smembers_impl(key).then([&state, index = k] (auto&& u) {
                if (u.second == REDIS_OK) {
                    state.items_set[index] = std::move(u.first);
                }
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
    return sinter_impl(std::move(args._command_args)).then([] (auto&& u) {
        return items_message<true, false>(u);
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
            return this->sadds_impl(state.dest, std::move(members)).then([&state] (auto) {
                return items_message<true, false>(state.result);
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
            return this->smembers_impl(key).then([&state] (auto&& u) {
                if (u.second == REDIS_OK) {
                    auto& result = state.result;
                    for (auto& i : u.first) {
                        if (std::find_if(result.begin(), result.end(), [&i] (auto& o) { return i->key_size() == o->key_size() && i->key() == o->key(); }) == result.end()) {
                        result.emplace_back(std::move(i));
                        }
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
        return items_message<true, false>(items);
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
            return this->sadds_impl(state.dest, std::move(members)).then([&state] (auto) {
                return items_message<true, false>(state.result);
            });
        });
    });
}

future<int> redis_service::srem_impl(sstring& key, sstring& member)
{
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db.local().srem(std::move(rk), std::move(member)));
    }
    return _db.invoke_on(cpu, &database::srem, std::move(rk), std::move(member));
}

future<int> redis_service::sadd_impl(sstring& key, sstring& member)
{
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<int>(_db.local().sadd(std::move(rk), std::move(member)));
    }
    return _db.invoke_on(cpu, &database::sadd, std::move(rk), std::move(member));
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return type_message(_db.local().type(std::move(rk)));
    }
    return _db.invoke_on(cpu, &database::type, std::move(rk)).then([] (auto u) {
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return size_message(_db.local().expire(std::move(rk), expir * 1000));
    }
    return _db.invoke_on(cpu, &database::expire, std::move(rk), expir).then([] (auto u) {
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return size_message(_db.local().expire(std::move(rk), expir));
    }
    return _db.invoke_on(cpu, &database::expire, std::move(rk), expir).then([] (auto u) {
        return size_message(u);
    });
}

future<message> redis_service::pttl(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return size_message(_db.local().pttl(std::move(rk)));
    }
    return _db.invoke_on(cpu, &database::pttl, std::move(rk)).then([] (auto u) {
        return size_message(u);
    });
}

future<message> redis_service::ttl(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return size_message(_db.local().ttl(std::move(rk)));
    }
    return _db.invoke_on(cpu, &database::ttl, std::move(rk)).then([] (size_t u) {
        return size_message(u);
    });
}

future<message> redis_service::persist(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return size_message(_db.local().persist(std::move(rk)));
    }
    return _db.invoke_on(cpu, &database::persist, std::move(rk)).then([] (auto u) {
        return size_message(u);
    });
}

future<std::pair<size_t, int>> redis_service::zadds_impl(sstring& key, std::unordered_map<sstring, double>&& members, int flags)
{
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<std::pair<size_t, int>>(_db.local().zadds(std::move(rk), std::move(members), flags));
    }
    else {
        return _db.invoke_on(cpu, &database::zadds<remote_origin_tag>, std::move(rk), std::move(members), flags);
    }
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
    if (zadd_flags & ZADD_INCR) {
        if (args._command_args_count - first_score_index > 2) {
            return syntax_err_message();
        }
        redis_key rk{std::move(key)};
        auto cpu = get_cpu(rk);
        sstring& member = args._command_args[first_score_index + 1];
        sstring& delta = args._command_args[first_score_index];
        double score = 0;
        try {
            score = std::stod(delta.c_str());
        } catch (const std::invalid_argument&) {
            return syntax_err_message();
        }
        if (engine().cpu_id() == cpu) {
            auto&& u = _db.local().zincrby(std::move(rk), std::move(member), score);
            return u.second ? double_message<true>(u.first) : err_message();
        }
        else {
            return _db.invoke_on(cpu, &database::zincrby<remote_origin_tag>, std::move(rk), std::move(member), score).then([] (auto&& u) {
                return u.second ? double_message<true>(u.first) : err_message();
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
        double score = 0;
        try {
            score = std::stod(score_.c_str());
        } catch (const std::invalid_argument&) {
            return syntax_err_message();
        }
        members.emplace(std::pair<sstring, double>(member, score));
    }
    return zadds_impl(key, std::move(members), zadd_flags).then([] (auto&& u) {
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

future<message> redis_service::zcard(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().zcard(std::move(rk));
        return size_message(u);
    }
    return _db.invoke_on(cpu, &database::zcard, std::move(rk)).then([] (auto&& u) {
        return size_message(u);
    });
}

future<std::pair<std::vector<item_ptr>, int>> redis_service::range_impl(sstring& key, long begin, long end, bool reverse)
{
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<std::pair<std::vector<item_ptr>, int>>(_db.local().zrange(std::move(rk), begin, end, reverse));
    }
    return _db.invoke_on(cpu, &database::zrange, std::move(rk), begin, end, reverse);
}

future<message> redis_service::zrange(args_collection& args, bool reverse)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    long begin = 0, end = 0;
    try {
        begin = std::stoi(args._command_args[1].c_str());
        end = std::stoi(args._command_args[2].c_str());
    } catch (const std::invalid_argument&) {
        return syntax_err_message();
    }
    bool with_score = false;
    if (args._command_args_count == 4) {
        auto ws = args._command_args[3];
        std::transform(ws.begin(), ws.end(), ws.begin(), ::toupper);
        if (ws == "WITHSCORES") {
            with_score = true;
        }
    }
    return range_impl(key, begin, end, reverse).then([with_score] (auto&& u) {
        if (u.second == REDIS_OK) {
            return with_score ? items_message<true, true>(u.first) : items_message<true, false>(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return nil_message();
        }
    });
}

future<message> redis_service::zrangebyscore(args_collection& args, bool reverse)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    double min = 0, max = 0;
    try {
        min = std::stod(args._command_args[1].c_str());
        max = std::stod(args._command_args[2].c_str());
    } catch (const std::invalid_argument&) {
        return syntax_err_message();
    }
    bool with_score = false;
    if (args._command_args_count == 4) {
        auto ws = args._command_args[3];
        std::transform(ws.begin(), ws.end(), ws.begin(), ::toupper);
        if (ws == "WITHSCORES") {
            with_score = true;
        }
    }
    if (engine().cpu_id() == cpu) {
        auto&& items = _db.local().zrangebyscore(std::move(rk), min, max, reverse);
        return with_score ? items_message<true, true>(items) : items_message<true, false>(items);
    }
    return _db.invoke_on(cpu, &database::zrangebyscore, std::move(rk), min, max, reverse).then([with_score] (auto&& items) {
        return with_score ? items_message<true, true>(items) : items_message<true, false>(items);
    });
}

future<message> redis_service::zcount(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    double min = 0, max = 0;
    try {
        min = std::stod(args._command_args[1].c_str());
        max = std::stod(args._command_args[2].c_str());
    } catch (const std::invalid_argument&) {
        return syntax_err_message();
    }
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().zcount(std::move(rk), min, max);
        return size_message(u);
    }
    return _db.invoke_on(cpu, &database::zcount, std::move(rk), min, max).then([] (auto&& u) {
        return size_message(u);
    });
}

future<message> redis_service::zincrby(args_collection& args)
{
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    sstring& member = args._command_args[2];
    double delta = 0;
    try {
        delta = std::stod(args._command_args[1].c_str());
    } catch (const std::invalid_argument&) {
        return syntax_err_message();
    }
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& result = _db.local().zincrby(std::move(rk), std::move(member), delta);
        if (result.second) {
            return double_message<true>(result.first);
        }
        return wrong_type_err_message();
    }
    return _db.invoke_on(cpu, &database::zincrby, std::move(rk), std::move(member), delta).then([] (auto u) {
        if (u.second) {
            return double_message<true>(u.first);
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().zrank(std::move(rk), std::move(member), reverse);
        if (u.second == REDIS_OK) {
            return size_message(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return nil_message();
        }
    }
    return _db.invoke_on(cpu, &database::zrank, std::move(rk), std::move(member), reverse).then([] (auto&& u) {
        if (u.second == REDIS_OK) {
            return size_message(u.first);
        }
        else if (u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return nil_message();
        }
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
       auto&& u = _db.local().zrem(std::move(rk), std::move(members));
       return size_message(u);
    }
    else {
        return _db.invoke_on(cpu, &database::zrem, std::move(rk), std::move(members)).then([] (auto&& u) {
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
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().zscore(std::move(rk), std::move(member));
        if (u.second == REDIS_OK) {
            return double_message<true>(u.first);
        }
        else if(u.second == REDIS_WRONG_TYPE) {
            return wrong_type_err_message();
        }
        else {
            return nil_message();
        }
    }
    else {
        return _db.invoke_on(cpu, &database::zscore, std::move(rk), std::move(member)).then([] (auto&& u) {
            if (u.second == REDIS_OK) {
                return double_message<true>(u.first);
            }
            else if(u.second == REDIS_WRONG_TYPE) {
                return wrong_type_err_message();
            }
            else {
                return nil_message();
            }
        });
    }
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

future<message> redis_service::zunionstore(args_collection& args)
{
    zset_args uargs;
    if (parse_zset_args(args, uargs) == false) {
        return syntax_err_message();
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
                if (u.second == REDIS_OK) {
                    auto& items = u.first;
                    std::unordered_map<sstring, double> result;
                    for (size_t i = 0; i < items.size(); ++i) {
                        const auto& member = items[i]->key();
                        result.emplace(sstring(member.data(), member.size()), items[i]->Double() * weight);
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
}

future<message> redis_service::zinterstore(args_collection& args)
{
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
}

future<message> redis_service::zremrangebyscore(args_collection& args)
{
    // ZREMRANGEBYSCORE key min max
    // Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
    // Integer reply: the number of elements removed.
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    double min = 0, max = 0;
    try {
        min = std::stod(args._command_args[1].c_str());
        max = std::stod(args._command_args[2].c_str());
    } catch (const std::invalid_argument&) {
        return syntax_err_message();
    }
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().zremrangebyscore(std::move(rk), min, max);
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
        return _db.invoke_on(cpu, &database::zremrangebyscore, std::move(rk), min, max).then([] (auto&& u) {
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

future<message> redis_service::zremrangebyrank(args_collection& args)
{
    // ZREMRANGEBYRANK key begin end
    // Removes all elements in the sorted set stored at key with a rank between start and end (inclusive).
    // Integer reply: the number of elements removed.
    if (args._command_args_count < 3 || args._command_args.empty()) {
        return syntax_err_message();
    }
    sstring& key = args._command_args[0];
    long begin = 0, end = 0;
    try {
        begin = std::stol(args._command_args[1].c_str());
        end = std::stol(args._command_args[2].c_str());
    } catch(const std::invalid_argument& e) {
        return syntax_err_message();
    }
    redis_key rk{std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().zremrangebyrank(std::move(rk), begin, end);
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
        return _db.invoke_on(cpu, &database::zremrangebyrank, std::move(rk), begin, end).then([] (auto&& u) {
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

future<message> redis_service::select(args_collection& args)
{
    if (args._command_args_count < 1 || args._command_args.empty()) {
        return syntax_err_message();
    }
    int index = 0;
    try {
        index = std::stoi(args._command_args[0].c_str());
    } catch (const std::invalid_argument&) {
        return syntax_err_message();
    }
    if (static_cast<size_t>(index) >= smp::count) {
        return err_message();
    }
    return do_with(int {0}, [this, index] (auto& count) {
        return parallel_for_each(boost::irange<unsigned>(0, smp::count), [this, index, &count] (unsigned cpu) {
            return _db.invoke_on(cpu, &database::select, index).then([&count] (auto&& u) {
                if (u == REDIS_OK) {
                    count++;
                }
            });
        }).then([&count] () {
            return (static_cast<size_t>(count) == smp::count) ? ok_message() : err_message();
        });
    });
}

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
    redis_key rk {std::move(key)};
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
    redis_key rk {std::move(key)};
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
    redis_key rk {std::move(key)};
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
                   members.emplace(std::pair<sstring, double>(std::move(key), score));
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
    redis_key rk {std::move(key)};
    auto cpu = get_cpu(rk);
    if (engine().cpu_id() == cpu) {
        auto&& u = _db.local().georadius_coord(std::move(rk), log, lat, radius, count, flags);
        return make_ready_future<georadius_result_type>(std::move(u));
    }
    return _db.invoke_on(cpu, &database::georadius_coord, std::move(rk), log, lat, radius, count, flags);
}

future<georadius_result_type> redis_service::fetch_points_by_coord_radius(sstring& key, sstring& member_key, double radius, size_t count, int flags)
{
    redis_key rk {std::move(key)};
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
    redis_key rk {std::move(key)};
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
    redis_key rk {std::move(key)};
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
    redis_key rk {std::move(key)};
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
    redis_key rk {std::move(key)};
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
} /* namespace redis */
