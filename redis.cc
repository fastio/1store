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
#include "db.hh"
#include "reply_builder.hh"
#include  <experimental/vector>
#include "core/metrics.hh"
#include "request_wrapper.hh"
using namespace net;
namespace redis {


using logger =  seastar::logger;
static logger redis_log ("redis");

namespace stdx = std::experimental;

distributed<redis_service> _the_redis;

future<> redis_service::start()
{
    return make_ready_future<>();
}

future<> redis_service::stop()
{
    return make_ready_future<>();
}

future<> redis_service::set(request_wrapper& args, output_stream<char>& out)
{
    // parse args
    if (args._args_count < 2) {
        return out.write(msg_syntax_err);
    }
    bytes& key = args._args[0];
    bytes& val = args._args[1];
    long expir = 0;
    uint8_t flag = FLAG_SET_NO;
    // [EX seconds] [PS milliseconds] [NX] [XX]
    if (args._args_count > 2) {
        for (unsigned int i = 2; i < args._args_count; ++i) {
            bytes* v = (i == args._args_count - 1) ? nullptr : &(args._args[i + 1]);
            bytes& o = args._args[i];
            if (o.size() != 2) {
                return out.write(msg_syntax_err);
            }
            if ((o[0] == 'e' || o[0] == 'E') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_EX;
                if (v == nullptr) {
                    return out.write(msg_syntax_err);
                }
                try {
                    expir = std::atol(v->c_str()) * 1000;
                } catch (const std::invalid_argument&) {
                    return out.write(msg_syntax_err);
                }
                i++;
            }
            if ((o[0] == 'p' || o[0] == 'P') && (o[1] == 'x' || o[1] == 'X') && o[2] == '\0') {
                flag |= FLAG_SET_PX;
                if (v == nullptr) {
                    return out.write(msg_syntax_err);
                }
                try {
                    expir = std::atol(v->c_str());
                } catch (const std::invalid_argument&) {
                    return out.write(msg_syntax_err);
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
    redis_key rk { key };
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::set, std::move(rk), std::ref(val), expir, flag).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });;
}

future<bool> redis_service::remove_impl(bytes& key) {
    return make_ready_future<bool>();
}

future<> redis_service::del(request_wrapper& args, output_stream<char>& out)
{
    /*
    if (args._args_count <= 0 || args._args.empty()) {
        return out.write(msg_syntax_err);
    }
    if (args._args.size() == 1) {
        bytes& key = args._args[0];
        return remove_impl(key).then([&out] (auto r) {
            return out.write( r ? msg_one : msg_zero);
        });
    }
    else {
        struct mdel_state {
            std::vector<bytes>& keys;
            size_t success_count;
        };
        for (size_t i = 0; i < args._args_count; ++i) {
            args._tmp_keys.emplace_back(args._args[i]);
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
    */
    return make_ready_future<>();
}

future<> redis_service::get(request_wrapper& args, output_stream<char>& out)
{
    if (args._args_count < 1) {
        return out.write(msg_syntax_err);
    }
    bytes& key = args._args[0];
    redis_key rk { key };
    auto cpu = get_cpu(rk);
    return get_database().invoke_on(cpu, &database::get, std::move(rk)).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });
}
}
