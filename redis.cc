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
#include "service.hh"
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
#include "log.hh"
#include <unistd.h>
#include <cstdlib>
#include "redis_protocol.hh"
#include "db.hh"
#include "reply_builder.hh"
#include  <experimental/vector>
#include "core/metrics.hh"
using namespace net;
namespace redis {


using logger =  seastar::logger;
static logger redis_log ("redis");

distributed<service> _the_redis_srvice;

future<> service::set(const dht::decorated_key& dk, const sstring& value, output_stream<char>& out)
{
    return make_ready_future<>();
    /*
    auto cpu = get_cpu(dk);
    return _db.invoke_on(cpu, &database::set, std::ref(dk.key()), std::ref(value), 0, 0).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });;
    */
}

future<> service::del(const dht::decorated_key& dk, output_stream<char>& out)
{
    return make_ready_future<>();
/*
    auto cpu = get_cpu(dk);
    return _db.invoke_on(cpu, &database::set, std::ref(dk.key())).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });;
*/
}

future<> service::get(const dht::decorated_key& dk, output_stream<char>& out)
{
    return make_ready_future<>();
/*
    auto cpu = get_cpu(dk);
    return _db.invoke_on(cpu, &database::get, std::ref(dk.key())).then([&out] (auto&& m) {
        return out.write(std::move(*m));
    });;
*/
}

}
