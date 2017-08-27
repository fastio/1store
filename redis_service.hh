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
#include "db.hh"
#include "dht/i_partitioner.hh"
namespace redis {
using namespace seastar;
class redis_service {
private:
    inline unsigned get_cpu(const dht::decorated_key& dk) {
        return std::hash<managed_bytes>()(dk.key().representation()) % smp::count;
    }
public:
    redis_service()
    {
    }

    future<> set(const dht::decorated_key& dk, const sstring& value, output_stream<char>& out);
    future<> del(const dht::decorated_key& dk, output_stream<char>& out);
    future<> get(const dht::decorated_key& dk, output_stream<char>& out);

    future<> stop();
private:
    distributed<database> _db;
    seastar::metrics::metric_groups _metrics;
};

extern distributed<redis_service> _the_redis_srvice;
inline distributed<redis_service>& get_redis_service() {
    return _the_redis_srvice;
}
inline redis_service& get_local_redis_service() {
    return _the_redis_srvice.local();
}
} /* namespace redis */
