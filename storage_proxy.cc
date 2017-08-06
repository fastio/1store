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
#include "storage_proxy.hh"
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
#include "common.hh"
#include "geo.hh"
namespace redis {

distributed<storage_proxy> _storage_proxy;

future<> storage_proxy::start()
{
    return make_ready_future<>();
}

future<> storage_proxy::stop()
{
    return make_ready_future<>();
}

future<> storage_proxy::process(args_collection& request_args, output_stream<char>& out)
{
    /*
    auto rk = extract_request_key(request_args);
    auto type = request_args.type();
    if (type == request::write) {
        // The key will be processed by local node.
        if (get_ring().local().should_served_by_me(rk)) {
            // First, handle the write request locally.
            // Second, submit the write request to remote node for replication.
            // Finally, write message to client. 
            return _redis.local().handle(request_args).then([this, rk, &out] (auto m) {
                auto remote_request = make_remote_request(request_args);
                auto replica_nodes = get_ring().local().get_replica_nodes_for_write(rk);
                for (auto& node : replica_nodes) {
                   _ms.local().send_internal_write(node, remote_request);
                }
                return out.write(*m);
            }); 
        }
        // This request will be served by remote node.
        // When the primary replica is successfuly served, return the message to the client.
        auto remote_request = make_remote_request(request_args);
        auto target_nodes = get_ring().local().get_target_node_for_write(rk);
        assert(target_nodes.empty() == false);
        auto f = _ms.local().send_internal_write(target_nodes[0], remote_request);
        for (auto i = target_nodes.begin() + 1, i != target_nodes.end(); ++i) {
            _ms.local().send_internal_write(node, remote_request);
        }
        return f.then([this, &out] (auto m) {
                return out.write(m);
        });
    }
    else if (type == request::read) {
        if (get_ring().local().should_served_by_me(rk)) {
            return _redis.local().handle(request_args).then([this, &out] (auto m) {
                return out.write(*m);
            });
        }
        auto remote_request = make_remote_request(request_args);
        auto primary_node = get_ring().local().get_replica_node_for_read(rk);
        return _ms.send_internal_read(primary_node, remote_request).then([this, &out] (auto m) {
            return out.write(*m);
        });
    }
    // The system commands
    */
    return make_ready_future<>();
}
}
