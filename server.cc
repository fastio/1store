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
*  Copyright (c) 2016-2026, Peng Jian, pstack at 163.com.
*
*/
#include "server.hh"

#include <boost/bimap/unordered_set_of.hpp>
#include <boost/range/irange.hpp>
#include <boost/bimap.hpp>
#include <boost/assign.hpp>
#include <boost/locale/encoding_utf.hpp>
#include <boost/range/adaptor/sliced.hpp>

#include "cql3/statements/batch_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "db/consistency_level.hh"
#include "db/write_type.hh"
#include "core/future-util.hh"
#include "core/reactor.hh"
#include "utils/UUID.hh"
#include "database.hh"
#include "net/byteorder.hh"
#include <seastar/core/metrics.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/util/lazy.hh>
#include <seastar/core/execution_stage.hh>

#include "enum_set.hh"
#include "service/query_state.hh"
#include "service/client_state.hh"
#include "exceptions/exceptions.hh"

#include "auth/authenticator.hh"

#include <cassert>
#include <string>

#include <snappy-c.h>
#include <lz4.h>

namespace cql_transport {
static logging::logger clogger("redis_server");
redis_server::redis_server(distributed<service::redis_storage_proxy>& proxy)
    : _proxy(proxy)
    , _max_request_size(memory::stats().total_memory() / 10)
    , _memory_available(_max_request_size)
{
    namespace sm = seastar::metrics;

    _metrics.add_group("redis-transport", {
        sm::make_derive("cql-connections", _connects,
                        sm::description("Counts a number of client connections.")),

        sm::make_gauge("current_connections", _connections,
                        sm::description("Holds a current number of client connections.")),

        sm::make_derive("requests_served", _requests_served,
                        sm::description("Counts a number of served requests.")),

        sm::make_gauge("requests_serving", _requests_serving,
                        sm::description("Holds a number of requests that are being processed right now.")),

        sm::make_gauge("requests_blocked_memory", [this] { return _memory_available.waiters(); },
                        sm::description(
                            seastar::format("Holds a number of requests that are blocked due to reaching the memory quota limit ({}B). "
                                            "Non-zero value indicates that our bottleneck is memory and more specifically - the memory quota allocated for the \"CQL transport\" component.", _max_request_size))),
    });
}

future<> redis_server::stop() {
    _stopping = true;
    size_t nr = 0;
    size_t nr_total = _listeners.size();
    clogger.debug("redis_server: abort accept nr_total={}", nr_total);
    for (auto&& l : _listeners) {
        l.abort_accept();
        clogger.debug("redis_server: abort accept {} out of {} done", ++nr, nr_total);
    }
    auto nr_conn = make_lw_shared<size_t>(0);
    auto nr_conn_total = _connections_list.size();
    clogger.debug("redis_server: shutdown connection nr_total={}", nr_conn_total);
    return parallel_for_each(_connections_list.begin(), _connections_list.end(), [nr_conn, nr_conn_total] (auto&& c) {
        return c.shutdown().then([nr_conn, nr_conn_total] {
            clogger.debug("redis_server: shutdown connection {} out of {} done", ++(*nr_conn), nr_conn_total);
        });
    }).then([this] {
        return std::move(_stopped);
    });
}

future<> redis_server::listen(ipv4_addr addr, std::shared_ptr<seastar::tls::credentials_builder> creds, bool keepalive) {
    listen_options lo;
    lo.reuse_address = true;
    server_socket ss;
    try {
        ss = creds
          ? seastar::tls::listen(creds->build_server_credentials(), make_ipv4_address(addr), lo)
          : engine().listen(make_ipv4_address(addr), lo);
    } catch (...) {
        throw std::runtime_error(sprint("REDIS server error while listening on %s -> %s", make_ipv4_address(addr), std::current_exception()));
    }
    _listeners.emplace_back(std::move(ss));
    _stopped = when_all(std::move(_stopped), do_accepts(_listeners.size() - 1, keepalive, addr)).discard_result();
    return make_ready_future<>();
}

future<> redis_server::do_accepts(int which, bool keepalive, ipv4_addr server_addr) {
    ++_connections_being_accepted;
    return _listeners[which].accept().then([this, which, keepalive, server_addr] (connected_socket fd, socket_address addr) mutable {
        --_connections_being_accepted;
        if (_stopping) {
            maybe_idle();
            return make_ready_future<>();
        }
        fd.set_nodelay(true);
        fd.set_keepalive(keepalive);
        auto conn = make_lw_shared<connection>(*this, std::ref(_proxy), server_addr, std::move(fd), std::move(addr));
        return do_accepts(which, keepalive, server_addr);
    });
}


redis_server::connection::connection(redis_server& server, distributed<service::redis_storage_proxy>& proxy, ipv4_addr server_addr, connected_socket&& fd, socket_address addr)
    : _server(server)
    , _proxy(proxy)
    , _server_addr(server_addr)
    , _fd(std::move(fd))
    , _in(_fd.input())
    , _out(_fd.output())
    , _client_state(service::client_state::external_tag{}, addr)
{
    ++_server._total_connections;
    ++_server._current_connections;
    _server._connections_list.push_back(*this);
}

redis_server::connection::~connection() {
    --_server._current_connections;
    _server._connections_list.erase(_server._connections_list.iterator_to(*this));
    _server.maybe_idle();
}

future<> redis_server::connection::process()
{
    return do_until([this] { return _in.eof(); }, [this] {
         return with_gate(_pending_requests_gate, [this] {
             return _proto.handle(_in, _out).then([this] () { return _out.flush(); });
         });
    }).finally([this] {
         return _pending_requests_gate.close().then([this] {
             return _ready_to_respond.finally([this] {
                return _out.close();
             });
         });
    });
}

future<> redis_server::connection::shutdown()
{
    try {
        _fd.shutdown_input();
        _fd.shutdown_output();
    } catch (...) {
    }
    return make_ready_future<>();
}
}
