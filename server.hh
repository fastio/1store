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
#include "redis.hh"
#include "db.hh"
#include "redis.hh"
#include "redis_protocol.hh"
#include "core/metrics_registration.hh"
#include "core/thread.hh"
#include "core/gate.hh"
namespace redis {

class server;
extern distributed<server> _the_server;
inline distributed<server>& get_server() {
    return _the_server;
}
class server {
private:
    lw_shared_ptr<server_socket> _listener;
    uint16_t _port;
    struct connection {
        connected_socket _socket;
        socket_address _addr;
        input_stream<char> _in;
        output_stream<char> _out;
        redis_protocol _proto;
        connection(connected_socket&& socket, socket_address addr)
            : _socket(std::move(socket))
              , _addr(addr)
              , _in(_socket.input())
              , _out(_socket.output())
        {
        }
        ~connection() {
        }
    };
    seastar::metrics::metric_groups _metrics;
    void setup_metrics();
    struct stats {
        uint64_t _connections_current = 0;
        uint64_t _connections_total = 0;
    };
    stats _stats;
    request_latency_tracer _latency_tracer;
    seastar::gate _request_gate;
public:
    server(uint16_t port = 6379)
        : _port(port)
    {
        setup_metrics();
    }

    void start() {
        listen_options lo;
        lo.reuse_address = true;
        _listener = engine().listen(make_ipv4_address({_port}), lo);
        keep_doing([this] {
           return _listener->accept().then([this] (connected_socket fd, socket_address addr) mutable {
               return seastar::async([this, &fd, addr] {
                   ++_stats._connections_total;
                   ++_stats._connections_current;
                   auto conn = make_lw_shared<connection>(std::move(fd), addr);
                   do_until([conn] { return conn->_in.eof(); }, [this, conn] {
                       return with_gate(_request_gate, [this, conn] {
                           return conn->_proto.handle(conn->_in, conn->_out, _latency_tracer).then([this, conn] {
                               return conn->_out.flush();
                           });
                       });
                   }).finally([this, conn] {
                       --_stats._connections_current;
                       return conn->_out.close().finally([conn]{});
                   });
               });
           });
       }).or_terminate();
    }
    future<> stop() {
        return _request_gate.close().then([this] {
           return make_ready_future<>();
        });
    }
};
} /* namespace redis */
