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
#include "system_stats.hh"
#include "redis_protocol.hh"
namespace redis {
class server {
private:
    lw_shared_ptr<server_socket> _listener;
    redis_service& _redis;
    distributed<system_stats>& _system_stats;
    uint16_t _port;
    struct connection {
        connected_socket _socket;
        socket_address _addr;
        input_stream<char> _in;
        output_stream<char> _out;
        redis_protocol _proto;
        distributed<system_stats>& _system_stats;
        connection(connected_socket&& socket, socket_address addr, redis_service& redis, distributed<system_stats>& system_stats)
            : _socket(std::move(socket))
              , _addr(addr)
              , _in(_socket.input())
              , _out(_socket.output())
              , _proto(redis)
              , _system_stats(system_stats)
        {
            _system_stats.local()._curr_connections++;
            _system_stats.local()._total_connections++;
        }
        ~connection() {
            _system_stats.local()._curr_connections--;
        }
    };
public:
    server(redis_service& db, distributed<system_stats>& system_stats, uint16_t port = 6379)
        : _redis(db)
          , _system_stats(system_stats)
          , _port(port)
    {}

    void start() {
        listen_options lo;
        lo.reuse_address = true;
        _listener = engine().listen(make_ipv4_address({_port}), lo);
        keep_doing([this] {
           return _listener->accept().then([this] (connected_socket fd, socket_address addr) mutable {
               auto conn = make_lw_shared<connection>(std::move(fd), addr, _redis, _system_stats);
               do_until([conn] { return conn->_in.eof(); }, [this, conn] {
                   return conn->_proto.handle(conn->_in, conn->_out).then([conn] {
                       return conn->_out.flush();
                   });
               }).finally([conn] {
                   return conn->_out.close().finally([conn]{});
               });
           });
       }).or_terminate();
    }
    future<> stop() {
        return make_ready_future<>();
    }
};
} /* namespace redis */
