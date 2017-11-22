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
#include "core/metrics_registration.hh"
#include "core/thread.hh"
#include "reply_wrapper.hh"
#include "protocol_parser.hh"
namespace redis {
class server {
public:
    lw_shared_ptr<server_socket> _listener;
    uint16_t _port;
    bool _use_native_parser;
    struct connection {
        static constexpr size_t reply_queue_size = 16;
        connected_socket _socket;
        socket_address _addr;
        input_stream<char> _in;
        output_stream<char> _out;
        protocol_parser _parser;
        bool _done = false;
        bool _use_native_parser;
        queue<reply_wrapper> _replies { reply_queue_size };

        future<> process()
        {
            // Luanch request & reply fiblers simultaneously.
            return when_all(request(), reply()).then([] (std::tuple<future<>, future<>> joined) {
                std::get<0>(joined).ignore_ready_future();
                std::get<1>(joined).ignore_ready_future();
                return make_ready_future<>();
            });
        }
        future<scattered_message_ptr> handle();
        future<scattered_message_ptr> do_handle_one(request_wrapper& req);
        future<scattered_message_ptr> do_unexpect_request(request_wrapper& req);
        future<> request();
        future<> reply();

        connection(connected_socket&& socket, socket_address addr, bool use_native_parser)
            : _socket(std::move(socket))
            , _addr(addr)
            , _in(_socket.input())
            , _out(_socket.output())
            , _parser(make_ragel_protocol_parser())
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
public:
    server(uint16_t port = 6379, bool use_native_parser = false)
        : _port(port)
        , _use_native_parser(use_native_parser)
    {
        setup_metrics();
    }

    void start();
    future<> stop() {
        return make_ready_future<>();
    }
};
extern distributed<server> _server;
inline distributed<server>& get_server() {
    return _server;
}
inline server& get_local_server() {
    return _server.local();
}
} /* namespace redis */
