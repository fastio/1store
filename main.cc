/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *
 *  Copyright (c) 2016-2026, Peng Jian, pstack@163.com. All rights reserved.
 *
 */
#include "redis.hh"
#include "db.hh"
#include "redis.hh"
#include "redis_commands.hh"
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
        redis_commands_ptr()->attach_redis(&_redis);
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

#define PLATFORM "seastar"
#define VERSION "v1.0"
#define VERSION_STRING PLATFORM " " VERSION

int main(int ac, char** av) {
    distributed<redis::db> db_peers;
    distributed<redis::system_stats> system_stats;
    distributed<redis::server> server;

    redis::redis_service redis(db_peers);

    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("stats",
         "Print basic statistics periodically (every second)")
        ("port", bpo::value<uint16_t>()->default_value(6379),
         "Specify UDP and TCP ports for redis server to listen on")
        ;

    return app.run_deprecated(ac, av, [&] {
            engine().at_exit([&] { return server.stop(); });
            engine().at_exit([&] { return db_peers.stop(); });
            engine().at_exit([&] { return system_stats.stop(); });

            auto&& config = app.configuration();
            uint16_t port = config["port"].as<uint16_t>();
            return db_peers.start().then([&system_stats] {
                return system_stats.start(redis::clock_type::now());
                }).then([&] {
                    std::cout << PLATFORM << " pedis " << VERSION << "\n";
                    return make_ready_future<>();
                }).then([&, port] {
                    return server.start(std::ref(redis), std::ref(system_stats), port);
                }).then([&server] {
                    return server.invoke_on_all(&redis::server::start);
                }).then([&, port] {
                    std::cout << " == server start success == \n";
                    return make_ready_future<>();
                });
            });
}
