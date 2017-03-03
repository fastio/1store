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
#include "system_stats.hh"
#include "redis_protocol.hh"
#include "server.hh"

#define PLATFORM "seastar"
#define VERSION "v1.0"
#define VERSION_STRING PLATFORM " " VERSION

int main(int ac, char** av) {
    distributed<redis::database> db;
    distributed<redis::system_stats> system_stats;
    distributed<redis::server> server;

    redis::redis_service redis(db);

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
        engine().at_exit([&] { return db.stop(); });
        engine().at_exit([&] { return system_stats.stop(); });

        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        return db.start().then([&system_stats] {
            return system_stats.start(redis::clock_type::now());
        }).then([&, port] {
            return server.start(std::ref(redis), std::ref(system_stats), port);
        }).then([&server] {
            return server.invoke_on_all(&redis::server::start);
        }).then([&] {
            std::cout << "Parallel Redis ... \n";
            return make_ready_future<>();
        });
    });
}
