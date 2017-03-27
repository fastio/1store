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
    redis::metric_server metric(system_stats);
    redis::redis_service redis(db);

    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(6379), "Redis server port to listen on")
        ("metric_port", bpo::value<uint16_t>()->default_value(11218), "Metric server port to listen on")
        ;

    return app.run_deprecated(ac, av, [&] {
        engine().at_exit([&] { return server.stop(); });
        engine().at_exit([&] { return db.stop(); });
        engine().at_exit([&] { return system_stats.stop(); });
        engine().at_exit([&] { return metric.stop(); });

        auto&& config = app.configuration();
        auto port = config["port"].as<uint16_t>();
        auto metric_port = config["metric_port"].as<uint16_t>();
        return db.start().then([&system_stats] {
            return system_stats.start(redis::clock_type::now());
        }).then([&, port] {
            return server.start(std::ref(redis), std::ref(system_stats), port);
        }).then([&server] {
            return server.invoke_on_all(&redis::server::start);
        }).then([&, metric_port] {
            return metric.start(metric_port);
        }).then([&] {
            std::cout << "Parallel Redis ... \n";
            return make_ready_future<>();
        });
    });
}
