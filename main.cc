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
*  Copyright (c) 2016-2026, Peng Jian, pengjian.uestc@gmail.com. All rights reserved.
*
*/
#include "redis.hh"
#include "db.hh"
#include "redis.hh"
#include "redis_protocol.hh"
#include "server.hh"
#include "util/log.hh"
#include "core/prometheus.hh"
#define PLATFORM "seastar"
#define VERSION "v1.0"
#define VERSION_STRING PLATFORM " " VERSION

using logger =  seastar::logger;
static logger main_log ("main");


int main(int ac, char** av) {
    distributed<redis::database> db;
    distributed<redis::server> server;
    redis::redis_service redis(db);
    prometheus::config prometheus_config;
    httpd::http_server_control prometheus_server;
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(6379), "Redis server port to listen on")
        ("native_parser", bpo::value<bool>()->default_value(false), "Use native protocol parser")
        ("prometheus_port", bpo::value<uint16_t>()->default_value(10000), "Prometheus server port to listen on")
        ;

    return app.run_deprecated(ac, av, [&] {
        engine().at_exit([&] { return server.stop(); });
        engine().at_exit([&] { return db.stop(); });
        engine().at_exit([&] { return prometheus_server.stop(); });

        auto&& config = app.configuration();
        auto port = config["port"].as<uint16_t>();
        auto pport = config["prometheus_port"].as<uint16_t>();
        auto user_native_parser = config["native_parser"].as<bool>();
        return db.start().then([&, port] {
            return server.start(std::ref(redis), port, user_native_parser);
        }).then([&] {
            return server.invoke_on_all(&redis::server::start);
        }).then([&, pport] {
             prometheus_config.metric_help = "Redis server statistics";
             prometheus_config.prefix = "redis";
             return prometheus_server.start("prometheus").then([&] {
                 return prometheus::start(prometheus_server, prometheus_config);
             }).then([&, pport] {
                listen_options lo;
                lo.reuse_address = true;
                return prometheus_server.listen(make_ipv4_address({pport})).handle_exception([pport] (auto ep) {
                    return make_exception_future<>(ep);
                });
             });
        }).then([&, port] {
            main_log.info("Parallel Redis ... [{}]", port); 
            return make_ready_future<>();
        });
    });
}
